"""Server-side fast path for the cross-table raw_data expansion.

The classic expansion executes every recipe directly against the JSONB source,
so each destination statement re-detoasts and re-parses the whole ``raw``
column (seven full JSONB scans for the six-destination challenge workload).
This module implements a generic, semantics-preserving fast path:

1.  Every ``<alias>.raw -> 'values' ->> '<field>'`` access in the recipes is
    extracted exactly once into an UNLOGGED staging table, in parallel worker
    sessions, each covering a raw_id range through the primary-key index.
2.  Each recipe is rewritten to read the same values from the staging table.
    Text values are stored verbatim so every recipe expression keeps its exact
    semantics; casts that the recipes perform are left untouched.  When every
    occurrence of a field is wrapped in the same ``NULLIF(BTRIM(x),'')::type``
    cast, the field is staged pre-cast and the redundant wrapper is removed.
3.  The dependency manifest is derived from per-worker aggregates instead of
    an extra full scan of the source.
4.  When the destination tables are empty, foreign keys and redundant unique
    constraints of the destinations are dropped before the bulk insert and
    rebuilt/validated inside the same expansion transaction, so the final
    schema (and the validated guarantees) are identical.

The rewrite is byte-transparent in semantics: ``pgdm_v_<field>`` holds exactly
``raw -> 'values' ->> '<field>'``.  Any statement that does not match the
supported access patterns causes the whole expansion to fall back to the
classic path, keeping the Manager fully generic.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass, field

_ACCESS_RE_TEMPLATE = (
    r"(?P<alias>[A-Za-z_][\w$]*)\s*\.\s*raw\s*->\s*'values'\s*->>\s*"
    r"'(?P<field>[A-Za-z_][\w$]*)'"
)

_TYPE_RE = re.compile(
    r"(?P<type>smallint|integer|bigint|numeric(?:\s*\(\s*\d+\s*,\s*\d+\s*\))?"
    r"|real|double\s+precision|text|character\s+varying(?:\s*\(\s*\d+\s*\))?"
    r"|varchar(?:\s*\(\s*\d+\s*\))?|timestamptz|timestamp(?:\s+with(?:out)?\s+time\s+zone)?"
    r"|date|boolean|uuid)",
    re.IGNORECASE,
)

_SOURCE_VALUE_COLUMNS = ("raw_id", "raw_schema", "raw_hash", "raw_ingested_at", "raw_tab")


def _stage_column_name(jsonb_field: str) -> str:
    return f"pgdm_v_{jsonb_field}"


@dataclass
class _Access:
    start: int
    end: int
    alias: str
    jsonb_field: str


@dataclass
class OptimizedStatement:
    destination_index: int
    statement_index: int
    original: str
    rewritten: str


@dataclass
class OptimizedExpansionPlan:
    job_id: str
    staging_columns: list[dict] = field(default_factory=list)
    statements: list[OptimizedStatement] = field(default_factory=list)
    source_table_sql: str = ""
    raw_schemas: list[str] = field(default_factory=list)
    source_schema: str = ""
    source_table: str = ""
    recipes_hash: str = ""
    destinations_payload: list = field(default_factory=list)


def _find_accesses(statement: str, mask: str, alias: str) -> list[_Access]:
    pattern = re.compile(_ACCESS_RE_TEMPLATE, re.IGNORECASE)
    accesses = []
    for match in pattern.finditer(statement):
        if match.group("alias").lower() != alias.lower():
            continue
        # Inside a comment or literal the mask blanks every character,
        # including the structural alias prefix.
        if mask[match.start("alias")] == " ":
            continue
        accesses.append(
            _Access(
                start=match.start(),
                end=match.end(),
                alias=match.group("alias"),
                jsonb_field=match.group("field"),
            )
        )
    return accesses


_BACKWARD_WRAPPER_RE = re.compile(r"nullif\s*\(\s*btrim\s*\(\s*$", re.IGNORECASE)
_FORWARD_CLOSE_RE = re.compile(r"^\s*\)\s*,", re.IGNORECASE)


def _match_cast_wrapper(
    statement: str, mask: str, access: _Access
) -> tuple[int, int, str] | None:
    """Match ``NULLIF(BTRIM(<access>), '')::<type>`` around the access.

    Returns (span_start, span_end, canonical_type) or None.
    """
    prefix = statement[: access.start]
    match_wrapper = _BACKWARD_WRAPPER_RE.search(prefix)
    if not match_wrapper:
        return None
    span_start = match_wrapper.start()
    if mask[span_start] == " ":
        return None

    suffix = statement[access.end :]
    match_close = _FORWARD_CLOSE_RE.match(suffix)
    if not match_close:
        return None
    cursor = access.end + match_close.end()
    literal_match = re.match(r"\s*''", statement[cursor:])
    if not literal_match:
        return None
    cursor += literal_match.end()
    match_paren = re.match(r"\s*\)", statement[cursor:])
    if not match_paren:
        return None
    cursor += match_paren.end()
    match_cast = re.match(r"\s*::", statement[cursor:])
    if not match_cast:
        return None
    cursor += match_cast.end()
    type_match = _TYPE_RE.match(statement[cursor:])
    if not type_match:
        return None
    canonical = re.sub(r"\s+", " ", type_match.group("type")).strip().lower()
    return span_start, cursor + type_match.end(), canonical


@dataclass
class _Occurrence:
    statement_index_in_plan: int
    access: _Access
    wrapper: tuple[int, int, str] | None  # (span_start, span_end, cast_type) or None


def _scan_statement_occurrences(
    statement: str,
    mask: str,
    alias: str,
) -> tuple[list[_Occurrence], bool] | None:
    """Find every source access occurrence in one statement.

    Returns (occurrences, supported) where supported=False means the
    statement uses an unsupported pattern and the classic path must run.
    """
    accesses = _find_accesses(statement, mask, alias)
    if not accesses:
        return [], True
    occurrences: list[_Occurrence] = []
    matched_spans: list[tuple[int, int]] = []
    for access in accesses:
        wrapper = _match_cast_wrapper(statement, mask, access)
        occurrences.append(_Occurrence(-1, access, wrapper))
        if wrapper:
            matched_spans.append((wrapper[0], wrapper[1]))
        else:
            matched_spans.append((access.start, access.end))

    # Any remaining direct use of alias.raw cannot be staged generically.
    for raw_reference in re.finditer(
        rf"\b{re.escape(alias)}\s*\.\s*raw\b", statement, re.IGNORECASE
    ):
        inside = any(
            span_start <= raw_reference.start() < span_end
            for span_start, span_end in matched_spans
        )
        if not inside:
            return occurrences, False
    return occurrences, True


def _rewrite_statement(
    statement: str,
    alias: str,
    occurrences: list[_Occurrence],
    field_types: dict[str, str | None],
) -> str:
    """Apply the final substitutions for one statement.

    field_types maps jsonb field -> staging DDL type (None means text).
    """
    replacements: list[tuple[int, int, str]] = []
    for occurrence in occurrences:
        access = occurrence.access
        column_reference = f"{alias}.{_stage_column_name(access.jsonb_field)}"
        ddl_type = field_types.get(access.jsonb_field)
        if occurrence.wrapper:
            span_start, span_end, cast_type = occurrence.wrapper
            if ddl_type:
                replacements.append((span_start, span_end, column_reference))
            else:
                # Field stays text: rebuild the original cast wrapper around
                # the staged text column so the expression is unchanged.
                rebuilt = (
                    f"NULLIF(BTRIM({column_reference}), '')::{cast_type}"
                )
                replacements.append((span_start, span_end, rebuilt))
        else:
            if ddl_type and ddl_type != "text":
                replacements.append(
                    (access.start, access.end, f"{column_reference}::text")
                )
            else:
                replacements.append((access.start, access.end, column_reference))
    rewritten = statement
    for span_start, span_end, replacement in sorted(replacements, reverse=True):
        rewritten = rewritten[:span_start] + replacement + rewritten[span_end:]
    return rewritten


def _resolve_source_alias(statement: str) -> str | None:
    match = re.search(
        r"\{\{\s*source\s*\}\}\s+as\s+([A-Za-z_][\w$]*)", statement, re.IGNORECASE
    )
    if match:
        return match.group(1)
    # Statements prepared by the service carry the guarded source subselect;
    # its correlation alias is the one bound right after the closing paren.
    match = re.search(r"\)\s+as\s+([A-Za-z_][\w$]*)", statement, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def _restore_source_placeholder(
    statement: str,
    source_schema: str,
    source_table: str,
    raw_schemas: list[str],
) -> str | None:
    """Replace the service's guarded source subselect with ``{{source}}``."""
    from services.postgres_service import sql_ident

    scope_sql = ", ".join(f"'{value}'" for value in raw_schemas)
    guarded_source = (
        "(\n"
        "    SELECT *\n"
        f"    FROM {sql_ident(source_schema)}.{sql_ident(source_table)}\n"
        f"    WHERE {sql_ident('raw_schema')} IN ({scope_sql})\n"
        ")"
    )
    if guarded_source in statement:
        return statement.replace(guarded_source, "{{source}}")
    if "{{source}}" in statement:
        return statement
    return None


def _split_top_level(text: str, separator: str = ",") -> list[str]:
    parts = []
    depth = 0
    current = []
    index = 0
    while index < len(text):
        char = text[index]
        if char == "'":
            current.append(char)
            index += 1
            while index < len(text):
                current.append(text[index])
                if text[index] == "'":
                    if index + 1 < len(text) and text[index + 1] == "'":
                        current.append(text[index + 1])
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
        elif char == "(":
            depth += 1
            current.append(char)
            index += 1
        elif char == ")":
            depth -= 1
            current.append(char)
            index += 1
        elif char == separator and depth == 0:
            parts.append("".join(current))
            current = []
            index += 1
        else:
            current.append(char)
            index += 1
    if current:
        parts.append("".join(current))
    return parts


def _find_matching_paren(text: str, open_index: int) -> int:
    depth = 0
    index = open_index
    while index < len(text):
        if text[index] == "'":
            index += 1
            while index < len(text):
                if text[index] == "'":
                    if index + 1 < len(text) and text[index + 1] == "'":
                        index += 2
                        continue
                    break
                index += 1
        elif text[index] == "(":
            depth += 1
        elif text[index] == ")":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return -1


_LATERAL_HEADER_RE = re.compile(
    r"(?P<keyword>cross\s+join\s+lateral|lateral)\s*\(", re.IGNORECASE
)
_FIELD_ENTRY_RE_TEMPLATE = r"^\s*{alias}\.pgdm_v_(?P<field>[A-Za-z_][\w$]*)(?:::text)?\s*$"


def _rewrite_lateral_typed_flow(
    statement: str,
    source_alias: str,
    field_types: dict[str, str | None],
) -> str:
    """Let typed staging columns flow through LATERAL VALUES columns.

    When every row of a LATERAL ``VALUES`` column passes exactly one typed
    staging field straight through, the column keeps that field's type: the
    ``::text`` detour is removed and downstream
    ``NULLIF(BTRIM(col),'')::<type>`` wrappers of the same type collapse to
    the bare column.  Row selection semantics are unchanged because the
    staged value already equals the cast of the trimmed JSONB text.
    """
    rewrites: list[tuple[int, int, str]] = []
    typed_columns: list[tuple[str, str]] = []  # (qualified column, ddl_type)

    for header in _LATERAL_HEADER_RE.finditer(statement):
        open_index = statement.index("(", header.end() - 1)
        close_index = _find_matching_paren(statement, open_index)
        if close_index == -1:
            continue
        body = statement[open_index + 1 : close_index]
        values_match = re.search(r"\bvalues\b", body, re.IGNORECASE)
        if not values_match:
            continue
        rows_text = body[values_match.end() :]
        rows = [row.strip() for row in _split_top_level(rows_text)]
        rows = [row[1:-1].strip() if row.startswith("(") and row.endswith(")") else row for row in rows]

        # Alias and column list follow the closing paren.
        after = statement[close_index + 1 :]
        alias_match = re.match(
            r"\s*as\s+([A-Za-z_][\w$]*)\s*\((?P<cols>[^)]*)\)", after, re.IGNORECASE
        )
        if not alias_match:
            continue
        lateral_alias = alias_match.group(1)
        column_names = [
            column.strip() for column in alias_match.group("cols").split(",")
        ]
        if len(column_names) != len(rows[0].split(",")) if rows else True:
            pass  # verified per-column below
        row_entries = [
            [entry.strip() for entry in _split_top_level(row)] for row in rows
        ]
        entry_counts = {len(entries) for entries in row_entries}
        if len(entry_counts) != 1 or len(column_names) not in entry_counts:
            continue

        field_entry_re = re.compile(
            _FIELD_ENTRY_RE_TEMPLATE.format(alias=re.escape(source_alias)),
            re.IGNORECASE,
        )
        for column_index, column_name in enumerate(column_names):
            column_type = None
            spans = []
            supported = True
            for entries, row in zip(row_entries, rows):
                entry = entries[column_index]
                entry_match = field_entry_re.match(entry)
                if not entry_match:
                    supported = False
                    break
                ddl_type = field_types.get(entry_match.group("field").lower())
                if not ddl_type or ddl_type == "text":
                    supported = False
                    break
                if column_type is None:
                    column_type = ddl_type
                elif column_type != ddl_type:
                    supported = False
                    break
                # Span of the "::text" suffix inside the original statement.
                row_offset = statement.index(row, open_index + 1)
                entry_offset = statement.index(entry, row_offset)
                if entry.lower().endswith("::text"):
                    spans.append(
                        (entry_offset + len(entry) - len("::text"), entry_offset + len(entry))
                    )
            if not supported or column_type is None:
                continue
            for span_start, span_end in spans:
                rewrites.append((span_start, span_end, ""))
            typed_columns.append(
                (f"{lateral_alias}.{column_name}", column_type)
            )

    rewritten = statement
    for span_start, span_end, replacement in sorted(rewrites, reverse=True):
        rewritten = rewritten[:span_start] + replacement + rewritten[span_end:]

    # Collapse same-type cast wrappers around the typed lateral columns.
    for qualified_column, ddl_type in typed_columns:
        wrapper_re = re.compile(
            r"nullif\s*\(\s*btrim\s*\(\s*"
            + re.escape(qualified_column)
            + r"\s*\)\s*,\s*''\s*\)\s*::"
            + re.escape(ddl_type)
            + r"\b",
            re.IGNORECASE,
        )
        spans = [
            (match.start(), match.end(), qualified_column)
            for match in wrapper_re.finditer(rewritten)
        ]
        for span_start, span_end, replacement in sorted(spans, reverse=True):
            rewritten = (
                rewritten[:span_start] + replacement + rewritten[span_end:]
            )
    return rewritten


def plan_optimized_expansion(
    prepared: dict,
    source_columns: dict[str, str],
) -> OptimizedExpansionPlan | None:
    """Build a staging plan for the prepared destinations, or None.

    ``prepared`` is the output of ``_prepare_cross_table_expansion``;
    ``source_columns`` maps source column name -> data type.
    """
    source_schema = prepared["source_schema_name"]
    source_table = prepared["source_table_name"]
    if not re.fullmatch(r"[A-Za-z_][\w$]*", source_schema) or not re.fullmatch(
        r"[A-Za-z_][\w$]*", source_table
    ):
        return None
    if any(name not in source_columns for name in ("raw_id", "raw_schema")):
        return None

    staging_columns: dict[str, dict] = {}
    referenced_value_columns: set[str] = set()
    scanned: list[tuple[int, int, str, str, list[_Occurrence]]] = []

    for destination_index, destination in enumerate(prepared["destinations"]):
        for statement_index, statement in enumerate(destination["statements"], start=1):
            restored = _restore_source_placeholder(
                statement,
                source_schema,
                source_table,
                list(prepared["raw_schemas"]),
            )
            if restored is None:
                return None
            alias = _resolve_source_alias(restored)
            if alias is None:
                return None
            mask = _mask_statement(restored)
            scan = _scan_statement_occurrences(restored, mask, alias)
            if scan is None:
                return None
            occurrences, supported = scan
            if not supported:
                return None
            for occurrence in occurrences:
                meta = staging_columns.setdefault(
                    occurrence.access.jsonb_field,
                    {
                        "name": _stage_column_name(occurrence.access.jsonb_field),
                        "kind": "jsonb",
                        "source": occurrence.access.jsonb_field,
                    },
                )
                if occurrence.wrapper:
                    cast_type = occurrence.wrapper[2]
                    existing_type = meta.get("typed_as")
                    if existing_type is None:
                        meta["typed_as"] = cast_type
                    elif existing_type != cast_type:
                        meta["typed_as"] = False
            for value_column in _SOURCE_VALUE_COLUMNS:
                if re.search(
                    rf"\b{re.escape(alias)}\s*\.\s*{value_column}\b",
                    restored,
                    re.IGNORECASE,
                ):
                    referenced_value_columns.add(value_column)
            scanned.append(
                (destination_index, statement_index, restored, alias, occurrences)
            )

    field_types: dict[str, str | None] = {
        field: (
            meta["typed_as"] if isinstance(meta.get("typed_as"), str) else None
        )
        for field, meta in staging_columns.items()
    }

    statements: list[OptimizedStatement] = []
    for destination_index, statement_index, restored, alias, occurrences in scanned:
        rewritten = _rewrite_statement(restored, alias, occurrences, field_types)
        rewritten = _rewrite_lateral_typed_flow(rewritten, alias, field_types)
        statements.append(
            OptimizedStatement(
                destination_index=destination_index,
                statement_index=statement_index,
                original=restored,
                rewritten=rewritten,
            )
        )

    columns: list[dict] = [
        {"name": "raw_id", "kind": "value", "source": "raw_id", "ddl_type": "bigint"},
        {"name": "raw_schema", "kind": "value", "source": "raw_schema", "ddl_type": "text"},
        {"name": "raw_hash", "kind": "value", "source": "raw_hash", "ddl_type": "text"},
    ]
    for value_column in ("raw_ingested_at", "raw_tab"):
        if value_column in referenced_value_columns:            columns.append(
                {
                    "name": value_column,
                    "kind": "value",
                    "source": value_column,
                    "ddl_type": source_columns.get(value_column, "text"),
                }
            )
    for meta in staging_columns.values():
        typed_as = meta.get("typed_as")
        columns.append(
            {
                "name": meta["name"],
                "kind": "jsonb",
                "source": meta["source"],
                "ddl_type": typed_as if isinstance(typed_as, str) else "text",
                "typed": isinstance(typed_as, str),
            }
        )

    return OptimizedExpansionPlan(
        job_id=uuid.uuid4().hex[:12],
        staging_columns=columns,
        statements=statements,
        source_table_sql=f"{source_schema}.{source_table}",
        raw_schemas=list(prepared["raw_schemas"]),
        source_schema=source_schema,
        source_table=source_table,
        recipes_hash=compute_recipes_hash(prepared),
        destinations_payload=[
            {
                "table_name": destination["table_name"],
                "schema_name": destination["schema_name"],
                "pure_table_name": destination["pure_table_name"],
                "version_title": destination["version_title"],
            }
            for destination in prepared["destinations"]
        ],
    )


def _mask_statement(sql_text: str) -> str:
    from services.postgres_service import PostgresAdminService

    return PostgresAdminService._mask_sql_literals_and_comments(sql_text)


def build_staging_ddl(plan: OptimizedExpansionPlan, partition_name: str) -> str:
    column_defs = ",\n    ".join(
        f"{column['name']} {column['ddl_type']} NOT NULL"
        if column["name"] == "raw_id"
        else f"{column['name']} {column['ddl_type']}"
        for column in plan.staging_columns
    )
    return f"CREATE UNLOGGED TABLE {partition_name} (\n    {column_defs}\n);"


def build_staging_insert(
    plan: OptimizedExpansionPlan,
    partition_name: str,
    raw_id_lo: int,
    raw_id_hi: int,
) -> str:
    extraction_sql = ",\n        ".join(
        (
            column["source"]
            if column["kind"] == "value"
            else (
                (
                    f"NULLIF(BTRIM(raw -> 'values' ->> '{column['source']}'), '')"
                    f"::{column['ddl_type']}"
                )
                if column.get("typed")
                else f"raw -> 'values' ->> '{column['source']}'"
            )
        )
        for column in plan.staging_columns
    )
    scope_sql = ", ".join(f"'{value}'" for value in plan.raw_schemas)
    return (
        f"INSERT INTO {partition_name}\n"
        f"SELECT\n        {extraction_sql}\n"
        f"FROM {plan.source_table_sql}\n"
        f"WHERE raw_schema IN ({scope_sql})\n"
        f"  AND raw_id >= {raw_id_lo} AND raw_id <= {raw_id_hi};"
    )


def build_staging_union(plan: OptimizedExpansionPlan, partitions: list[str]) -> str:
    parts = "\n            UNION ALL\n            ".join(
        f"SELECT * FROM {partition}" for partition in partitions
    )
    return f"(\n            {parts}\n        )"


def build_worker_result_sql(partition_name: str) -> str:
    return (
        "SELECT COALESCE(json_build_object(\n"
        "    'rows', COUNT(*),\n"
        "    'min_raw_id', MIN(raw_id),\n"
        "    'max_raw_id', MAX(raw_id),\n"
        "    'manifest', COALESCE((SELECT json_agg(json_build_object(\n"
        "        'raw_schema', raw_schema, 'raw_hash', raw_hash, 'row_count', row_count\n"
        "    ) ORDER BY raw_schema, raw_hash NULLS FIRST) FROM (\n"
        "        SELECT raw_schema, raw_hash, COUNT(*) AS row_count\n"
        f"        FROM {partition_name}\n"
        "        GROUP BY raw_schema, raw_hash\n"
        "    ) AS grouped), '[]'::json)\n"
        ")::text, '{}'::text)\n"
        f"FROM {partition_name};"
    )


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _sql_text_literal(value) -> str:
    if value is None:
        return "NULL"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def default_worker_count() -> int:
    import os

    override = os.getenv("PDM_EXPANSION_WORKERS")
    if override and override.strip().isdigit() and int(override) >= 0:
        return int(override)
    cores = os.cpu_count() or 4
    return max(2, min(8, cores - 1))


class StagingWorkerError(RuntimeError):
    pass


def shlex_quote(value: str) -> str:
    import shlex

    return shlex.quote(value)


def run_staging_phase(
    service,
    plan: OptimizedExpansionPlan,
    timing_report: dict,
    database_name: str,
    cancel_event=None,
    worker_count: int | None = None,
    job_manager=None,
    job: dict | None = None,
) -> dict:
    """Build (or resume) the staging partitions in parallel sessions.

    When a job manager and a resumable job record are supplied, partitions
    checkpointed as ``staged`` are validated and reused; only incomplete
    partitions are re-extracted, and every finished worker is checkpointed
    as soon as it completes.
    """
    if worker_count is None:
        worker_count = default_worker_count()

    if job_manager is not None and job is not None:
        plan.job_id = job["job_id"]
        pending = [
            (
                int(partition["partition_index"]),
                int(partition["raw_id_lo"]),
                int(partition["raw_id_hi"]),
            )
            for partition in job.get("partitions") or []
            if partition.get("status") != "staged"
        ]
        resumed_aggregates = []
        for partition in job.get("partitions") or []:
            if partition.get("status") != "staged":
                continue
            resumed_aggregates.append(
                {
                    "worker": int(partition["partition_index"]),
                    "partition_name": partition["partition_name"],
                    "raw_id_lo": partition["raw_id_lo"],
                    "raw_id_hi": partition["raw_id_hi"],
                    "rows": partition.get("staged_rows") or 0,
                    "insert_seconds": partition.get("insert_seconds") or 0.0,
                    "manifest": partition.get("manifest") or [],
                    "server_seconds": 0.0,
                    "reused": True,
                }
            )
        if resumed_aggregates and timing_report is not None:
            service._append_expansion_timing(
                timing_report,
                "staging",
                "Resume interrupted expansion job",
                0.0,
                "checkpoint_event",
                details=(
                    f"Resumed job {job['job_id']}: "
                    f"{len(resumed_aggregates)} checkpointed partition(s) "
                    f"({sum(int(item['rows']) for item in resumed_aggregates):,} rows) "
                    "reused; only incomplete partitions are re-extracted."
                ),
            )
    else:
        bounds_sql = (
            "SELECT COALESCE(MIN(raw_id), 0)::text, COALESCE(MAX(raw_id), 0)::text "
            f"FROM {plan.source_table_sql};"
        )
        command = (
            f"psql -h localhost -U {shlex_quote(service.sql_username)} "
            f"-d {shlex_quote(database_name)} -X -qAt -v ON_ERROR_STOP=1 "
            f"-c {shlex_quote(bounds_sql)}"
        )
        bounds_output = service.run_remote_command(
            command, cancel_event=cancel_event
        ).strip()
        bounds = bounds_output.splitlines()[-1].split("|") if bounds_output else ["0", "0"]
        min_raw_id, max_raw_id = int(bounds[0] or 0), int(bounds[1] or 0)
        if max_raw_id <= 0 or worker_count <= 1:
            worker_count = 1
            ranges = [(min_raw_id, max_raw_id)]
        else:
            span = max_raw_id - min_raw_id + 1
            edges = [
                min_raw_id + (span * index) // worker_count
                for index in range(worker_count + 1)
            ]
            ranges = [
                (
                    edges[index],
                    edges[index + 1] - 1 if index + 1 < len(edges) else max_raw_id,
                )
                for index in range(worker_count)
            ]
            ranges = [(lo, hi) for lo, hi in ranges if lo <= hi]
        if job_manager is not None:
            job = job_manager.create_job(
                plan,
                worker_count,
                os.getenv("PDM_EXPANSION_REQUESTED_BY", "expansion"),
                os.getenv("PDM_EXPANSION_WORKSTATION", ""),
                ranges,
            )
            plan.job_id = job["job_id"]
        pending = [
            (index, raw_id_lo, raw_id_hi)
            for index, (raw_id_lo, raw_id_hi) in enumerate(ranges)
        ]
        resumed_aggregates = []

    phase_started = time.perf_counter()
    results: dict[int, dict] = {}
    errors: dict[int, str] = {}

    def execute_worker(index: int, raw_id_lo: int, raw_id_hi: int) -> None:
        try:
            aggregate = _execute_staging_worker(
                service, plan, database_name, index, raw_id_lo, raw_id_hi, cancel_event
            )
            results[index] = aggregate
            if job_manager is not None and job is not None:
                job_manager.checkpoint_partition(job["job_id"], aggregate)
        except Exception as exc:  # noqa: BLE001 - reported per worker below
            errors[index] = str(exc)

    threads = [
        threading.Thread(
            target=execute_worker,
            args=(index, raw_id_lo, raw_id_hi),
            name=f"pgdm-staging-{index}",
            daemon=True,
        )
        for index, raw_id_lo, raw_id_hi in pending
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    phase_seconds = max(time.perf_counter() - phase_started, 0.0)

    if errors:
        detail = "; ".join(
            f"worker {index}: {message}" for index, message in errors.items()
        )
        raise RuntimeError(f"The staging extraction failed. {detail}")

    ordered = resumed_aggregates + [results[index] for index in sorted(results)]
    ordered.sort(key=lambda item: int(item.get("worker", 0)))
    if timing_report is not None:
        service._append_expansion_timing(
            timing_report,
            "staging",
            f"Parallel staging extraction ({len(pending)} worker session(s))",
            phase_seconds,
            "orchestrated_phase_wall",
            rows=sum(
                int(item.get("rows") or 0)
                for item in ordered
                if not item.get("reused")
            ),
            details=(
                "Wall time of the parallel staging phase; each worker session is "
                "clocked by the Linux server. SSH connection and return transit "
                "are excluded. Per-worker server times are listed below as "
                "diagnostics."
            ),
        )
        for item in ordered:
            service._append_expansion_timing(
                timing_report,
                "staging",
                f"Staging worker {item.get('worker', 0)}"
                + (" (reused checkpoint)" if item.get("reused") else " (diagnostic)"),
                max(float(item.get("server_seconds") or 0.0), 0.0),
                "remote_server_wall",
                rows=item.get("rows"),
                destination=item.get("partition_name"),
                details=(
                    f"raw_id {item.get('raw_id_lo')}..{item.get('raw_id_hi')}; "
                    "insert clocked inside PostgreSQL: "
                    f"{float(item.get('insert_seconds') or 0.0):.3f}s. "
                    + (
                        "Reused from the persistent checkpoint; not re-extracted."
                        if item.get("reused")
                        else "Diagnostic only; excluded from totals."
                    )
                ),
                include_in_total=False,
            )

    return {
        "partitions": [item["partition_name"] for item in ordered],
        "aggregates": ordered,
        "job": job,
    }


def _execute_staging_worker(
    service,
    plan: OptimizedExpansionPlan,
    database_name: str,
    index: int,
    raw_id_lo: int,
    raw_id_hi: int,
    cancel_event=None,
) -> dict:
    partition_name = f"public.pgdm_stage_{plan.job_id}_p{index}"
    ddl = build_staging_ddl(plan, partition_name)
    insert_sql = build_staging_insert(plan, partition_name, raw_id_lo, raw_id_hi)
    result_sql = build_worker_result_sql(partition_name)
    worker_sql = f"""
SET client_min_messages = warning;
BEGIN;
SET LOCAL synchronous_commit = off;
SET LOCAL enable_seqscan = off;
{ddl}
CREATE TEMP TABLE pgdm_worker_timing (seconds double precision, rows bigint);
DO $pgdm_staging_worker$
DECLARE
    pgdm_started_at timestamptz;
    pgdm_rows bigint;
BEGIN
    pgdm_started_at := clock_timestamp();
    {insert_sql}
    GET DIAGNOSTICS pgdm_rows = ROW_COUNT;
    INSERT INTO pgdm_worker_timing (seconds, rows)
    VALUES (EXTRACT(EPOCH FROM (clock_timestamp() - pgdm_started_at)), pgdm_rows);
END
$pgdm_staging_worker$;
ANALYZE {partition_name};
{result_sql}
COMMIT;
""".strip()

    marker = f"__PGDM_WORKER_{index}_{time.time_ns()}__="
    command = (
        "pgdm_t0=$(date +%s%N); { "
        f"psql -h localhost -U {shlex_quote(service.sql_username)} "
        f"-d {shlex_quote(database_name)} -X -qAt -v ON_ERROR_STOP=1 -f -"
        " }; pgdm_rc=$?; pgdm_t1=$(date +%s%N); "
        f"printf '%s%s\\n' '{marker}' \"$((pgdm_t1 - pgdm_t0))\"; exit $pgdm_rc"
    )
    command = command.replace(" -f - }", " -f -; }")
    output = service.run_remote_command(command, stdin_text=worker_sql, cancel_event=cancel_event)
    server_seconds = 0.0
    lines = []
    for line in output.splitlines():
        candidate = line.strip()
        if candidate.startswith(marker):
            server_seconds = int(candidate[len(marker):]) / 1_000_000_000.0
        elif candidate.startswith("{"):
            lines.append(candidate)
    if not lines:
        raise StagingWorkerError(
            f"Staging worker {index} did not return its manifest payload."
        )
    import json

    payload = json.loads(lines[-1])
    payload.update(
        {
            "worker": index,
            "partition_name": partition_name,
            "raw_id_lo": raw_id_lo,
            "raw_id_hi": raw_id_hi,
            "server_seconds": server_seconds,
        }
    )
    return payload


def build_staging_cleanup_sql(partitions: list[str]) -> str:
    names = ",\n    ".join(partitions)
    return f"DROP TABLE IF EXISTS\n    {names};"


def build_optimized_manifest_sql(
    plan: OptimizedExpansionPlan,
    aggregates: list[dict],
    source_columns_json: str,
) -> str:
    """Manifest built from the per-partition aggregates (no extra source scan)."""
    combined: dict[tuple[str, str | None], int] = {}
    for aggregate in aggregates:
        for entry in aggregate.get("manifest") or []:
            key = (str(entry.get("raw_schema")), entry.get("raw_hash"))
            combined[key] = combined.get(key, 0) + int(entry.get("row_count") or 0)

    def _sort_key(item):
        (raw_schema, raw_hash), _count = item
        return (raw_schema, raw_hash is not None, raw_hash or "")

    values_sql = ",\n    ".join(
        f"({_sql_text_literal(raw_schema)}::text, {_sql_text_literal(raw_hash)}::text, {row_count}::bigint)"
        for (raw_schema, raw_hash), row_count in sorted(combined.items(), key=_sort_key)
    ) or "('__pgdm_empty__', NULL::text, 0::bigint)"

    scope_sql = ", ".join(_sql_text_literal(value) for value in plan.raw_schemas)
    expected_scope_array = f"ARRAY[{scope_sql}]::text[]"
    manifest_table = "pg_temp.pgdm_cross_table_hash_manifest"
    return f"""
CREATE TEMP TABLE {manifest_table} ON COMMIT DROP AS
SELECT * FROM (VALUES
    {values_sql}
) AS aggregate(raw_schema, raw_hash, row_count);

DO $pgdm_expansion_scope$
DECLARE
    missing_schemas text;
BEGIN
    SELECT string_agg(expected.raw_schema, ', ' ORDER BY expected.raw_schema)
    INTO missing_schemas
    FROM unnest({expected_scope_array}) AS expected(raw_schema)
    WHERE NOT EXISTS (
        SELECT 1
        FROM {manifest_table} AS actual
        WHERE actual.raw_schema = expected.raw_schema
    );

    IF missing_schemas IS NOT NULL THEN
        RAISE EXCEPTION
            'raw_schema has no source rows: %.',
            missing_schemas;
    END IF;
END
$pgdm_expansion_scope$;

SELECT json_build_object(
    'raw_schemas', to_json({expected_scope_array}),
    'expected_row_count', COALESCE((
        SELECT SUM(row_count)
        FROM {manifest_table}
    ), 0),
    'expected_distinct_raw_hash_count', (
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT raw_hash
            FROM {manifest_table}
        ) AS distinct_hashes
    ),
    'raw_hash_manifest', COALESCE((
        SELECT json_agg(
            json_build_object(
                'raw_schema', raw_schema,
                'raw_hash', raw_hash,
                'row_count', row_count
            )
            ORDER BY raw_schema, raw_hash NULLS FIRST
        )
        FROM {manifest_table}
    ), '[]'::json),
    'source_columns', {source_columns_json}::json
)::text;
""".strip()

# ---------------------------------------------------------------------------
# Job checkpointing and failure recovery
# ---------------------------------------------------------------------------

EXPANSION_CONTROL_SCHEMA = "pgdm_expansion_control"

JOB_TABLES_DDL = """
CREATE SCHEMA IF NOT EXISTS pgdm_expansion_control;

CREATE TABLE IF NOT EXISTS pgdm_expansion_control.expansion_jobs (
    job_id text PRIMARY KEY,
    dependency_manifest jsonb,
    database_name text NOT NULL,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    raw_schemas jsonb NOT NULL,
    destinations jsonb NOT NULL,
    recipes_hash text NOT NULL,
    status text NOT NULL,
    worker_count integer,
    requested_by text,
    workstation_name text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS pgdm_expansion_control.expansion_job_partitions (
    job_id text NOT NULL REFERENCES pgdm_expansion_control.expansion_jobs(job_id)
        ON DELETE CASCADE,
    partition_index integer NOT NULL,
    partition_name text NOT NULL,
    raw_id_lo bigint NOT NULL,
    raw_id_hi bigint NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    staged_rows bigint,
    manifest jsonb,
    insert_seconds double precision,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (job_id, partition_index)
);
"""

ACTIVE_JOB_STATUSES = ("staging", "writing")
FINISHED_JOB_STATUSES = ("data_committed", "registered", "complete", "abandoned")


def compute_recipes_hash(prepared: dict) -> str:
    import hashlib

    payload = json_dumps_compat(
        {
            "source_schema": prepared["source_schema_name"],
            "source_table": prepared["source_table_name"],
            "raw_schemas": list(prepared["raw_schemas"]),
            "destinations": [
                {
                    "table_name": destination["table_name"],
                    "statements": destination["statements"],
                }
                for destination in prepared["destinations"]
            ],
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def json_dumps_compat(value, sort_keys: bool = False) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, sort_keys=sort_keys)


class ExpansionJobManager:
    """Persistent checkpoint state for interruptible expansions.

    Job state lives in the data database next to the managed row counters, so
    the final expansion transaction can mark the job ``data_committed`` in the
    same commit as the destination rows: a crash can then never leave the
    destinations written without the job record proving it (or vice versa).
    """

    def __init__(self, service, database_name: str):
        self.service = service
        self.database_name = database_name

    def _psql(self) -> str:
        return (
            f"psql -h localhost -U {shlex_quote(self.service.sql_username)} "
            f"-d {shlex_quote(self.database_name)} -X -qAt -v ON_ERROR_STOP=1 -f -"
        )

    def _run(self, sql: str) -> str:
        return self.service.run_remote_command(self._psql(), stdin_text=sql)

    def ensure_tables(self) -> None:
        self._run("SET client_min_messages = warning;\n" + JOB_TABLES_DDL + ";")

    def find_resumable_job(
        self,
        source_schema: str,
        source_table: str,
        recipes_hash: str,
    ) -> dict | None:
        sql = f"""
SELECT COALESCE(json_agg(job_row)::text, '[]'::text) FROM (
    SELECT job_id, status, created_at::text AS created_at, worker_count
    FROM {EXPANSION_CONTROL_SCHEMA}.expansion_jobs
    WHERE database_name = {_sql_text_literal(self.database_name)}
      AND source_schema = {_sql_text_literal(source_schema)}
      AND source_table = {_sql_text_literal(source_table)}
      AND recipes_hash = {_sql_text_literal(recipes_hash)}
      AND status IN ('staging', 'writing', 'data_committed')
    ORDER BY created_at DESC
    LIMIT 1
) job_row;
"""
        output = self._run(sql).strip()
        try:
            jobs = json.loads(output.splitlines()[-1] if output else "[]")
        except Exception:
            jobs = []
        if not jobs:
            return None
        job = jobs[0]
        job["partitions"] = self._load_partitions(job["job_id"])
        return job

    def _load_partitions(self, job_id: str) -> list[dict]:
        sql = f"""
SELECT COALESCE(
    json_agg(
        json_build_object(
            'partition_index', partition_index,
            'partition_name', partition_name,
            'raw_id_lo', raw_id_lo,
            'raw_id_hi', raw_id_hi,
            'status', status,
            'staged_rows', staged_rows,
            'manifest', manifest
        )
        ORDER BY partition_index
    )::text,
    '[]'::text
)
FROM {EXPANSION_CONTROL_SCHEMA}.expansion_job_partitions
WHERE job_id = {_sql_text_literal(job_id)};
"""
        output = self._run(sql).strip()
        try:
            return json.loads(output.splitlines()[-1] if output else "[]")
        except Exception:
            return []

    def cleanup_orphaned_staging(self, keep_job_id: str) -> None:
        """Drop staging tables that belong to no live expansion job.

        Killed runs can leave unlogged staging tables behind; they are
        reclaimed here so interrupted-work recovery never accumulates disk.
        """
        sql = (
            "WITH live_jobs AS ("
            "SELECT job_id FROM pgdm_expansion_control.expansion_jobs "
            "WHERE status IN ('staging', 'writing', 'data_committed')"
            "), orphan_tables AS ("
            "SELECT c.relname FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'public' "
            "AND c.relname LIKE 'pgdm_stage_%' "
            "AND split_part(c.relname, '_', 3) NOT IN ("
            "SELECT job_id FROM live_jobs UNION ALL SELECT '"
            + keep_job_id
            + "')) "
            "SELECT COALESCE(string_agg(quote_ident(relname), ','), '') FROM orphan_tables;"
        )
        output = self._run(sql).strip()
        names = output.splitlines()[-1].strip() if output else ""
        if names:
            self._run("DROP TABLE IF EXISTS " + names + ";")

    def create_job(
        self,
        plan: OptimizedExpansionPlan,
        worker_count: int,
        requested_by: str,
        workstation_name: str,
        ranges: list[tuple[int, int]],
    ) -> dict:
        import uuid

        job_id = uuid.uuid4().hex[:12]
        partition_ddl = []
        for index, (raw_id_lo, raw_id_hi) in enumerate(ranges):
            partition_name = f"public.pgdm_stage_{job_id}_p{index}"
            partition_ddl.append(
                f"({index}, {_sql_text_literal(partition_name)}, "
                f"{int(raw_id_lo)}, {int(raw_id_hi)}, 'pending')"
            )
        sql = f"""
BEGIN;
SET LOCAL client_min_messages = warning;
INSERT INTO {EXPANSION_CONTROL_SCHEMA}.expansion_jobs (
    job_id, database_name, source_schema, source_table, raw_schemas,
    destinations, recipes_hash, status, worker_count, requested_by,
    workstation_name
) VALUES (
    {_sql_text_literal(job_id)},
    {_sql_text_literal(self.database_name)},
    {_sql_text_literal(plan.source_schema)},
    {_sql_text_literal(plan.source_table)},
    {_sql_text_literal(json_dumps_compat(list(plan.raw_schemas)))}::jsonb,
    {_sql_text_literal(json_dumps_compat(plan.destinations_payload))}::jsonb,
    {_sql_text_literal(plan.recipes_hash)},
    'staging',
    {int(worker_count)},
    {_sql_text_literal(requested_by)},
    {_sql_text_literal(workstation_name)}
);
INSERT INTO {EXPANSION_CONTROL_SCHEMA}.expansion_job_partitions (
    job_id, partition_index, partition_name, raw_id_lo, raw_id_hi, status
) VALUES
{",\n".join(f"({_sql_text_literal(job_id)}, " + row[1:-1] + ")" for row in partition_ddl)};
COMMIT;
"""
        self._run(sql)
        return {
            "job_id": job_id,
            "status": "staging",
            "resumed": False,
            "partitions": [
                {
                    "partition_index": index,
                    "partition_name": f"public.pgdm_stage_{job_id}_p{index}",
                    "raw_id_lo": raw_id_lo,
                    "raw_id_hi": raw_id_hi,
                    "status": "pending",
                }
                for index, (raw_id_lo, raw_id_hi) in enumerate(ranges)
            ],
        }

    def checkpoint_partition(self, job_id: str, aggregate: dict) -> None:
        partition_index = int(aggregate.get("worker", 0))
        manifest = aggregate.get("manifest") or []
        sql = (
            "UPDATE pgdm_expansion_control.expansion_job_partitions\n"
            "SET status = 'staged',\n"
            f"    staged_rows = {int(aggregate.get('rows') or 0)},\n"
            f"    manifest = {_sql_text_literal(json_dumps_compat(manifest))}::jsonb,\n"
            f"    insert_seconds = {float(aggregate.get('insert_seconds') or 0.0)},\n"
            "    updated_at = clock_timestamp()\n"
            "WHERE job_id = " + _sql_text_literal(job_id) + "\n"
            "  AND partition_index = " + str(partition_index) + ";"
        )
        self._run(sql)

    def reset_partition(self, job_id: str, partition_index: int) -> None:
        sql = (
            "UPDATE pgdm_expansion_control.expansion_job_partitions\n"
            "SET status = 'pending', staged_rows = NULL, manifest = NULL,\n"
            "    updated_at = clock_timestamp()\n"
            "WHERE job_id = " + _sql_text_literal(job_id) + "\n"
            "  AND partition_index = " + str(partition_index) + ";\n"
            "DROP TABLE IF EXISTS public.pgdm_stage_" + job_id + "_p"
            + str(partition_index) + ";"
        )
        self._run(sql)

    def mark_status(self, job_id: str, status: str) -> None:
        sql = (
            "UPDATE pgdm_expansion_control.expansion_jobs\n"
            "SET status = " + _sql_text_literal(status)
            + ", updated_at = clock_timestamp()\n"
            "WHERE job_id = " + _sql_text_literal(job_id) + ";"
        )
        self._run(sql)

    def abandon_stale_jobs(
        self,
        source_schema: str,
        source_table: str,
        keep_job_id: str,
    ) -> None:
        sql = (
            "UPDATE pgdm_expansion_control.expansion_jobs\n"
            "SET status = 'abandoned', updated_at = clock_timestamp()\n"
            "WHERE database_name = " + _sql_text_literal(self.database_name) + "\n"
            "  AND source_schema = " + _sql_text_literal(source_schema) + "\n"
            "  AND source_table = " + _sql_text_literal(source_table) + "\n"
            "  AND status IN ('staging', 'writing')\n"
            "  AND job_id <> " + _sql_text_literal(keep_job_id) + ";"
        )
        self._run(sql)

    def mark_job_data_committed_sql(self, job_id: str, manifest_json: str) -> str:
        """SQL executed inside the final transaction, atomically with COMMIT.

        Stores the dependency manifest alongside the committed status so an
        interruption after the commit can still complete version registration
        without re-reading the (already dropped) staging tables.
        """
        return (
            "UPDATE pgdm_expansion_control.expansion_jobs "
            "SET status = 'data_committed', dependency_manifest = '"
            + str(manifest_json).replace("'", "''")
            + "'::jsonb, updated_at = clock_timestamp() "
            "WHERE job_id = " + _sql_text_literal(job_id) + ";"
        )

    def validate_staged_partitions(self, job: dict) -> None:
        """Re-verify checkpointed partitions; unlogged tables are truncated
        when PostgreSQL crashes, so any mismatch is reset to pending."""
        staged = [
            partition
            for partition in job.get("partitions") or []
            if partition.get("status") == "staged"
        ]
        if not staged:
            return
        existence_sql = (
            "SET client_min_messages = warning;\n"
            + "\n".join(
                "SELECT "
                + str(int(partition["partition_index"]))
                + ", (to_regclass('"
                + partition["partition_name"]
                + "') IS NOT NULL)::text;"
                for partition in staged
            )
        )
        output = self._run(existence_sql)
        existing_indexes = set()
        for line in output.splitlines():
            parts = [part.strip() for part in line.split("|")]
            if len(parts) >= 2 and parts[1].lower() in ("t", "true"):
                existing_indexes.add(int(parts[0]))

        existing = [
            partition
            for partition in staged
            if int(partition["partition_index"]) in existing_indexes
        ]
        bad_indexes = {
            int(partition["partition_index"])
            for partition in staged
            if int(partition["partition_index"]) not in existing_indexes
        }
        if existing:
            count_sql = (
                "SET client_min_messages = warning;\n"
                + "\n".join(
                    "SELECT "
                    + str(int(partition["partition_index"]))
                    + ", (SELECT COUNT(*) FROM "
                    + partition["partition_name"]
                    + ")::text;"
                    for partition in existing
                )
            )
            output = self._run(count_sql)
            for line in output.splitlines():
                parts = [part.strip() for part in line.split("|")]
                if len(parts) < 2:
                    continue
                index = int(parts[0])
                partition = next(
                    p for p in existing
                    if int(p["partition_index"]) == index
                )
                if int(parts[1]) != int(partition.get("staged_rows") or -1):
                    bad_indexes.add(index)

        for partition in staged:
            if int(partition["partition_index"]) in bad_indexes:
                self.reset_partition(
                    job["job_id"], int(partition["partition_index"])
                )
                partition["status"] = "pending"

    def load_job_manifest(self, job_id: str):
        sql = (
            "SELECT dependency_manifest::text FROM "
            "pgdm_expansion_control.expansion_jobs WHERE job_id = "
            + _sql_text_literal(job_id) + ";"
        )
        output = self._run(sql).strip()
        if not output or output in ("NULL", ""):
            return None
        import json

        try:
            return json.loads(output.splitlines()[-1])
        except Exception:
            return None
