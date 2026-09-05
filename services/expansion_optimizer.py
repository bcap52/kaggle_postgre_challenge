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
) -> dict:
    """Build the staging partitions in parallel sessions.

    Returns the partition metadata consumed by the final batch builder.
    """
    if worker_count is None:
        worker_count = default_worker_count()

    bounds_sql = (
        "SELECT COALESCE(MIN(raw_id), 0)::text, COALESCE(MAX(raw_id), 0)::text "
        f"FROM {plan.source_table_sql};"
    )
    command = (
        f"psql -h localhost -U {shlex_quote(service.sql_username)} "
        f"-d {shlex_quote(database_name)} -X -qAt -v ON_ERROR_STOP=1 "
        f"-c {shlex_quote(bounds_sql)}"
    )
    bounds_output = service.run_remote_command(command, cancel_event=cancel_event).strip()
    bounds = bounds_output.splitlines()[-1].split("|") if bounds_output else ["0", "0"]
    min_raw_id, max_raw_id = int(bounds[0] or 0), int(bounds[1] or 0)

    phase_started = time.perf_counter()
    if max_raw_id <= 0 or worker_count <= 1:
        worker_count = 1
        ranges = [(min_raw_id, max_raw_id)]
    else:
        span = max_raw_id - min_raw_id + 1
        edges = [min_raw_id + (span * index) // worker_count for index in range(worker_count + 1)]
        ranges = [
            (edges[index], edges[index + 1] - 1 if index + 1 < len(edges) else max_raw_id)
            for index in range(worker_count)
        ]
        ranges = [(lo, hi) for lo, hi in ranges if lo <= hi]

    results: dict[int, dict] = {}
    errors: dict[int, str] = {}

    def execute_worker(index: int, raw_id_lo: int, raw_id_hi: int) -> None:
        try:
            results[index] = _execute_staging_worker(
                service, plan, database_name, index, raw_id_lo, raw_id_hi, cancel_event
            )
        except Exception as exc:  # noqa: BLE001 - reported per worker below
            errors[index] = str(exc)

    threads = [
        threading.Thread(
            target=execute_worker,
            args=(index, raw_id_lo, raw_id_hi),
            name=f"pgdm-staging-{index}",
            daemon=True,
        )
        for index, (raw_id_lo, raw_id_hi) in enumerate(ranges)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    phase_seconds = max(time.perf_counter() - phase_started, 0.0)

    if errors:
        detail = "; ".join(f"worker {index}: {message}" for index, message in errors.items())
        raise RuntimeError(f"The staging extraction failed. {detail}")

    ordered = [results[index] for index in sorted(results)]
    if timing_report is not None:
        service._append_expansion_timing(
            timing_report,
            "staging",
            f"Parallel staging extraction ({len(ranges)} worker session(s))",
            phase_seconds,
            "orchestrated_phase_wall",
            rows=sum(int(item.get("rows") or 0) for item in ordered),
            details=(
                "Wall time of the parallel staging phase; each worker session is "
                "clocked by the Linux server. SSH connection and return transit "
                "are excluded. Per-worker server times are listed below as "
                "diagnostics."
            ),
        )
        for index, item in enumerate(ordered):
            service._append_expansion_timing(
                timing_report,
                "staging",
                f"Staging worker {item.get('worker', index)} (diagnostic)",
                max(float(item.get("server_seconds") or 0.0), 0.0),
                "remote_server_wall",
                rows=item.get("rows"),
                destination=item.get("partition_name"),
                details=(
                    f"raw_id {item.get('raw_id_lo')}..{item.get('raw_id_hi')}; "
                    "insert clocked inside PostgreSQL: "
                    f"{float(item.get('insert_seconds') or 0.0):.3f}s. "
                    "Diagnostic only; excluded from totals."
                ),
                include_in_total=False,
            )

    return {
        "partitions": [item["partition_name"] for item in ordered],
        "aggregates": ordered,
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