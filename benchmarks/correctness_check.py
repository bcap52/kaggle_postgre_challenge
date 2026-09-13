"""Correctness check: compare destination contents between expansion paths.

Runs the expansion twice on the freshly reset challenge database - once with
the classic path (PDM_EXPANSION_FASTPATH=0) and once with the fast path - and
compares the logical content of all six destination tables, the Manager row
counters, and the registered version metadata.

Surrogate identity columns (``id_column_1``) are excluded from the comparison
because their values depend on the execution plan's join strategy.  Foreign
key references are translated to logical keys (``raw_id`` for table1
references, natural dimension keys for table3/table4 references).  The
comparison therefore verifies the relational content the recipes define,
independent of surrogate assignment.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from services.postgres_service import PostgresAdminService  # noqa: E402

DESTINATIONS = [
    "public.table1",
    "public.table2",
    "public.table3",
    "public.table4",
    "public.table5",
    "public.table6",
]

_DIGEST = (
    "SELECT COALESCE(MD5(string_agg(row_md5, E'\n' ORDER BY row_md5)), 'empty') "
    "FROM (SELECT MD5(ROW_TO_JSON(t)::text) AS row_md5 FROM ({inner}) t) s;"
)

LOGICAL_DIGEST_SQL = {
    # table3/table4: natural keys only (identity surrogate excluded).
    "public.table3": (
        "SELECT text_column_1, text_column_2, numeric_column_1, "
        "text_column_3, text_column_4 FROM public.table3"
    ),
    "public.table4": (
        "SELECT id_column_1, text_column_1, text_column_2 FROM public.table4"
    ),
    # table1: keyed by raw_id; FK to table3 translated to its natural key.
    "public.table1": (
        "SELECT t.id_column_3 AS raw_id, t.id_column_2, t.datetime_column_1, "
        "t.integer_column_1, t.integer_column_2, t.integer_column_3, "
        "t.integer_column_4, t.integer_column_5, t.id_column_6, "
        "t.integer_column_6, t.integer_column_7, "
        "d.text_column_1 AS d_text_1, d.text_column_2 AS d_text_2, "
        "d.text_column_3 AS d_text_3, d.text_column_4 AS d_text_4, "
        "d.numeric_column_1 AS d_num_1 "
        "FROM public.table1 t LEFT JOIN public.table3 d "
        "ON d.id_column_1 = t.id_column_4"
    ),
    "public.table5": (
        "SELECT m.id_column_3 AS raw_id, s.numeric_column_1, "
        "s.numeric_column_2, s.integer_column_1, s.integer_column_2, "
        "s.id_column_3 FROM public.table5 s "
        "JOIN public.table1 m ON m.id_column_1 = s.id_column_2"
    ),
    # table6: keyed through table1.raw_id; table4 FK is its natural smallint key.
    "public.table6": (
        "SELECT m.id_column_3 AS raw_id, s.id_column_3 "
        "FROM public.table6 s JOIN public.table1 m "
        "ON m.id_column_1 = s.id_column_2"
    ),
    # table2: keyed through table1.raw_id.
    "public.table2": (
        "SELECT m.id_column_3 AS raw_id, s.integer_column_1, "
        "s.integer_column_2, s.text_column_1, s.integer_column_3, "
        "s.integer_column_4, s.integer_column_5, s.integer_column_6 "
        "FROM public.table2 s JOIN public.table1 m "
        "ON m.id_column_1 = s.id_column_2"
    ),
}


def dump_destinations(service: PostgresAdminService) -> dict:
    """Logical content digest + row counts for every destination."""
    command = (
        f"psql -h localhost -U {service.sql_username} -d kaggle_challenge "
        "-X -qAt -v ON_ERROR_STOP=1"
    )

    def digest(sql: str) -> str:
        return service.run_remote_command(command, stdin_text=sql).strip()

    output = {}
    for table in DESTINATIONS:
        digest_sql = _DIGEST.format(inner=LOGICAL_DIGEST_SQL[table])
        output[table] = {
            "rows": int(digest(f"SELECT COUNT(*) FROM {table};")),
            "content_md5": digest(digest_sql),
        }
    counters_sql = (
        "SELECT json_object_agg(schema_name || '.' || table_name, row_count)::text "
        "FROM public.pgdm_table_row_counts;"
    )
    output["row_counters"] = json.loads(
        service.run_remote_command(command, stdin_text=counters_sql).strip()
    )
    return output


def dump_versions(service: PostgresAdminService) -> dict:
    command = (
        f"psql -h localhost -U {service.sql_username} -d postgres_data_manager "
        "-X -qAt -v ON_ERROR_STOP=1"
    )
    sql = (
        "SELECT COALESCE(json_object_agg(table_name, json_build_object("
        "'versions', version_count, 'deps', dep_count)), '{}'::json)::text FROM ("
        "SELECT v.table_name, COUNT(*) AS version_count, "
        "(SELECT COUNT(*) FROM app_control.table_version_dependencies d "
        " WHERE d.table_version_id = min(v.id)) AS dep_count "
        "FROM app_control.table_versions v WHERE v.database_name = 'kaggle_challenge' "
        "GROUP BY v.table_name) s;"
    )
    return json.loads(service.run_remote_command(command, stdin_text=sql).strip() or "{}")


def run_expansion(mode: str, label: str) -> None:
    reset = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "benchmarks" / "reset_challenge.py")],
        capture_output=True,
        text=True,
    )
    if reset.returncode != 0:
        print(reset.stdout[-2000:])
        print(reset.stderr[-2000:])
        raise SystemExit("reset failed")
    env = dict(os.environ)
    env["PDM_EXPANSION_FASTPATH"] = "1" if mode == "fastpath" else "0"
    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "benchmarks" / "expansion_driver.py"),
            "--label",
            label,
        ],
        capture_output=True,
        text=True,
        env=env,
    )
    tail = (result.stdout + result.stderr).strip().splitlines()[-3:]
    print(f"  [{mode}] " + " | ".join(tail))
    if result.returncode != 0:
        raise SystemExit(f"{mode} expansion failed")


def main() -> int:
    service = PostgresAdminService()
    service.connect(
        host="localhost", ssh_port=2222, ssh_username="kaggleuser",
        ssh_password="kaggle_dev_2026", postgres_port=5432,
        sql_username="kaggle", sql_password="kaggle_dev_2026",
    )
    try:
        print("Running classic expansion...")
        run_expansion("classic", "correctness_classic")
        classic = dump_destinations(service)
        classic_versions = dump_versions(service)
        print("Running fast-path expansion...")
        run_expansion("fastpath", "correctness_fastpath")
        fastpath = dump_destinations(service)
        fastpath_versions = dump_versions(service)

        all_ok = True
        for table in DESTINATIONS:
            same = classic[table] == fastpath[table]
            all_ok &= same
            print(
                f"  {table}: rows {classic[table]['rows']:,} vs "
                f"{fastpath[table]['rows']:,} -> "
                + ("IDENTICAL" if same else "MISMATCH")
            )
            if not same:
                print(f"    classic:  {classic[table]}")
                print(f"    fastpath: {fastpath[table]}")
        counters_ok = classic["row_counters"] == fastpath["row_counters"]
        all_ok &= counters_ok
        print("  row counters: " + ("IDENTICAL" if counters_ok else "MISMATCH"))
        versions_ok = classic_versions == fastpath_versions
        print("  version registration: " + ("IDENTICAL" if versions_ok else "MISMATCH"))
        print("RESULT:", "PASS" if all_ok and counters_ok and versions_ok else "FAIL")
        return 0 if all_ok and counters_ok and versions_ok else 1
    finally:
        service.close()


if __name__ == "__main__":
    raise SystemExit(main())
