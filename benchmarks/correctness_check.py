"""Correctness check: compare destination contents between expansion paths.

Runs the expansion twice on the freshly reset challenge database - once with
the classic path (PDM_EXPANSION_FASTPATH=0) and once with the fast path - and
compares the full contents of all six destination tables, the Manager row
counters, and the registered version metadata.
"""
from __future__ import annotations

import argparse
import hashlib
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


def dump_destinations(service: PostgresAdminService) -> dict:
    """Order-independent content digest + row counts for every destination."""
    output = {}
    for table in DESTINATIONS:
        columns_sql = (
            "SELECT string_agg(quote_ident(attribute.attname), ', ' ORDER BY attribute.attnum) "
            "FROM pg_attribute AS attribute "
            "JOIN pg_class AS relation ON relation.oid = attribute.attrelid "
            "JOIN pg_namespace AS ns ON ns.oid = relation.relnamespace "
            f"WHERE ns.nspname || '.' || relation.relname = '{table.split('.')[-1]}' "
            "AND ns.nspname = 'public' AND attribute.attnum > 0 AND NOT attribute.attisdropped"
        )
        command = (
            f"psql -h localhost -U {service.sql_username} -d kaggle_challenge "
            "-X -qAt -v ON_ERROR_STOP=1"
        )
        columns = service.run_remote_command(command, stdin_text=columns_sql).strip()
        digest_sql = (
            "SELECT COALESCE(MD5(string_agg(row_md5, E'\\n' ORDER BY row_md5)), 'empty') FROM ("
            f"SELECT MD5(ROW_TO_JSON(t)::text) AS row_md5 FROM (SELECT {columns} FROM {table}) t"
            ") s;"
        )
        digest = service.run_remote_command(command, stdin_text=digest_sql).strip()
        count_sql = f"SELECT COUNT(*) FROM {table};"
        count = service.run_remote_command(command, stdin_text=count_sql).strip()
        output[table] = {"rows": int(count), "content_md5": digest}
    counters_sql = (
        "SELECT json_object_agg(schema_name || '.' || table_name, row_count)::text "
        "FROM public.pgdm_table_row_counts;"
    )
    command = (
        f"psql -h localhost -U {service.sql_username} -d kaggle_challenge "
        "-X -qAt -v ON_ERROR_STOP=1"
    )
    output["row_counters"] = json.loads(
        service.run_remote_command(command, stdin_text=counters_sql).strip()
    )
    return output


def dump_versions(service: PostgresAdminService) -> dict:
    command = (
        f"psql -h localhost -U {service.sql_username} -d {service and 'postgres_data_manager'} "
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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--ssh-port", type=int, default=2222)
    parser.add_argument("--ssh-user", default="kaggleuser")
    parser.add_argument("--ssh-password", default="kaggle_dev_2026")
    parser.add_argument("--postgres-port", type=int, default=5432)
    parser.add_argument("--pg-user", default="kaggle")
    parser.add_argument("--pg-password", default="kaggle_dev_2026")
    args = parser.parse_args()

    service = PostgresAdminService()
    service.connect(
        host=args.host,
        ssh_port=args.ssh_port,
        ssh_username=args.ssh_user,
        ssh_password=args.ssh_password,
        postgres_port=args.postgres_port,
        sql_username=args.pg_user,
        sql_password=args.pg_password,
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
                print(f"    classic: {classic[table]}")
                print(f"    fastpath: {fastpath[table]}")
        counters_ok = classic["row_counters"] == fastpath["row_counters"]
        all_ok &= counters_ok
        print("  row counters: " + ("IDENTICAL" if counters_ok else "MISMATCH"))
        versions_ok = classic_versions == fastpath_versions
        print("  version registration: " + ("IDENTICAL" if versions_ok else "MISMATCH"))
        print("RESULT:", "PASS" if all_ok and counters_ok else "FAIL")
        return 0 if all_ok and counters_ok else 1
    finally:
        service.close()


if __name__ == "__main__":
    raise SystemExit(main())
