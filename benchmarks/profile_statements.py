"""EXPLAIN (ANALYZE, BUFFERS) the six guarded expansion recipes in order.

Runs each destination INSERT inside a rolled-back transaction so the database
is unchanged, and prints the plans including trigger times.
"""
from __future__ import annotations

import argparse
import shlex
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from services.postgres_service import PostgresAdminService  # noqa: E402
from benchmarks.expansion_driver import load_destinations  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--ssh-port", type=int, default=2222)
    parser.add_argument("--ssh-user", default="kaggleuser")
    parser.add_argument("--ssh-password", default="kaggle_dev_2026")
    parser.add_argument("--postgres-port", type=int, default=5432)
    parser.add_argument("--pg-user", default="kaggle")
    parser.add_argument("--pg-password", default="kaggle_dev_2026")
    parser.add_argument("--database", default="kaggle_challenge")
    parser.add_argument("--source", default="public.raw_data")
    parser.add_argument("--out", default=None)
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
        prepared = service._prepare_cross_table_expansion(
            args.source,
            ["group001"],
            load_destinations("profile"),
        )
        chunks = [
            "BEGIN;\nSET LOCAL client_min_messages = warning;\n"
            "SET LOCAL max_parallel_workers_per_gather = 4;\n"
            "SET LOCAL work_mem = '256MB';\n"
        ]
        for destination in prepared["destinations"]:
            for index, statement in enumerate(destination["statements"], start=1):
                chunks.append(
                    f"\\echo === {destination['table_name']} statement {index} ===\n"
                    "EXPLAIN (ANALYZE, BUFFERS, SUMMARY ON, FORMAT TEXT)\n"
                    + statement
                    + ";\n"
                )
        chunks.append("ROLLBACK;")
        sql = "\n".join(chunks)
        command = (
            f"psql -h localhost -U {shlex.quote(service.sql_username)} "
            f"-d {shlex.quote(args.database)} -X -qAt -v ON_ERROR_STOP=1 -f -"
        )
        output = service.run_remote_command(command, stdin_text=sql)
        if args.out:
            Path(args.out).write_text(output, encoding="utf-8")
        else:
            print(output)
    finally:
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
