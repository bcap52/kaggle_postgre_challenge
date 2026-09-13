"""Headless expansion driver for benchmarking.

Runs the exact same code path as the GUI Expand flow
(``main._execute_cross_table_expansion_thread``) without opening a window, and
writes the timing report to disk.  This is the harness used for all local
benchmark results in the competition writeup.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from datetime import datetime
from pathlib import Path

def _env(name: str, default: str = "") -> str:
    """Benchmark connection settings come from the environment."""
    return os.getenv(name, default)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from services.postgres_service import PostgresAdminService  # noqa: E402
from ui.dialogs import ExpansionTimingReportDialog  # noqa: E402

RECIPE_DIR = PROJECT_ROOT / "SQL files"
RECIPE_DESTINATIONS = (
    ("01_table3.sql", "public.table3"),
    ("02_table4.sql", "public.table4"),
    ("03_table1.sql", "public.table1"),
    ("04_table5.sql", "public.table5"),
    ("05_table6.sql", "public.table6"),
    ("06_table2.sql", "public.table2"),
)


def load_destinations(version_title: str) -> list[dict]:
    destinations = []
    for file_name, table_name in RECIPE_DESTINATIONS:
        destinations.append(
            {
                "table_name": table_name,
                "version_title": version_title,
                "sql": (RECIPE_DIR / file_name).read_text(encoding="utf-8"),
            }
        )
    return destinations


def run(
    ssh_host: str,
    ssh_port: int,
    ssh_user: str,
    ssh_password: str,
    postgres_port: int,
    pg_user: str,
    pg_password: str,
    database: str,
    source_table: str,
    raw_schemas: list[str],
    version_title: str,
    out_dir: Path,
    label: str,
) -> dict:
    service = PostgresAdminService()
    service.connect(
        host=ssh_host,
        ssh_port=ssh_port,
        ssh_username=ssh_user,
        ssh_password=ssh_password,
        postgres_port=postgres_port,
        sql_username=pg_user,
        sql_password=pg_password,
    )
    try:
        destinations = load_destinations(version_title)
        timing_report = service.create_cross_table_expansion_timing_report(
            database,
            source_table,
            raw_schemas,
            [item["table_name"] for item in destinations],
        )

        wall_started = time.perf_counter()
        # Mirrors main.py's expansion thread (GUI-free).
        service.ensure_admin_schema()
        result = service.execute_cross_table_expansion(
            database_name=database,
            source_full_table_name=source_table,
            raw_schemas=raw_schemas,
            destinations=destinations,
            cancel_event=None,
            progress_callback=None,
            post_commit_callback=None,
            timing_report=timing_report,
        )
        service.register_cross_table_expansion_versions(
            expansion_result=result,
            requested_by="benchmark",
            workstation_name=socket.gethostname(),
            cancel_event=None,
            progress_callback=None,
            timing_report=timing_report,
        )
        wall_seconds = time.perf_counter() - wall_started

        timing_report["completed_at"] = (
            datetime.now().astimezone().isoformat(timespec="seconds")
        )
        measured_total = sum(
            max(float(item.get("seconds") or 0.0), 0.0)
            for item in timing_report.get("items", [])
            if item.get("include_in_total", True)
        )
        report_text = ExpansionTimingReportDialog.build_report_text(timing_report)

        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = f"{stamp}_{label}"
        (out_dir / f"{base}.json").write_text(
            json.dumps(
                {
                    "label": label,
                    "database": database,
                    "source_table": source_table,
                    "raw_schemas": raw_schemas,
                    "measured_processing_total": measured_total,
                    "wall_seconds": wall_seconds,
                    "source_rows": timing_report.get("source_rows"),
                    "total_rows_inserted": timing_report.get("total_rows_inserted"),
                    "destination_rows": timing_report.get("destination_rows"),
                    "items": timing_report.get("items"),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        (out_dir / f"{base}.txt").write_text(report_text, encoding="utf-8")
        print(
            f"[{label}] MEASURED PROCESSING TOTAL: {measured_total:.3f}s "
            f"(wall {wall_seconds:.1f}s, source_rows="
            f"{timing_report.get('source_rows'):,})"
        )
        return timing_report
    finally:
        service.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows-label", default="100k")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--ssh-port", type=int, default=2222)
    parser.add_argument("--ssh-user", default=_env("PDM_BENCH_SSH_USER", "kaggleuser"))
    parser.add_argument("--ssh-password", default=_env("PDM_BENCH_PASSWORD", ""))
    parser.add_argument("--postgres-port", type=int, default=5432)
    parser.add_argument("--pg-user", default=_env("PDM_BENCH_PG_USER", "kaggle"))
    parser.add_argument("--pg-password", default=_env("PDM_BENCH_PASSWORD", ""))
    parser.add_argument("--database", default="kaggle_challenge")
    parser.add_argument("--source", default="public.raw_data")
    parser.add_argument("--raw-schemas", nargs="+", default=["group001"])
    parser.add_argument("--label", default=None)
    parser.add_argument("--out-dir", default=str(PROJECT_ROOT / "benchmarks" / "results"))
    args = parser.parse_args()

    run(
        ssh_host=args.host,
        ssh_port=args.ssh_port,
        ssh_user=args.ssh_user,
        ssh_password=args.ssh_password,
        postgres_port=args.postgres_port,
        pg_user=args.pg_user,
        pg_password=args.pg_password,
        database=args.database,
        source_table=args.source,
        raw_schemas=args.raw_schemas,
        version_title=f"benchmark {args.rows_label}",
        out_dir=Path(args.out_dir),
        label=args.label or f"expansion_{args.rows_label}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
