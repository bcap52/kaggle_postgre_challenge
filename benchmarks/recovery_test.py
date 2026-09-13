"""End-to-end failure-recovery tests for the expansion fast path.

Scenario A: kill the expansion during staging; rerun; verify the operation
resumes from checkpoints and produces reference-identical output.

Scenario B: simulate a crash between the atomic data commit and version
registration; rerun; verify only version registration runs and the job
finishes without duplicating rows.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

def _env(name: str, default: str = "") -> str:
    """Benchmark connection settings come from the environment."""
    return os.getenv(name, default)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from services.postgres_service import PostgresAdminService  # noqa: E402
from benchmarks.correctness_check import (  # noqa: E402
    LOGICAL_DIGEST_SQL,
    _DIGEST,
)

DRIVER = PROJECT_ROOT / "benchmarks" / "expansion_driver.py"
RESET = PROJECT_ROOT / "benchmarks" / "reset_challenge.py"

DESTINATIONS = [f"public.table{i}" for i in (1, 2, 3, 4, 5, 6)]


def run(*args, env_extra=None, timeout=900, check=True):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    result = subprocess.run(
        [sys.executable, *[str(a) for a in args]],
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
    )
    if check and result.returncode != 0:
        print((result.stdout + result.stderr)[-3000:])
        raise SystemExit(f"command failed: {args}")
    return result


def reset(service=None):
    run(RESET)
    if service is not None:
        try:
            # Terminate lingering psql sessions from earlier killed runs and
            # clear all job state, so scenarios start from a clean slate.
            service.run_remote_command(
                _psql(service, "kaggle_challenge"),
                stdin_text=(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE pid <> pg_backend_pid() AND usename = 'kaggle' "
                    "AND application_name = 'psql';"
                    "DELETE FROM pgdm_expansion_control.expansion_job_partitions;"
                    "DELETE FROM pgdm_expansion_control.expansion_jobs;"
                ),
            )
        except Exception:
            pass


def _psql(service, database):
    return (
        f"psql -h localhost -U {service.sql_username} -d {database} "
        "-X -qAt -v ON_ERROR_STOP=1"
    )


def job_state(service, database="kaggle_challenge"):
    return json.loads(service.run_remote_command(
        _psql(service, database),
        stdin_text=(
            "SELECT COALESCE(json_agg(t)::text, '[]') FROM ("
            "SELECT job_id, status, worker_count, "
            "dependency_manifest IS NOT NULL AS has_manifest"
            " FROM pgdm_expansion_control.expansion_jobs ORDER BY created_at) t;"
        ),
    ).strip())


def partition_state(service, database="kaggle_challenge"):
    return json.loads(service.run_remote_command(
        _psql(service, database),
        stdin_text=(
            "SELECT COALESCE(json_agg(t)::text, '[]') FROM ("
            "SELECT job_id, partition_index, status, staged_rows"
            " FROM pgdm_expansion_control.expansion_job_partitions"
            " ORDER BY job_id, partition_index) t;"
        ),
    ).strip())


def destination_digests(service):
    """Logical (surrogate-independent) content digests per destination."""
    digests = {}
    for table in DESTINATIONS:
        digests[table] = service.run_remote_command(
            _psql(service, "kaggle_challenge"),
            stdin_text=_DIGEST.format(inner=LOGICAL_DIGEST_SQL[table]),
        ).strip()
    return digests


def version_count(service, job_id):
    output = service.run_remote_command(
        _psql(service, "postgres_data_manager"),
        stdin_text=(
            "SELECT COUNT(*) FROM app_control.table_version_dependencies "
            "WHERE dependency_payload->>'expansion_job_id' = '"
            + job_id + "';"
        ),
    ).strip()
    return int(output.splitlines()[-1])


def main() -> int:
    service = PostgresAdminService()
    service.connect(
        host="localhost", ssh_port=2222, ssh_username=_env("PDM_BENCH_SSH_USER", "kaggleuser"),
        ssh_password=_env("PDM_BENCH_PASSWORD", ""), postgres_port=5432,
        sql_username=_env("PDM_BENCH_PG_USER", "kaggle"), sql_password=_env("PDM_BENCH_PASSWORD", ""),
    )
    failures = []
    try:
        print("== Reference run ==")
        reset(service)
        run(DRIVER, "--label", "recovery_reference")
        reference = destination_digests(service)

        print("== Scenario A: interruption during staging ==")
        reset(service)
        # Deterministically construct an interrupted staging state: stage two
        # of four partitions, checkpoint them, and leave the job 'staging'.
        from services.expansion_optimizer import (  # noqa: E402
            ExpansionJobManager,
            _execute_staging_worker,
            plan_optimized_expansion,
        )
        from benchmarks.expansion_driver import load_destinations  # noqa: E402

        prepared = service._prepare_cross_table_expansion(
            "public.raw_data", ["group001"], load_destinations("recovery")
        )
        plan = plan_optimized_expansion(
            prepared,
            {item["name"]: item["type"] for item in service.get_table_column_definitions(
                "kaggle_challenge", "public.raw_data")},
        )
        manager = ExpansionJobManager(service, "kaggle_challenge")
        manager.ensure_tables()
        ranges = [(1, 250000), (250001, 500000), (500001, 750000), (750001, 1000000)]
        job = manager.create_job(plan, 4, "recovery-test", "localhost", ranges)
        plan.job_id = job["job_id"]
        for index in (0, 1):
            aggregate = _execute_staging_worker(
                service, plan, "kaggle_challenge", index, ranges[index][0], ranges[index][1]
            )
            manager.checkpoint_partition(job["job_id"], aggregate)
        partitions = partition_state(service)
        staged_now = sum(1 for p in partitions if p["status"] == "staged")
        pending_now = sum(1 for p in partitions if p["status"] == "pending")
        print(f"  interrupted job {job['job_id']}: staged={staged_now} pending={pending_now}")
        if staged_now != 2 or pending_now != 2:
            failures.append("interrupted staging state not constructed")

        run(DRIVER, "--label", "recovery_resume")
        reports = sorted(
            (PROJECT_ROOT / "benchmarks" / "results").glob("*recovery_resume.txt")
        )
        report_text = reports[-1].read_text(encoding="utf-8")
        resumed = "Resume interrupted expansion job" in report_text
        reused = "reused checkpoint" in report_text
        print(f"  resume reported: {resumed}; reused: {reused}")
        if not resumed:
            failures.append("staging resume not reported after interruption")
        if not reused:
            failures.append("checkpointed partitions were not reused")
        after_resume = destination_digests(service)
        if after_resume != reference:
            for table in DESTINATIONS:
                mark = "ok" if after_resume[table] == reference[table] else "DIFF"
                print(f"    {table}: {mark} {reference[table][:10]} vs {after_resume[table][:10]}")
            failures.append("post-resume output differs from reference")
        else:
            print("  post-resume output matches reference")

        print("== Scenario B: crash between commit and version registration ==")
        reset(service)
        run(DRIVER, "--label", "recovery_b_normal")
        jobs = job_state(service)
        job_id = jobs[-1]["job_id"]
        if jobs[-1]["status"] != "complete":
            failures.append(f"expected complete job, got {jobs[-1]['status']}")
        if version_count(service, job_id) != 6:
            failures.append("expected 6 registered versions before the simulated crash")

        # Simulate the crash: destination rows committed (untouched), version
        # registration lost, job stuck at data_committed.
        service.run_remote_command(
            _psql(service, "postgres_data_manager"),
            stdin_text=(
                "BEGIN;\n"
                "DELETE FROM app_control.table_version_dependencies "
                "WHERE dependency_payload->>'expansion_job_id' = '"
                + job_id + "';\n"
                "DELETE FROM app_control.table_versions "
                "WHERE operation_kind = 'cross_table_expand_v1' AND id NOT IN ("
                "SELECT table_version_id FROM app_control.table_version_dependencies);\n"
                "COMMIT;"
            ),
        )
        service.run_remote_command(
            _psql(service, "kaggle_challenge"),
            stdin_text=(
                "UPDATE pgdm_expansion_control.expansion_jobs "
                "SET status = 'data_committed' WHERE job_id = '" + job_id + "';"
            ),
        )
        before_b = destination_digests(service)

        run(DRIVER, "--label", "recovery_b_resume")
        reports = sorted(
            (PROJECT_ROOT / "benchmarks" / "results").glob("*recovery_b_resume.txt")
        )
        report_text = reports[-1].read_text(encoding="utf-8")
        recovered = (
            "committed-but-unregistered" in report_text
            or "Recovered committed expansion" in report_text
        )
        print(f"  post-commit recovery reported: {recovered}")
        if not recovered:
            failures.append("post-commit recovery not reported")
        after_b = destination_digests(service)
        if after_b != before_b:
            failures.append("destination rows duplicated/changed by recovery run")
        else:
            print("  destinations unchanged by recovery run (no duplicates)")
        if version_count(service, job_id) != 6:
            failures.append("version registration not restored after recovery")
        else:
            print("  versions restored: 6")
        jobs = job_state(service)
        if jobs[-1]["status"] != "complete":
            failures.append(f"job not complete after recovery: {jobs[-1]['status']}")
        else:
            print("  job marked complete")
    finally:
        service.close()

    print("RESULT:", "PASS" if not failures else "FAIL: " + "; ".join(failures))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
