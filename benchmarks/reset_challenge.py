"""Reset the challenge database state between benchmark runs.

Empties the six destination tables, resets identity sequences, zeroes the
Manager row counters, and removes cross-table-expansion version records from
the control database.  Equivalent to regenerating the database from scratch
but much faster.  The raw_data source is never touched.
"""
from __future__ import annotations

import argparse
import shlex
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from services.postgres_service import (  # noqa: E402
    CONTROL_DB,
    CONTROL_TABLE_VERSION_DEPENDENCIES,
    CONTROL_TABLE_VERSIONS,
    PostgresAdminService,
)

DESTINATIONS = [
    "public.table1",
    "public.table2",
    "public.table3",
    "public.table4",
    "public.table5",
    "public.table6",
]

RESET_SQL = """
BEGIN;
SET LOCAL client_min_messages = warning;
CREATE TABLE IF NOT EXISTS public.pgdm_table_row_counts (
    schema_name text NOT NULL,
    table_name text NOT NULL,
    row_count bigint NOT NULL CHECK (row_count >= 0),
    initialized_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_reconciled_at timestamptz,
    PRIMARY KEY (schema_name, table_name)
);
TRUNCATE TABLE public.table1, public.table2, public.table3,
               public.table4, public.table5, public.table6
    RESTART IDENTITY;
INSERT INTO public.pgdm_table_row_counts (schema_name, table_name, row_count)
VALUES ('public', 'table1', 0), ('public', 'table2', 0), ('public', 'table3', 0),
       ('public', 'table4', 0), ('public', 'table5', 0), ('public', 'table6', 0)
ON CONFLICT (schema_name, table_name)
DO UPDATE SET row_count = 0, updated_at = clock_timestamp();
COMMIT;
""".strip()

CONTROL_RESET_SQL = """
BEGIN;
SET LOCAL client_min_messages = warning;
DELETE FROM {dependencies}
 WHERE table_version_id IN (
     SELECT id FROM {versions}
      WHERE operation_kind = 'cross_table_expand_v1'
        AND database_name = {database}
 );
DELETE FROM {versions}
 WHERE operation_kind = 'cross_table_expand_v1'
   AND database_name = {database};
COMMIT;
""".strip()


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
        service.run_remote_command(
            f"psql -h localhost -U {shlex.quote(service.sql_username)} "
            f"-d {shlex.quote(args.database)} -X -qAt -v ON_ERROR_STOP=1",
            stdin_text=RESET_SQL,
        )
        service.run_remote_command(
            f"psql -h localhost -U {shlex.quote(service.sql_username)} "
            f"-d {shlex.quote(CONTROL_DB)} -X -qAt -v ON_ERROR_STOP=1",
            stdin_text=CONTROL_RESET_SQL.format(
                dependencies=CONTROL_TABLE_VERSION_DEPENDENCIES,
                versions=CONTROL_TABLE_VERSIONS,
                database="'" + args.database.replace("'", "''") + "'",
            ),
        )
        print(f"Reset complete for {args.database}")
    finally:
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
