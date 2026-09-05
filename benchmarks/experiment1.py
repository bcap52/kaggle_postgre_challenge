"""Quick experiments for optimization hypotheses at dataset scale."""
from __future__ import annotations

import json
import re
import sys
import threading
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from services.postgres_service import PostgresAdminService  # noqa: E402

SVC_KW = dict(
    host="localhost", ssh_port=2222, ssh_username="kaggleuser",
    ssh_password="kaggle_dev_2026", postgres_port=5432,
    sql_username="kaggle", sql_password="kaggle_dev_2026",
)

FIELDS_SMALLINT = [f"integer_column_{i}" for i in range(2, 16)]
FIELDS_TEXT = [f"text_column_{i}" for i in range(1, 18)]
FIELDS_NUMERIC = [f"numeric_column_{i}" for i in range(1, 31)]


def svc():
    s = PostgresAdminService()
    s.connect(**SVC_KW)
    return s


def psql(s: PostgresAdminService, sql: str, db="kaggle_challenge", timed=True) -> str:
    cmd = "psql -h localhost -U kaggle -d kaggle_challenge -X -qAt -v ON_ERROR_STOP=1"
    if timed:
        marker = f"__T_{time.time_ns()}__="
        wrapped = (
            "pgdm_t0=$(date +%s%N); { " + cmd + "; }; rc=$?; "
            "pgdm_t1=$(date +%s%N); "
            f"printf '\\n{marker}%s\\n' \"$((pgdm_t1 - pgdm_t0))\" >&2; exit $rc"
        )
        out = s.run_remote_command(wrapped, stdin_text=sql)
        return out
    return s.run_remote_command(cmd, stdin_text=sql)


def timed_psql(s, sql, db="kaggle_challenge"):
    """Returns (output, server_side_seconds)."""
    sql = "SET client_min_messages = warning;\n" + sql
    cmd = f"psql -h localhost -U kaggle -d {db} -X -qAt -v ON_ERROR_STOP=1"
    marker = f"__T_{time.time_ns()}__="
    wrapped = (
        "pgdm_t0=$(date +%s%N); { " + cmd + "; }; rc=$?; "
        "pgdm_t1=$(date +%s%N); "
        f"printf '%s%s\\n' '{marker}' \"$((pgdm_t1 - pgdm_t0))\"; exit $rc"
    )
    t0 = time.perf_counter()
    out = s.run_remote_command(wrapped, stdin_text=sql)
    wall = time.perf_counter() - t0
    m = re.search(marker.replace("{", "{{") + r"(\d+)", out)
    server_ns = int(m.group(1)) if m else None
    return out, (server_ns / 1e9 if server_ns else None), wall


STAGE_DDL = """
DROP TABLE IF EXISTS public.pgdm_stage_exp1;
CREATE UNLOGGED TABLE public.pgdm_stage_exp1 AS
SELECT
    raw_id,
    raw_schema,
    {extractions}
FROM public.raw_data
WHERE raw_schema IN ('group001');
"""

def extraction_list():
    parts = ["NULLIF(BTRIM(raw -> 'values' ->> 'id_column_1'), '')::integer AS v_id_column_1"]
    for i in range(1, 18):
        parts.append(f"raw -> 'values' ->> 'text_column_{i}' AS v_text_column_{i}")
    parts.append("NULLIF(BTRIM(raw -> 'values' ->> 'datetime_column_1'), '')::timestamptz AS v_datetime_column_1")
    parts.append("NULLIF(BTRIM(raw -> 'values' ->> 'integer_column_1'), '')::numeric AS v_integer_column_1")
    for i in range(2, 16):
        parts.append(f"NULLIF(BTRIM(raw -> 'values' ->> 'integer_column_{i}'), '')::smallint AS v_integer_column_{i}")
    for i in range(1, 31):
        parts.append(f"NULLIF(BTRIM(raw -> 'values' ->> 'numeric_column_{i}'), '')::numeric AS v_numeric_column_{i}")
    return ",\n    ".join(parts)


def main():
    rows_arg = sys.argv[1] if len(sys.argv) > 1 else "100000"
    s = svc()
    try:
        # 0. sanity: table size
        out, server, wall = timed_psql(s, "SELECT count(*), pg_size_pretty(pg_total_relation_size('public.raw_data')) FROM public.raw_data;")
        print(f"raw_data: {out}")

        # 1. Single-connection typed staging extraction
        sql = STAGE_DDL.format(extractions=extraction_list())
        out, server, wall = timed_psql(s, sql)
        print(f"STAGING single: server={server}s wall={wall}s")
        out, server, wall = timed_psql(s, "SELECT pg_size_pretty(pg_total_relation_size('public.pgdm_stage_exp1'));")
        print(f"stage size: {out}")

        # 2. Parallel staging: 4 workers over raw_id ranges
        n, _ = out, None
        workers = 4
        out_max = s.run_remote_command("psql -h localhost -U kaggle -d kaggle_challenge -X -qAt -c \"SELECT max(raw_id) FROM public.raw_data\"")
        max_id = int(out_max.strip().splitlines()[-1])
        bounds = [(i * max_id // workers + 1, (i + 1) * max_id // workers) for i in range(workers)]
        results = {}
        errors = {}
        def worker(i, lo, hi):
            wsvc = svc()
            try:
                sql = f"""
DROP TABLE IF EXISTS public.pgdm_stage_exp1_p{i};
CREATE UNLOGGED TABLE public.pgdm_stage_exp1_p{i} AS
SELECT raw_id, raw_schema, {extraction_list()}
FROM public.raw_data
WHERE raw_schema IN ('group001') AND raw_id >= {lo} AND raw_id <= {hi};
"""
                _, server, wall = timed_psql(wsvc, sql)
                results[i] = (server, wall)
            except Exception as exc:
                errors[i] = str(exc)
            finally:
                wsvc.close()
        t0 = time.perf_counter()
        threads = [threading.Thread(target=worker, args=(i, lo, hi)) for i, (lo, hi) in enumerate(bounds)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        phase_wall = time.perf_counter() - t0
        print(f"STAGING parallel{workers}: phase_wall={phase_wall}s per_worker={results} errors={errors}")

        # 3. FK drop + insert + validate vs trigger path on table1
        # 3a. Baseline trigger insert (empty table1)
        s.run_remote_command("psql -h localhost -U kaggle -d kaggle_challenge -X -qAt -v ON_ERROR_STOP=1",
                             stdin_text="TRUNCATE public.table1, public.table2, public.table3, public.table4, public.table5, public.table6 RESTART IDENTITY CASCADE;")
        sql_t1_trigger = """
INSERT INTO public.table1 (
    id_column_2, text_column_1, datetime_column_1, text_column_2,
    integer_column_1, integer_column_2, integer_column_3, text_column_3,
    id_column_3, id_column_4, id_column_5, integer_column_4, integer_column_5,
    id_column_6, integer_column_6, integer_column_7
)
SELECT
    v_id_column_1, NULL::varchar, v_datetime_column_1 AT TIME ZONE 'America/Sao_Paulo', NULL::varchar,
    GREATEST(v_integer_column_2, v_integer_column_3, v_integer_column_4, v_integer_column_5,
             v_integer_column_6, v_integer_column_7, v_integer_column_8, v_integer_column_9,
             v_integer_column_10, v_integer_column_11, v_integer_column_12, v_integer_column_13,
             v_integer_column_14, v_integer_column_15),
    NULL::smallint,
    CASE WHEN v_numeric_column_2 > 0 THEN ROUND(v_numeric_column_2 * 1000)::integer ELSE NULL::integer END,
    NULL::varchar,
    raw_id,
    dimension_row.id_column_1,
    NULL::bigint, NULL::integer, NULL::integer, NULL::integer, NULL::integer,
    CASE WHEN v_numeric_column_1 > 0 THEN ROUND(v_numeric_column_1 * 1000)::integer ELSE NULL::integer END
FROM public.pgdm_stage_exp1 AS source_row
JOIN public.table3 AS dimension_row
  ON dimension_row.text_column_1 = BTRIM(source_row.v_text_column_1)
 AND dimension_row.text_column_2 = 'SYNTHETIC/REFERENCE'
 AND dimension_row.numeric_column_1 IS NULL
 AND dimension_row.text_column_3 = LEFT(UPPER(BTRIM(source_row.v_text_column_2)), 1)
 AND dimension_row.text_column_4 = RIGHT(UPPER(BTRIM(source_row.v_text_column_2)), 1);
"""
        # fill dims first
        s.run_remote_command("psql -h localhost -U kaggle -d kaggle_challenge -X -qAt -v ON_ERROR_STOP=1", stdin_text="""
INSERT INTO public.table3 (text_column_1, text_column_2, numeric_column_1, text_column_3, text_column_4)
SELECT DISTINCT BTRIM(raw -> 'values' ->> 'text_column_1'), 'SYNTHETIC/REFERENCE', NULL::numeric,
       LEFT(UPPER(BTRIM(raw -> 'values' ->> 'text_column_2')), 1), RIGHT(UPPER(BTRIM(raw -> 'values' ->> 'text_column_2')), 1)
FROM public.raw_data
WHERE BTRIM(raw -> 'values' ->> 'text_column_1') <> ''
  AND UPPER(BTRIM(raw -> 'values' ->> 'text_column_2')) ~ '^[A-Z]{2}$';
""")
        out, server, wall = timed_psql(s, sql_t1_trigger)
        print(f"TABLE1 from typed stage (triggers): server={server}s")
        out, _, _ = timed_psql(s, "SELECT count(*) FROM public.table1;")
        print(f"table1 rows: {out.strip().splitlines()[-1]}")

        # 3b. FK dropped, insert, re-validate
        s.run_remote_command("psql -h localhost -U kaggle -d kaggle_challenge -X -qAt -v ON_ERROR_STOP=1",
                             stdin_text="TRUNCATE public.table1, public.table2, public.table3, public.table4, public.table5, public.table6 RESTART IDENTITY CASCADE;")
        sql_fk = f"""
BEGIN;
SET LOCAL synchronous_commit = off;
ALTER TABLE public.table1 DROP CONSTRAINT table1_id_column_3_fkey;
ALTER TABLE public.table1 DROP CONSTRAINT table1_id_column_4_fkey;
{sql_t1_trigger}
ALTER TABLE public.table1 ADD CONSTRAINT table1_id_column_3_fkey
    FOREIGN KEY (id_column_3) REFERENCES public.raw_data(raw_id)
    ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;
ALTER TABLE public.table1 VALIDATE CONSTRAINT table1_id_column_3_fkey;
ALTER TABLE public.table1 ADD CONSTRAINT table1_id_column_4_fkey
    FOREIGN KEY (id_column_4) REFERENCES public.table3(id_column_1)
    ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;
ALTER TABLE public.table1 VALIDATE CONSTRAINT table1_id_column_4_fkey;
COMMIT;
"""
        out, server, wall = timed_psql(s, sql_fk)
        print(f"TABLE1 from typed stage (drop+validate): server={server}s")
        out, _, _ = timed_psql(s, "SELECT count(*) FROM public.table1;")
        print(f"table1 rows: {out.strip().splitlines()[-1]}")
    finally:
        s.close()


if __name__ == "__main__":
    main()
