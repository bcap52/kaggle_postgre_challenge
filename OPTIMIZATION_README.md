# Optimized expansion architecture — submission notes

This repository optimizes the PostgreSQL Data Manager's cross-table **Expand**
operation for the Kaggle competition *Python & PostgreSQL Data Pipeline
Optimization Challenge*.  The baseline implementation executes every expansion
recipe directly against the JSONB source, so each destination statement
re-detoasts and re-parses the whole ``raw`` column (seven full JSONB scans for
the six-destination challenge workload).  This submission replaces the
pipeline internals with a parallel staging architecture while preserving every
functional guarantee of the original operation.

## What was changed

### 1. Single-pass parallel staging (`services/expansion_optimizer.py`)

* A generic analyzer parses every recipe and finds each
  ``<alias>.raw -> 'values' ->> '<field>'`` access.
* The fields are extracted **exactly once** into unlogged staging tables built
  by parallel worker sessions over ``raw_id`` ranges (primary-key index range
  scans).  Fields whose occurrences are all wrapped in the same
  ``NULLIF(BTRIM(x), '')::<type>`` cast are staged pre-cast.
* Each recipe is rewritten to read the staged columns.  The rewrite is
  byte-transparent in semantics: a staged column holds exactly the JSONB text
  (or the cast result) the recipe would have read.  Typed values flow through
  LATERAL ``VALUES`` columns without the text round-trip.
* Any statement that does not match the supported patterns makes the whole
  expansion fall back to the classic path — the Manager stays fully generic.

### 2. Consolidated preflight

All preflight inspections (source existence/columns, SQL function safety,
managed dependency validation, row counters) run as a single server-side
psql script: identical checks and error semantics, one SSH round trip instead
of ~24.

### 3. Bulk-load constraint strategy

When every destination is empty, destination foreign keys, redundant unique
constraints and secondary indexes are dropped before the inserts and rebuilt
and **validated inside the same expansion transaction**.  The final schema and
the validated guarantees are identical; non-empty destinations keep their
per-row constraint checks.

### 4. Dependency manifest from staging aggregates

The per-worker extraction also aggregates ``(raw_schema, raw_hash, count)``,
so the batch no longer performs an extra full scan of the source to build the
dependency manifest.

### 5. Resumable expansion jobs (failure recovery)

Job state lives in ``pgdm_expansion_control`` inside the data database:

* Each staging worker is checkpointed (partition status, row count, manifest)
  as soon as it finishes.  An interrupted run resumes: checkpointed partitions
  are validated and reused, incomplete ones are re-extracted.  Unlogged
  staging tables that PostgreSQL truncated after a crash are detected via the
  row-count check and re-staged.
* The final transaction marks the job ``data_committed`` **in the same
  commit** as the destination rows, and stores the dependency manifest.
  A crash between commit and version registration therefore leaves a
  provable state that the next run resolves by re-registering versions only —
  without duplicating rows.
* Version registration is idempotent per job id; the job is marked
  ``complete`` after registration.
* Orphaned staging tables from killed runs are reclaimed automatically.
* ``PDM_EXPANSION_RESUME_MODE=fresh`` forces a clean restart;
  ``PDM_EXPANSION_RESUME_MODE=auto`` (default) resumes matching jobs.

### 6. Session tuning inside the measured transaction

``synchronous_commit = off`` for the expansion transaction, explicit
``max_parallel_workers_per_gather = 0`` so destination inserts keep a
deterministic row order, and ``maintenance_work_mem`` for constraint rebuilds.

## What was deliberately NOT changed

* The official SQL recipes, their destination order and mapping.
* Source validation, destination validation, SQL function safety validation,
  the protected single-transaction commit, row counters, version
  registration, dependency manifests — all preserved (consolidated where that
  removes round trips).
* The Manager remains deployable in the required Windows → SSH →
  Linux/PostgreSQL architecture; the GUI flow is unchanged.

## Local benchmark results (informative, not official)

Hardware: Windows 11 host, 12 logical cores, 16 GB RAM; PostgreSQL 17.11
inside WSL2 Ubuntu 22.04 (7.7 GB), DB Manager on Windows, SSH localhost:2222.
Numbers are MEASURED PROCESSING TOTAL medians of three runs
(``benchmarks/expansion_driver.py``; reports in ``benchmarks/results/``).

| Dataset  | Baseline (classic) | Optimized | Speed-up |
|----------|-------------------:|----------:|---------:|
| 100k     |          10.0 s    |  5.3 s    |   1.9×   |
| 1M       |         129.3 s    | 36.3 s    |   3.6×   |

Staging storage is transient (unlogged, dropped inside the measured
transaction): ≈16 MB per 100k source rows (≈8–12× smaller than the JSONB
source).  Peak additional RAM is bounded by ``work_mem``/``maintenance_work_mem``
session settings.

## Reproducing

```powershell
# 1. Linux server: PostgreSQL 17 + OpenSSH (see repository README)
# 2. Windows:
python -m pip install -r requirements.txt
$env:PG_KAGGLE_CHALLENGE_PASS = "<postgres_password>"
python generate_database.py --rows 100000 --host <linux_ip> --ssh_port 22 `
  --ssh_user <linux_user> --postgres_port 5432 --user <postgres_user> `
  --admin_database postgres
python main.py    # connect, open kaggle_challenge.public.raw_data, Expand
```

Headless reproduction of the benchmarks and tests:

```powershell
python -m unittest tests.test_expansion_timing tests.test_challenge_expansion_recipes tests.test_expand_ui_english
python benchmarks/correctness_check.py   # classic vs fast path logical content
python benchmarks/recovery_test.py       # interruption + recovery scenarios
python benchmarks/expansion_driver.py    # measured run + timing report
```

Environment knobs: ``PDM_EXPANSION_WORKERS`` (default ``min(8, cores-1)``),
``PDM_EXPANSION_FASTPATH=0`` (classic path), ``PDM_EXPANSION_RESUME_MODE``.
