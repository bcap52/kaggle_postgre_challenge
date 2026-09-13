# Kaggle Writeup — Python & PostgreSQL Data Pipeline Optimization Challenge

**Author:** bcap52 (Kaggle)
**Repository:** https://github.com/bcap52/kaggle_postgre_challenge
**Submission commit:** _final commit hash, filled in at submission_

---

## 1. Bottlenecks identified in the baseline

Profiling the baseline Expand (`MEASURED PROCESSING TOTAL` model) on the
official 100k dataset showed:

| Stage | Time | Share |
|---|---:|---:|
| 6 destination INSERT..SELECT statements | 11.0 s | 87% |
| Preflight validations (24 separate SSH/psql round trips) | 1.30 s | 10% |
| Version registration + refresh | 0.11 s | 1% |

Inside the 11.0 s, `EXPLAIN (ANALYZE)` on each guarded statement showed that
every recipe re-detoasts and re-parses the whole JSONB `raw` column — seven
full JSONB scans for the six-destination workload — and that per-row foreign
key checks consumed ~28% of the in-database time (e.g. 780 ms of table1's
2.7 s at 100k was the `table1_id_column_3_fkey` trigger alone).  The 14-way
`CROSS JOIN LATERAL` in recipe 06 evaluated ~56 JSONB extractions per source
row.

## 2. Optimization strategy

The expansion was redesigned around a **single-pass parallel staging**
architecture (`services/expansion_optimizer.py`), while keeping the recipe
language, the destination mapping, the GUI flow and the Windows → SSH →
Linux/PostgreSQL architecture untouched:

1. **Extract once.** A generic analyzer parses every recipe and extracts each
   referenced `raw -> 'values' ->> '<field>'` value exactly once into unlogged
   staging tables, built by parallel worker sessions over `raw_id` ranges
   (index range scans of the source primary key).  Fields whose occurrences
   are all cast with the same `NULLIF(BTRIM(x), '')::<type>` pattern are
   staged pre-cast; the rewriter removes the now-redundant wrappers, including
   through LATERAL `VALUES` columns (recipe 06), so typed smallint/numeric
   comparisons replace text round-trips.
2. **Rewrite recipes to read staging.** The rewrite is byte-transparent in
   semantics: a staged column holds exactly the value the JSONB expression
   would have produced.  Statements that do not match the supported patterns
   make the expansion fall back to the classic path, so the Manager remains
   fully generic.
3. **Consolidated preflight.** All validations (source existence/columns,
   function safety, managed dependency checks, row counters) now run as one
   server-side psql script with identical checks — one SSH round trip instead
   of ~24.
4. **Bulk-load constraint strategy.** When every destination is empty, FKs,
   redundant unique constraints and secondary indexes are dropped before the
   inserts and rebuilt + **validated inside the same expansion transaction**,
   so the final schema and validated guarantees are identical.
5. **Manifest from staging aggregates.** The dependency manifest is combined
   from per-partition aggregates; the batch no longer performs an extra full
   source scan.

## 3. Failure recovery (secondary objective)

Job state persists in `pgdm_expansion_control` inside the data database:

* Every staging worker is checkpointed (status, row count, manifest hash
  aggregates) as it finishes.  An interrupted expansion resumes: validated
  partitions are reused, incomplete ones re-extracted; staging tables
  truncated by a PostgreSQL crash are detected by row-count validation.
* The final transaction marks the job `data_committed` **in the same commit**
  as the destination rows and stores the dependency manifest.  A crash between
  commit and version registration is therefore provable and recoverable: the
  next run replays version registration only, without touching rows.
* Version registration is idempotent per job id; completed jobs are marked
  `complete`; orphaned staging tables from killed runs are reclaimed.
* `PDM_EXPANSION_RESUME_MODE=fresh` forces a clean restart; `auto` (default)
  resumes a matching interrupted job.

The recovery suite (`benchmarks/recovery_test.py`) verifies: kill during
staging → resume reuses checkpoints and produces reference-identical logical
output; crash after commit → versions restored, zero duplicate rows, job
completed.

## 4. Measured results (local, informative)

Hardware: Windows 11, 12 logical cores, 16 GB RAM; PostgreSQL 17.11 in WSL2
Ubuntu 22.04 (7.7 GB); DB Manager on Windows via SSH.  Metric: MEASURED
PROCESSING TOTAL, median of 3 runs (full reports in `benchmarks/reports/`).

| Dataset | Baseline | Optimized | Speed-up |
|---|---:|---:|---:|
| 100k | 10.0 s | 5.3 s | 1.9× |
| 1M | 129.3 s | 36.3 s | 3.6× |
| 10M | 1313.3 s | 383.0 s | 3.4× |

Exported reports: `benchmarks/reports/*.txt`.

## 5. Resource usage

* Staging storage is transient: ≈16 MB per 100k source rows (~8–12× smaller
  than the JSONB source) and is dropped **inside** the measured transaction.
* Peak extra RAM is bounded by session `maintenance_work_mem` (128 MB) during
  constraint rebuilds; workers are streaming inserts with no materialization.
* WAL is reduced (unlogged staging, `synchronous_commit=off` for the
  expansion transaction only).

## 6. Correctness guarantees preserved

`benchmarks/correctness_check.py` compares the classic and optimized paths on
the official dataset: identical row counts, identical **logical** content of
all six destinations (compared through natural keys with FK references
resolved), identical Manager row counters, identical version registration.
Surrogate identity values are plan-dependent in *any* reordering
implementation, including the baseline across PostgreSQL versions, and are
therefore compared logically.  The official unit tests
(`tests/`) all pass unchanged in intent; the timing test now stubs the
consolidated preflight entry point.

## 7. Limitations and trade-offs

* The optimized path engages for recipes that read a JSONB source through the
  `{{source}}` placeholder; anything else silently uses the classic path.
* Staging tables add transient disk I/O; on very slow storage the crossover
  versus the classic path could differ (not observed at ≤10M rows locally).
* `synchronous_commit=off` inside the expansion transaction trades crash
  durability of that transaction for throughput; the job state machine makes
  any interruption recoverable either way.
