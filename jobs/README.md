# Yeedu Job-Type Demos

Worked examples of Yeedu Spark **jobs** (as distinct from Functions and
notebooks) covering the remaining `job_type` values: `Jar`, `Python3`, and
`SQL` (CLI display names: `JAR`, `Python`, `Spark SQL`). See
[`functions/`](../functions/) for the fourth type (`Functions`) and
[`notebooks/`](../notebooks/) for interactive notebook-as-a-job.

All three query the same demo data already set up in `notebooks/` — the
digital marketing customer RFM segmentation table
(`retail.customer_rfm_segments_v1`) — so the only thing that differs
between them is the job type mechanics, not the use case.

## Demos

### [`jar/`](jar/README.md) — Table Summary (JAR)
`table-summary-job-1.0.jar` — a thin, repo-committed jar (Java): prints a
table's row count and top rows. Takes the table name as its one argument
(`job_arguments`).

### [`python/`](python/README.md) — Table Summary (Python3)
`table_summary_job.py` — a plain (non-notebook) PySpark script, same idea
as the JAR demo, different job type.

### [`sql/`](sql/README.md) — Table Summary (SQL)
`table_summary.sql` — the Spark SQL equivalent: a single `SELECT` against
the same table, run directly as a `job_type: SQL` job (no Python/JVM code
at all).

## Deploying

`automation/deploy_other_jobs.py` creates all three as part of
`automation/provision.py`'s run — see `automation/README.md` for the exact
`job_command`/`job_class_name` values used and what's confirmed vs.
assumed (Python3/SQL `job_command` semantics aren't shown anywhere in
Yeedu's docs, only JAR is — see that README's "Known gaps").
