# Yeedu Job-Type Demos

Worked examples of Yeedu Spark **jobs** (as distinct from Functions and
notebooks) covering the remaining `job_type` values: `Jar`, `Python3`,
`SQL`, and `Custom Code` (Raw Scala) (CLI display names: `JAR`, `Python`,
`Spark SQL`, `Custom Code`). See [`functions/`](../functions/) for the
fifth type (`Functions`) and [`notebooks/`](../notebooks/) for interactive
notebook-as-a-job.

The first three query the same demo data already set up in `notebooks/` —
the digital marketing customer RFM segmentation table
(`retail.customer_rfm_segments_v1`) — so the only thing that differs
between them is the job type mechanics, not the use case. `scala/` is a
different kind of demo: a long-running Structured Streaming job rather
than a one-shot query.

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

### [`scala/`](scala/README.md) — Streaming Autoloader (Raw Scala)
`parquet_autoloader_job.scala` — a `job_type: Custom Code`, `language: Raw
Scala` job that uses Yeedu's built-in `cloudFiles` source (its Auto Loader
equivalent) to incrementally ingest new parquet files, transform each
micro-batch, and write the result out. Checkpointed, runs continuously.

## Deploying

`automation/deploy_other_jobs.py` creates the JAR/Python/SQL demos as part
of `automation/provision.py`'s run — see `automation/README.md` for the
exact `job_command`/`job_class_name` values used and what's confirmed vs.
assumed (Python3/SQL `job_command` semantics aren't shown anywhere in
Yeedu's docs, only JAR is — see that README's "Known gaps"). `scala/` is
deployed as a one-off (see its own README) — its indefinitely-running
streaming semantics don't fit that script's one-shot create/start flow.
