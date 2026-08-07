# Yeedu Job-Type Demos

Worked examples of Yeedu Spark **jobs** (as distinct from Functions and
notebooks) covering the remaining `job_type` values: `Jar`, `Python3`, and
`SQL` (CLI display names: `JAR`, `Python`, `Spark SQL`). See
[`functions/`](../functions/) for the fourth type (`Functions`) and
[`notebooks/`](../notebooks/) for interactive notebook-as-a-job.

All three query the same demo data already set up in `notebooks/` — the
NYC Taxi daily trip summary gold table (`nyc_taxi.gold_taxi_trip_summary_v1`)
— so the only thing that differs between them is the job type mechanics,
not the use case.

## Demos

### [`jar/`](jar/README.md) — SparkPi (JAR)
No custom build needed: Yeedu's platform already vendors
`spark-examples_2.12-3.2.2.jar` internally (confirmed in Yeedu's own
OpenAPI docs) with `org.apache.spark.examples.SparkPi`. This demo is just
the job config pointing at it — the canonical "does JAR job type work at
all" smoke test — plus notes on how to point it at your own JAR instead.

### [`python/`](python/README.md) — Gold Table Summary (Python3)
`gold_table_summary_job.py` — a plain (non-notebook) PySpark script:
prints a gold table's row count and top rows. Takes the table name as a
command-line argument (`job_arguments`).

### [`sql/`](sql/README.md) — Gold Table Summary (SQL)
`gold_table_summary.sql` — the Spark SQL equivalent: a single `SELECT`
against the same gold table, run directly as a `job_type: SQL` job (no
Python/JVM code at all).

## Deploying

`automation/deploy_other_jobs.py` creates all three as part of
`automation/provision.py`'s run — see `automation/README.md` for the exact
`job_command`/`job_class_name` values used and what's confirmed vs.
assumed (Python3/SQL `job_command` semantics aren't shown anywhere in
Yeedu's docs, only JAR is — see that README's "Known gaps").
