# SQL Job: Gold Table Summary

Demonstrates a Yeedu `job_type: SQL` job (CLI display name: `Spark SQL`) —
a `.sql` script run directly, no Python/JVM code at all.

`gold_table_summary.sql` selects the 10 most recent rows from the NYC Taxi
gold summary table.

## Deploy config

**Confirmed live (2026-08-07):** `Spark SQL` jobs do NOT use `job_command`
like JAR/Python do — the API rejects it outright. The query goes in
`job_rawScalaCode` (confusingly named for a SQL job), and the `yeedu` CLI's
`--job-raw-scala-code` flag takes a **local filesystem path** — the CLI
reads that file itself and uploads its content; it's not a workspace path
and not the raw text inline.

```
job_type:         Spark SQL
job_rawScalaCode: <contents of gold_table_summary.sql, via a local file path>
```

Requires `nyc_taxi.gold_taxi_trip_summary_v1` to already exist — run
`notebooks/data-generators/bronze_ingest_nyc_taxi.ipynb` and
`notebooks/data-transformation/gold_taxi_trip_summary_v1.ipynb` first.

Config creation is confirmed (verified via `yeedu job get` — the stored
`job_rawScalaCode` matches this file byte for byte). Actual execution is
still unverified (needs a cluster — see `automation/README.md` "Known
gaps").
