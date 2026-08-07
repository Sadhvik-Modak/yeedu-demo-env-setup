# Python Job: Gold Table Summary

Demonstrates a Yeedu `job_type: Python3` job — a plain PySpark script (not
a notebook), run non-interactively via `spark-submit`.

`gold_table_summary_job.py` takes a `database.table` name as its one
command-line argument, prints the row count, and shows the top 10 rows.

## Deploy config

```
job_type:      Python3
job_command:   <workspace path to gold_table_summary_job.py>
job_arguments: nyc_taxi.gold_taxi_trip_summary_v1
```

Requires `nyc_taxi.gold_taxi_trip_summary_v1` to already exist — run
`notebooks/data-generators/bronze_ingest_nyc_taxi.ipynb` and
`notebooks/data-transformation/gold_taxi_trip_summary_v1.ipynb` first (or
point `job_arguments` at any other gold table already set up).

## Status

**Confirmed live (2026-08-07):** `job_command` = workspace path to the
`.py` entrypoint is correct for `job_type: Python` — config creation
succeeded via `yeedu job create`. Actual execution is still unverified
(needs a cluster — see `automation/README.md` "Known gaps"). Note this
differs from `job_type: Spark SQL`, which rejects `job_command` entirely —
see `../sql/README.md`.
