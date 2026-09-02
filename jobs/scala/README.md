# Scala Job: Streaming Autoloader

Demonstrates a Yeedu `job_type: Custom Code`, `language: Raw Scala` job —
the fourth job-type/language combination alongside the JAR, Python3, and
SQL demos in `../jar/`, `../python/`, `../sql/`. Unlike those three (each a
one-shot "query a business-metric table" job), this one is a long-running
**Structured Streaming** job: it uses Yeedu's own built-in `cloudFiles`
source (`com.yeedu.spark.streaming.CloudFilesSource`) — Yeedu's equivalent
of Databricks Auto Loader — to incrementally pick up new parquet files
landing in a source folder, apply a few business transformations per
micro-batch, and write the result out. Checkpointed for exactly-once,
incremental processing.

## What it does

- Watches `inputPath` for new (or already-present) `*.parquet` files.
- Every 5 minutes, transforms the new batch: buckets `revenue_usd` into a
  `revenue_tier`, flags `system_health_status` from `cpu_pct`, flags
  out-of-range vitals, partially masks `patient_id`, and stamps
  `processed_at`.
- Appends the transformed batch to `outputPath`.

## Deploy config

Raw Scala jobs carry their code inline (`job_rawScalaCode`), not a
workspace file path — there's no `job_command` to set, and no upload step.

```
job_type:         Custom Code
language:         Raw Scala
job_rawScalaCode: <contents of parquet_autoloader_job.scala>
packages:         org.apache.hadoop:hadoop-aws:3.2.4
```

Edit the four `val ...Path` assignments at the top of the script to point
at your own bucket before deploying — the `s3a://cdpmodakbucket/Streaming/...`
values in the file are what was used for the live verification below, not
fixed requirements.

## Known gaps / things learned running this live

- **`cloudFiles` is Yeedu's own implementation**, not Databricks' — confirms
  Yeedu ships a genuine Auto Loader equivalent, no external tooling needed.
- `cloudFiles.useNotifications` and `cloudFiles.schemaEvolutionMode` are
  **not supported** by this implementation — omit them.
- A large existing backlog is discovered **sequentially on the driver**
  (one file fully read at a time, ~2.5s/file observed) before the first
  micro-batch runs — fine for a trickle of new files, expect real delay
  before first output if seeding thousands of files into `inputPath` at once.
- Applying transformations directly on the streaming DataFrame
  (`withColumn`/`selectExpr` before `.writeStream`) hits a
  `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT` analysis error
  on this source after the first micro-batch — work around it with
  `foreachBatch`, transforming the materialized per-batch DataFrame instead
  (as this script does).
- `spark.sparkContext.setLogLevel(...)` only suppresses logging emitted
  *after* it runs — the JVM/Spark-context startup banner logs before that.
  Set `spark.log.level` via `SparkSession.builder.config(...)` instead to
  catch it earlier.

## Status

**Confirmed live (2026-09-03):** created via `POST .../spark/job`
(`job_type: Custom Code`, `language: Raw Scala`), started, and ran
continuously against a live S3 source — multiple micro-batches wrote real
transformed output to `outputPath`.

Not wired into `automation/deploy_other_jobs.py` — that script's flow
(`create` → optional one `start`) fits the other three's one-shot jobs;
this one runs indefinitely, so it's deployed and started as a one-off for
now.
