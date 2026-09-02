# Medallion architecture demo — banking transactions

A self-contained bronze → silver → gold demo for Yeedu 2.11.0: **12 notebooks**
and **2 pipelines** that show a realistic medallion DAG with setup, parallel
ingestion, quality gates, a quarantine branch, conformed silver builds, gold
aggregates, a for-each profiling loop, and an always-runs audit tail.

Everything here is standalone — it depends on nothing else in this repository.

> **This is a showcase, not a production job.** The notebooks synthesise their
> own data and are meant to be *looked at* in the UI. See
> [Before actually running anything](#before-actually-running-anything).

## Layout

```
pipelines/medallion/
  notebooks/        12 × .ipynb   the actual PySpark
  definitions/      2  × .json    the pipeline DAGs
  provision.py                    uploads + registers everything (idempotent)
  README.md
```

## The data model

Three Hive databases, all created by notebook `00`:

| Layer | Database | Tables |
|---|---|---|
| Bronze | `banking_bronze` | `raw_accounts`, `raw_transactions` (partitioned by `_run_date`), `dq_results`, `quarantine_records` |
| Silver | `banking_silver` | `dim_account`, `txn_clean`, `txn_rejects`, `dq_results` |
| Gold | `banking_gold` | `account_balance_daily`, `fraud_signal_summary`, `table_stats`, `pipeline_audit_log` |

Bronze is seeded with **deliberately dirty rows** — a duplicate `ACC-1003`, a
null `account_id`, a null `customer_id`, duplicate `TXN-90002`/`TXN-90005`, a
lowercase `"inr"` currency code, a zero amount, a `-750.00` amount, an orphan
`ACC-9999` reference, and a null `txn_id`. That is what gives the validation,
rejection and quarantine stages something real to catch, and it is the thing to
point at when demoing the quality gate.

## The notebooks

| # | Notebook | Writes | Cluster |
|---|---|---|---|
| 00 | `00_setup_environment` | the 3 databases + `pipeline_audit_log` | 979 small |
| 01 | `01_ingest_bronze_accounts` | `bronze.raw_accounts` | 977 medium |
| 02 | `02_ingest_bronze_transactions` | `bronze.raw_transactions` | 977 medium |
| 03 | `03_validate_bronze` | `bronze.dq_results`; emits `DQ_STATUS=PASS\|FAIL` | 979 small |
| 04 | `04_build_silver_dim_account` | `silver.dim_account` (window dedupe) | 978 aws |
| 05 | `05_build_silver_txn_clean` | `silver.txn_clean` + `txn_rejects` | 978 aws |
| 06 | `06_validate_silver` | `silver.dq_results` (strict, thresholds = 1.0) | 979 small |
| 07 | `07_gold_account_balance_daily` | `gold.account_balance_daily` | 978 aws |
| 08 | `08_gold_fraud_signal_summary` | `gold.fraud_signal_summary` | 977 medium |
| 09 | `09_publish_and_audit` | appends `gold.pipeline_audit_log` | 979 small |
| 10 | `10_quarantine_bad_records` | `bronze.quarantine_records` (failure branch) | 977 medium |
| 11 | `11_table_stats` | `gold.table_stats`, parameterised — the for-each body | 979 small |

Every notebook is `python3`, self-contained (data via `spark.createDataFrame`,
no external datasets), and sets `spark.sql.shuffle.partitions=16` rather than
the 200 default, which the on-prem boxes choke on.

### Cluster placement

Placement follows the shape of the work, which is the point worth making in the
demo — Yeedu picks the machine per task, not per pipeline:

- **979** `onprem_small` (Onprem-XS-4) — DDL, counts, ratios, metadata.
- **977** `onprem_medium` (Onprem-M-8) — ingestion feeds, exception handling.
- **978** `aws_medium` (c5ad.2xlarge) — window dedupe, the big join, the
  windowed gold aggregate.

The gold fan-out deliberately splits **across clouds**: `account_balance_daily`
runs on AWS (978) while `fraud_signal_summary` runs on-prem (977), in parallel.

Two tasks also carry `cluster_ids`, Yeedu's ordered **fallback list** tried on
OOM/resource failure — `ingest_bronze_transactions` falls back to 978, and
`build_silver_txn_clean` falls back to 977. Good thing to point at.

> Cluster **980** (`onprem_large`) is *not* attached to `demo_workspace`, so it
> cannot be referenced — the API rejects it with
> `RFA-000129: workspace id 1151 does not have access to cluster id 980`.
> Attach it to the workspace first if you want to use it.

## Pipeline 1 — `medallion_banking_pipeline`

`schedule_cron: 0 2 * * *`, `max_active_runs: 1`,
params `run_date`, `dq_threshold`, `gold_tables`. 14 tasks.

```
setup_environment (979)
        |
        +--> ingest_bronze_accounts     (977, retries 2)
        +--> ingest_bronze_transactions (977, retries 2, fallback -> 978)
                        |  fan-in, run_if: All Succeeded
                  validate_bronze (979)
                        |
                  bronze_quality_gate   [condition: dq_status EQUAL_TO PASS]
                   /                          \
          outcome=false                   outcome=true
                |                               |
   quarantine_bad_records            build_silver_dim_account (978)
        (977)                                   |
                |                    build_silver_txn_clean (978, fallback -> 977)
                |                               |
                |                       validate_silver (979)
                |                          /            \
                |     gold_account_balance_daily     gold_fraud_signal_summary
                |             (978, aws)                   (977, onprem)
                |                          \            /
                |                          settle_delay  [sleep 30s]
                |                                 |
                |                        collect_table_stats
                |                     [for_each over job.parameters.gold_tables,
                |                      concurrency 2, body = notebook 11]
                \_________________________________|
                                  |
                        publish_and_audit  [run_if: All Done] (979)
```

`build_silver_dim_account` → `build_silver_txn_clean` are **chained, not
parallel**: notebook 05 genuinely joins `banking_silver.dim_account`, so the
dependency is real. Parallel fan-out is shown at ingest and at gold instead.

What this one DAG exercises: fan-out, fan-in, a condition branch with **both**
outcomes wired, a for-each loop, a sleep, `run_if: All Done` so the audit tail
runs whether the gate passed or quarantine fired, per-task cluster placement
across three clusters and two clouds, retries with backoff, and `cluster_ids`
OOM fallback.

## Pipeline 2 — `medallion_daily_orchestrator`

`schedule_cron: 30 1 * * *`, params `run_mode`, `run_date`. 5 tasks. Shows
switch-case branching and **nested pipeline execution**:

```
choose_run_mode  [switch_case on job.parameters.run_mode: full | incremental]
        |                              |
   outcome=full                 outcome=incremental
        |                              |
  run_medallion_full          run_medallion_incremental
  [run_job_task -> pipeline 1]  [run_job_task -> pipeline 1]
                   \            /
                    cooldown  [sleep 15s, run_if: At Least One Succeeded]
                          |
                  notify_completion  [run_if: All Done] (979)
```

It must be created **after** pipeline 1, since it embeds pipeline 1's real id.

## Template syntax

Yeedu's templating is not the Airflow/Databricks form. The dialect this
instance accepts:

| Purpose | Expression |
|---|---|
| Pipeline parameter | `{{ job.parameters.run_date }}` |
| Upstream task output | `{{ tasks.validate_bronze.values.dq_status }}` |
| Current for-each item | `{{ loop_input }}` |
| Job / run metadata | `{{ job.id }}`, `{{ job.run_id }}`, `{{ job.name }}` |
| Task metadata | `{{ task.name }}`, `{{ task.run_id }}`, `{{ task.execution_count }}` |
| Workspace | `{{ workspace.id }}`, `{{ workspace.url }}` |

`{{ params.x }}`, `{{ tasks.x.output.y }}` and `{{ item }}` are all **rejected**
by the server with `invalid template syntax`.

### for-each inputs must be a JSON *string*

`for_each_task.inputs` looks like a template, but the DAG generator dereferences
it at generation time and emits `_base_params.get("gold_tables", "[]")` straight
into `resolve_foreach_input()`, which begins with `expr.strip()`. So if the
pipeline param is a real JSON **array**, the DAG fails to import with

```
AttributeError: 'list' object has no attribute 'strip'
```

The param must therefore be a **JSON-encoded string**:

```json
"gold_tables": "[\"banking_gold.account_balance_daily\", \"banking_gold.fraud_signal_summary\"]"
```

`resolve_foreach_input` then takes its "Case 1 (static list)" path and
`json.loads` it back into a list. `inputs` also accepts
`{{ tasks.<key>.values.<k> }}`, which becomes a runtime `XComArg` instead.

### `run_job_task` rejects `retries`

`Task 'run_medallion_full': retries is not allowed for task type run_job_task`.
Nested pipeline invocations carry no retry settings.

## Provisioning

```bash
export YEEDU_TOKEN='<jwt>'
python3 provision.py --dry-run          # print payloads, touch nothing
python3 provision.py                    # upload + register + create pipelines
```

Stdlib only, no dependencies. Flags: `--api-url`, `--token` (or `$YEEDU_TOKEN`),
`--tenant-id`, `--workspace-id`, `--insecure`, `--dry-run`, `--verbose`.
Defaults target `dev-onprem-009.yeedu.io:8080`, tenant
`c6b79638-ae11-428f-9a1f-246dfd3c6d01`, workspace `1151` (`demo_workspace`).

It is idempotent — it matches by name, skips notebooks that already exist and
PUTs pipelines that do. **It never triggers a run.**

Current live ids in `demo_workspace` (1151): notebooks `1015787`–`1015799`,
`medallion_banking_pipeline` = **212**, `medallion_daily_orchestrator` = **213**.

Order of operations: upload the 12 `.ipynb` files to `/medallion/` → register
each as a notebook → build a name→`notebook_id` map → resolve the
`"__notebook__"` / `"__pipeline__"` placeholders in the JSON definitions into
real `notebook_task.job_id` / `run_job_task.pipeline_id` values → POST both
pipelines. Keeping placeholders in the JSON is what lets the definitions stay
readable and re-provisionable into any workspace.

### API notes worth knowing

- The base path **`/api/v1` is mandatory**. Omitting it returns a misleading
  `You don't have the sufficient permission to perform GET on /workspaces`.
- Headers: `Authorization: Bearer <jwt>` and `TENANT-ID: <tenant>`. The dev cert
  is self-signed, hence `--insecure`.
- File upload is
  `POST /workspace/files?workspace_id=&path=/medallion/<file>.ipynb&overwrite=true`
  with an `x-file-size` header and an octet-stream body. Do **not** pass
  `target_dir` (rejected as invalid) and do **not** pass `is_dir`. The parent
  folder is created automatically.
- One file may back only one notebook — re-pointing a second notebook at the
  same path returns `RFA-000236 ... already associated to a notebook`.
- Notebook create returns `notebook_id` as a **string**; the pipeline schema
  requires an **integer**. `provision.py` coerces it.
- Pipeline create returns the new id as `id` (a string), not `pipeline_id`.
  `GET .../pipeline/{id}` returns `{"pipeline": {...}, "tasks": [...]}` — the
  tasks are a **sibling** of `pipeline`, not nested inside it.
- **Creating a pipeline is asynchronous.** The row appears in
  `GET .../pipelines` immediately, but Yeedu writes an Airflow DAG file that the
  dag-processor picks up on its next scan — normally **~10 s**. Until then
  `GET`, `PUT` and `DELETE` on that pipeline all return 404
  `Failed to fetch Airflow DAG tasks` / `Failed to trigger DAG reparse`. A 404
  that never clears means the generated DAG does not import; the parse error is
  the real diagnostic:

  ```bash
  ssh dev-onprem-009
  docker exec docker_yeedu-airflow-dag-processor_1 \
    tail -5 /opt/airflow/logs/dag_processor/<date>/dags-folder/tenant/<tenant>/workspace/1151/pipeline/<id>.py.log
  # the generated DAG itself:
  docker exec docker_yeedu-airflow-dag-processor_1 \
    cat /opt/airflow/dags/tenant/<tenant>/workspace/1151/pipeline/<id>.py
  ```

  A pipeline stuck in that state needs `DELETE` **twice**: the first call
  removes the DAG file and still 404s, the second clears the database row.

## Before actually running anything

Two things would need attention first:

1. **Yeedu executes markdown cells as Python** on `notebook start` (the UI
   renders them correctly, but the runner does not skip them). Every notebook
   here opens with a markdown title cell, so those cells must be stripped
   before a real execution.
2. The synthesised data is tiny and the dirty rows are intentional —
   `03_validate_bronze` is *designed* to be able to emit `FAIL` and send the DAG
   down the quarantine branch. That is a feature of the demo, not a bug.

## Demo script

1. Open **Notebooks** in `demo_workspace` — 12 notebooks, `00`…`11`, named in
   pipeline order. Open `05_build_silver_txn_clean` to show real PySpark:
   dedupe by window, currency normalisation, the reject split, the dimension
   join.
2. Open **Pipelines** → `medallion_banking_pipeline`. Let the DAG render. Walk
   bronze → silver → gold left to right.
3. Point at `bronze_quality_gate` — both branches are wired, so a bad load
   diverts to quarantine instead of poisoning silver.
4. Point at `collect_table_stats` — one task definition, fanned out over the
   `gold_tables` parameter at concurrency 2.
5. Point at `publish_and_audit` — `run_if: All Done`, so the audit log is
   written on the failure path too.
6. Open a couple of tasks to show `task_cluster_id` differing per task, and the
   `cluster_ids` fallback list on `build_silver_txn_clean`.
7. Open `medallion_daily_orchestrator` to show switch-case routing and one
   pipeline calling another.
