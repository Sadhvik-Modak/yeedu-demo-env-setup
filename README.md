# Yeedu Demo Environment Setup

Demo assets for Yeedu, plus a script that provisions all of it into a live
Yeedu workspace automatically. Goal: have a fully working demo environment
ready whenever needed, without hand-clicking through setup each time.

## Contents

- **[`functions/`](functions/README.md)** — 3 Yeedu Functions demos (ML
  inference as a REST endpoint): iris classification, insurance fraud
  scoring, sentiment analysis. Covers `job_type: Functions`.
- **[`jobs/`](jobs/README.md)** — the other 4 Spark job types: `jar/` (a
  thin, repo-committed jar), `python/` and `sql/` (all three query the
  digital marketing customer RFM table), and `scala/` (a Raw Scala
  streaming Autoloader demo). Covers `job_type: JAR/Python/Spark SQL/Custom Code`.
- **[`notebooks/`](notebooks/README.md)** — 6 industry verticals (life
  sciences, healthcare, pharma, agriculture, financial services, digital
  marketing), each with a real public dataset, a genuine business
  storyline, an ingest notebook, and a business-metric transform notebook
  (DataFrame API + `%%sql` variant), plus `visualization/` (Plotly charts,
  including a genuinely live-polling pharmacovigilance signal monitor).
- **[`automation/`](automation/README.md)** — `provision.py`: clones this
  repo into a Yeedu workspace and registers everything above as real Yeedu
  resources (jobs + notebooks), idempotently, driven by the `yeedu` CLI.

## Quickest path to a working demo

```bash
pip install -r automation/requirements.txt
python3 automation/provision.py \
  --api-url https://<host>:8080 \
  --username <user> --password <pass> \
  --tenant-id <tenant_id> \
  --insecure   # only for a known dev/QA host with a self-signed cert
```

Omit `--workspace-id` and a new workspace is created for you. See
`automation/README.md` for the full flag reference, what's confirmed vs.
still unverified against a live instance, and the "Confirmed live" section
recording real bugs found and fixed while running this against
`dev-onprem-008` (2.10.1) — worth reading before trusting any of it blind.

## Running the job-type demos

`automation/deploy_other_jobs.py` (part of `provision.py`'s run) creates
the 3 `jobs/` demos via `yeedu job create`, using the exact
`job_class_name`/`job_arguments` values documented in each subfolder's
README. Pass `--start` (with `--cluster-id`) to also trigger one run
right after creation; to trigger a run later, use the same mechanism
directly:

```bash
yeedu job start --job_id <job_id> --workspace_id <workspace_id>
```

Each job needs `retail.customer_rfm_segments_v1` to already exist —
run `notebooks/data-generators/retail_order_ingest.ipynb` and
`notebooks/data-transformation/customer_rfm_segmentation.ipynb` first.

**Confirmed live** (checked `/yeedu/reactors/logs/` on a running
cluster): the SQL job (`table_summary.sql`) ran successfully. The JAR
and Python jobs failed on real, easy-to-hit config mistakes — check
these before re-running either:
- **JAR job**: fails with `Error: Failed to load class TableSummaryJob.`
  if `job_class_name` is the bare class name instead of the
  fully-qualified `io.yeedu.demo.TableSummaryJob` (see
  `jobs/jar/README.md`).
- **Python job**: fails with the script's own
  `Usage: table_summary_job.py <database.table>` message and exits
  (`sys.exit(1)`) if `job_arguments` is empty — it must be set to a
  real `database.table`, e.g. `retail.customer_rfm_segments_v1` (see
  `jobs/python/README.md`).

## Adding a new demo

Each folder documents its own pattern for adding one more:
`functions/README.md`, `jobs/README.md`, `notebooks/README.md`. Then wire
it into `automation/` (`deploy_functions.py`, `deploy_other_jobs.py`, or
`create_notebooks.py` — notebooks are auto-discovered by file walk, jobs
need an entry in that module's demo list).
