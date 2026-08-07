# Yeedu Demo Environment Setup

Demo assets for Yeedu, plus a script that provisions all of it into a live
Yeedu workspace automatically. Goal: have a fully working demo environment
ready whenever needed, without hand-clicking through setup each time.

## Contents

- **[`functions/`](functions/README.md)** — 3 Yeedu Functions demos (ML
  inference as a REST endpoint): iris classification, insurance fraud
  scoring, sentiment analysis. Covers `job_type: Functions`.
- **[`jobs/`](jobs/README.md)** — the other 3 Spark job types: `jar/` (a
  thin, repo-committed jar), `python/` and `sql/` (all three query the NYC
  Taxi gold table). Covers `job_type: JAR/Python/Spark SQL`.
- **[`notebooks/`](notebooks/README.md)** — 5 datasets (NYC Taxi, Citi
  Bike, Wikipedia Clickstream, USGS Earthquakes, NOAA Weather), each with
  a bronze-ingest generator notebook and a gold-transform notebook
  (DataFrame API + `%%sql` variant), plus `visualization/` (Plotly charts,
  including a genuinely live-polling earthquake map).
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

## Adding a new demo

Each folder documents its own pattern for adding one more:
`functions/README.md`, `jobs/README.md`, `notebooks/README.md`. Then wire
it into `automation/` (`deploy_functions.py`, `deploy_other_jobs.py`, or
`create_notebooks.py` — notebooks are auto-discovered by file walk, jobs
need an entry in that module's demo list).
