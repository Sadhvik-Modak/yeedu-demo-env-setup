# Yeedu Demo Notebooks

Notebooks that stock a Yeedu workspace with real Spark tables so demos
(dashboards, jobs, Functions) always have data to point at, instead of
needing ad-hoc setup each time.

Six industry verticals — the ones Yeedu's actual customers are in — each
with a real public dataset and a storyline with genuine business value,
not a generic public dataset picked for convenience:

| Industry | Dataset | Storyline |
|---|---|---|
| Life Sciences | ClinicalTrials.gov registry | Trial pipeline health by phase |
| Healthcare | CMS synthetic Medicare claims | 30-day readmission risk by diagnosis |
| Pharma | openFDA adverse event reports | Drug safety signal monitoring |
| Agriculture | USDA NASS crop census | Crop yield trend, 2012 → 2017 |
| Financial Services | Card transaction fraud dataset | Fraud risk by amount/hour |
| Digital Marketing | Online retail transactions | Customer RFM segmentation |

## Pattern

Two-stage, per industry:

- **[`data-generators/`](data-generators/)** — one notebook per source
  dataset. Downloads or pulls (via API) raw data, creates the Spark
  database if needed, and persists it as a managed table, as close to
  source shape as reasonable. Naming: `<source>_ingest.ipynb` — e.g.
  `fda_adverse_event_ingest.ipynb`.
- **[`data-transformation/`](data-transformation/)** — one notebook per
  derived table. Reads a generator's table, cleans/aggregates it into the
  actual business-metric table for the demo, and persists it. Naming:
  `<business_metric_name>.ipynb` — e.g. `drug_safety_signal_summary.ipynb`,
  `readmission_risk_summary.ipynb`, `customer_rfm_segmentation.ipynb`.
  Persisted tables are versioned `<name>_v1`, `_v2`, ... like a real
  production pipeline would, not tagged `gold_`/`silver_`/`bronze_` —
  those are internal data-engineering layering terms, not names a
  workload would actually ship under. (The `data-generators/` /
  `data-transformation/` *folder* split already communicates the layering
  — the files inside don't need to repeat it in their names.)
  Each transformation may also have a `_sql` companion
  (`<business_metric_name>_sql.ipynb`) — same result, written with
  `%%sql` cells instead of the DataFrame API, for demos that want to show
  Yeedu's SQL notebook experience.
- **[`visualization/`](visualization/)** — chart notebooks, using
  [Plotly](https://plotly.com/python/) (interactive, polished defaults,
  built-in maps, no extra Jupyter widget extensions needed). See its own
  section below.

Each notebook is self-contained and re-runnable (`CREATE DATABASE IF NOT
EXISTS` + `mode("overwrite")`), with a config cell at the top so the
database/table names and source can be changed without touching the logic
below.

## Demos

### Life Sciences — Clinical Trial Pipeline
- Generator: [`data-generators/clinical_trials_registry_ingest.ipynb`](data-generators/clinical_trials_registry_ingest.ipynb)
  — pulls trial records from the ClinicalTrials.gov API v2 (no auth) and
  persists them as `clinical_trials.trial_registry`.
- Transformation: [`data-transformation/trial_pipeline_health_summary.ipynb`](data-transformation/trial_pipeline_health_summary.ipynb)
  (DataFrame API) and its SQL companion
  [`data-transformation/trial_pipeline_health_summary_sql.ipynb`](data-transformation/trial_pipeline_health_summary_sql.ipynb)
  — aggregate by trial phase (trial count, avg enrollment, termination
  rate) into `clinical_trials.trial_pipeline_health_summary_v1`.

### Healthcare — Medicare Readmission Risk
- Generators: [`data-generators/medicare_beneficiary_ingest.ipynb`](data-generators/medicare_beneficiary_ingest.ipynb)
  and [`data-generators/medicare_inpatient_claims_ingest.ipynb`](data-generators/medicare_inpatient_claims_ingest.ipynb)
  — download CMS's synthetic DE-SynPUF sample (fully synthetic, safe for
  public demos) and persist `medicare_claims.beneficiary_summary` /
  `medicare_claims.inpatient_claims`.
- Transformation: [`data-transformation/readmission_risk_summary.ipynb`](data-transformation/readmission_risk_summary.ipynb)
  (DataFrame API) and its SQL companion
  [`data-transformation/readmission_risk_summary_sql.ipynb`](data-transformation/readmission_risk_summary_sql.ipynb)
  — flag 30-day readmissions via a window function, aggregate by primary
  diagnosis into `medicare_claims.readmission_risk_summary_v1`.

### Pharma — Drug Safety Signal Monitoring
- Generator: [`data-generators/fda_adverse_event_ingest.ipynb`](data-generators/fda_adverse_event_ingest.ipynb)
  — pulls adverse event reports from the openFDA API (no auth) and
  persists them as `pharmacovigilance.adverse_events`.
- Transformation: [`data-transformation/drug_safety_signal_summary.ipynb`](data-transformation/drug_safety_signal_summary.ipynb)
  (DataFrame API) and its SQL companion
  [`data-transformation/drug_safety_signal_summary_sql.ipynb`](data-transformation/drug_safety_signal_summary_sql.ipynb)
  — aggregate by drug/reaction pair (report count, serious rate) into
  `pharmacovigilance.drug_safety_signal_summary_v1`.

### Agriculture — Crop Yield Trends
- Generator: [`data-generators/usda_crop_statistics_ingest.ipynb`](data-generators/usda_crop_statistics_ingest.ipynb)
  — downloads USDA NASS Census of Agriculture bulk files (2012 + 2017, no
  auth) and persists a state/crop-level subset as
  `agriculture.crop_statistics`.
- Transformation: [`data-transformation/crop_yield_trend_summary.ipynb`](data-transformation/crop_yield_trend_summary.ipynb)
  (DataFrame API) and its SQL companion
  [`data-transformation/crop_yield_trend_summary_sql.ipynb`](data-transformation/crop_yield_trend_summary_sql.ipynb)
  — clean NASS's raw value formatting, pivot into area/production/yield
  columns, compute the change between census years into
  `agriculture.crop_yield_trend_summary_v1`.

### Financial Services — Card Fraud Risk
- Generator: [`data-generators/card_transactions_ingest.ipynb`](data-generators/card_transactions_ingest.ipynb)
  — downloads the public Worldline/ULB card-transaction fraud dataset (no
  auth) and persists it as `payments.card_transactions`.
- Transformation: [`data-transformation/fraud_risk_summary.ipynb`](data-transformation/fraud_risk_summary.ipynb)
  (DataFrame API) and its SQL companion
  [`data-transformation/fraud_risk_summary_sql.ipynb`](data-transformation/fraud_risk_summary_sql.ipynb)
  — bucket by amount range and hour of day (fraud rate, flagged amount)
  into `payments.fraud_risk_summary_v1`.

### Digital Marketing — Customer RFM Segmentation
- Generator: [`data-generators/retail_order_ingest.ipynb`](data-generators/retail_order_ingest.ipynb)
  — downloads the UCI Online Retail II dataset (no auth) and persists it
  as `retail.orders`.
- Transformation: [`data-transformation/customer_rfm_segmentation.ipynb`](data-transformation/customer_rfm_segmentation.ipynb)
  (DataFrame API) and its SQL companion
  [`data-transformation/customer_rfm_segmentation_sql.ipynb`](data-transformation/customer_rfm_segmentation_sql.ipynb)
  — score each customer on Recency/Frequency/Monetary quintiles, label a
  named segment, into `retail.customer_rfm_segments_v1`.

## Visualization

[`visualization/`](visualization/):

- [`pharmacovigilance_live_monitor.ipynb`](visualization/pharmacovigilance_live_monitor.ipynb)
  — polls the openFDA adverse-event feed directly (no Spark table needed)
  on a fixed interval and re-renders a live chart of top reaction signals
  each time. A genuine live-refresh demo, not a canned animation.
- [`portfolio_summary_dashboard.ipynb`](visualization/portfolio_summary_dashboard.ipynb)
  — one chart per industry's business-metric table, styled consistently
  for a quick "what's in this demo environment" walkthrough across all
  six verticals.

Dependencies: `visualization/requirements.txt` (`plotly`, `pandas`).

## Adding a new dataset

1. Add a `<source>_ingest.ipynb` under `data-generators/` following the
   existing notebooks' shape: config cell → download/pull → load →
   `CREATE DATABASE IF NOT EXISTS` → `saveAsTable` (overwrite) → verify
   (count + describe).
2. Add a `<business_metric_name>.ipynb` (and, if wanted, its `_sql`
   companion) under `data-transformation/` that reads the source table,
   transforms it into something with a real storyline, and persists the
   result the same way, versioned `_v1`.
3. Add an entry under **Demos** above — dataset, storyline, one line each
   for generator and transformation.

No `bronze_`/`silver_`/`gold_` prefixes on new files, per the pattern
above.
