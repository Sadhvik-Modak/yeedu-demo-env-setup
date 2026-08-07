# Insurance Claim Fraud/Risk Scoring — Yeedu Functions Demo

A production-shaped example of deploying a fraud-risk classifier as a low-latency inference API on **Yeedu Functions**. Mirrors the `iris/` demo's project structure — the model and use case are different, the deployment pattern is identical.

## Use Case: Real-Time Fraud Triage at Claim Intake

An insurer processes thousands of claims a week. Auto-approving every claim is fast but exposes the business to fraud losses; routing every claim to a Special Investigations Unit (SIU) for manual review is safe but slow and expensive. Most claims are legitimate — the goal is to catch the small fraction that aren't without bottlenecking the rest.

**The fix:** at claim intake, the claims system submits a small set of derived claim/policy features to a Yeedu Function. The function returns a fraud probability in milliseconds. Claims below a risk threshold auto-approve immediately; claims at or above it get routed to the SIU queue for manual review — the review team's attention goes to the claims that actually warrant it.

This is one of Yeedu's own named example use cases for Functions (`fraud_score.py` / `score_claim()`), targeting the Insurance vertical.

## Project Structure

```
fraud-risk-scoring/
├── README.md
├── function.py                # <-- Yeedu entrypoint (script_path target)
├── requirements.txt            # runtime deps installed by the Yeedu Functions job
├── requirements-dev.txt        # + pytest, for local development
├── config/
│   ├── request_schema.json     # JSON Schema Yeedu validates the payload against
│   ├── example_payload.json    # shown in the generated Swagger UI
│   └── example_response.json   # shown in the generated Swagger UI
├── models/
│   └── fraud_risk_model.pkl    # trained artifact (see scripts/train_model.py)
├── scripts/
│   └── train_model.py          # reproducible training script that produces the .pkl
├── src/
│   └── fraud_scoring/
│       ├── __init__.py
│       ├── inference.py         # model load/score, isolated from the entrypoint contract
│       ├── validation.py        # payload validation
│       ├── schemas.py           # shared feature/label definitions + risk threshold
│       ├── settings.py          # env-var-driven configuration
│       └── exceptions.py        # typed errors
└── tests/
    ├── conftest.py              # puts project root + src/ on sys.path for test collection
    ├── test_function.py
    ├── test_inference.py
    └── test_validation.py
```

`function.py` at the project root is the `--yeedu_functions_script_path` target. It imports its supporting code as `src.fraud_scoring`, which resolves at deploy time via `--yeedu_functions_project_path` (or `PYTHONPATH`) pointing at this folder.

## On the Model: Synthetic Training Data

Real claims data is proprietary and there's no public dataset for this exact scenario, so `scripts/train_model.py` generates a synthetic-but-realistic imbalanced dataset (~5% fraud rate) via `sklearn.datasets.make_classification`, then labels the generated features onto this demo's claim schema. This is fully offline and reproducible — no external downloads, no dataset licensing to worry about, and the model retrains identically every time `train_model.py` runs. A `RandomForestClassifier(class_weight="balanced")` handles the class imbalance.

## Request / Response Contract

**Request payload** (validated against `config/request_schema.json` before `score_claim()` ever runs):
```json
{
  "claim_amount": 14041.37,
  "policy_tenure_months": 52,
  "num_prior_claims": 1,
  "premium_amount": 1683.56,
  "claim_to_premium_ratio": 12.38,
  "days_to_report": 0
}
```

**Success response:**
```json
{
  "status": "success",
  "request_id": "b6e6b3d2-1a4b-4b8a-9c1e-6b2e6f8a9d10",
  "risk_label": "fraud",
  "fraud_probability": 0.995
}
```

**Error response** (e.g. missing feature):
```json
{
  "status": "error",
  "request_id": "b6e6b3d2-1a4b-4b8a-9c1e-6b2e6f8a9d10",
  "message": "Missing required feature(s): days_to_report"
}
```

## Local Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

# (Re)generate the model artifact
python scripts/train_model.py

# Run the test suite (tests/conftest.py wires up sys.path)
pytest tests/ -v
```

## Deploying on Yeedu

```bash
spark-submit yeedu_functions_job.py \
    --yeedu_functions_project_path /path/to/yeedu-functions-demo/fraud-risk-scoring \
    --yeedu_functions_script_path /path/to/yeedu-functions-demo/fraud-risk-scoring/function.py \
    --yeedu_functions_function_name score_claim \
    --yeedu_functions_requirements_path /path/to/yeedu-functions-demo/fraud-risk-scoring/requirements.txt \
    --yeedu_functions_example_payload "$(cat config/example_payload.json)" \
    --yeedu_functions_example_response "$(cat config/example_response.json)" \
    --yeedu_functions_json_schema "$(cat config/request_schema.json)" \
    --yeedu_functions_max_request_concurrency 300 \
    --yeedu_functions_idle_timeout 3600 \
    --yeedu_functions_start_port 8000 \
    --yeedu_functions_uuid_token '<uuid_token>'
```

By default the model loads from `models/fraud_risk_model.pkl` relative to the project root. To stage it elsewhere, set `FRAUD_MODEL_PATH` in the job's environment — see `src/fraud_scoring/settings.py`.

Once running:
- Swagger UI: `http://<host>:<port>/docs`
- Invoke: `POST /execute` with the payload above, header `UUID: <uuid_token>`
- Metrics: `GET /metrics`

```bash
curl -X POST http://localhost:8000/execute \
    -H "Content-Type: application/json" \
    -H "UUID: <uuid_token>" \
    -d @config/example_payload.json
```

## Adapting This to Your Own Model

1. Replace `scripts/train_model.py` with a training pipeline against your real claims data, writing the artifact to `models/`.
2. Update `REQUIRED_FEATURES` in `src/fraud_scoring/schemas.py` and the corresponding fields in `config/request_schema.json`.
3. Tune `FRAUD_PROBABILITY_THRESHOLD` in `schemas.py` to match your desired precision/recall tradeoff for SIU routing.
4. `function.py`, the `init`/`score_claim` contract, and the error-response shape stay the same regardless of what's behind them.
