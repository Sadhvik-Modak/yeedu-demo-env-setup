# Iris Species Classifier — Yeedu Functions Demo

A production-shaped example of deploying a trained ML model as a low-latency inference API on **Yeedu Functions**. It uses a classic Iris flower classifier as a stand-in for any real scikit-learn model, so the project structure — not the ML — is the point.

## Use Case: Automated Plant Identification for a Nursery / Botanical Inventory System

A commercial nursery or botanical garden receives thousands of plant samples per season and needs to catalog each one by species during receiving/inventory intake. Field staff currently measure each flower by hand (sepal/petal length and width, via caliper or a mobile scanning app) and look up the species manually — slow, inconsistent between staff, and hard to audit.

**The fix:** field staff (or an automated imaging/measurement rig) submit the four measurements to a Yeedu Function over HTTP. The function returns the predicted species plus a confidence score in milliseconds, which the inventory system uses to auto-tag the sample. Low-confidence predictions get flagged for manual review instead of blocking the pipeline.

This mirrors real production patterns such as:
- **Quality control classification** — grading produce, parts, or materials from sensor measurements
- **Real-time scoring services** — fraud/risk scoring, lead scoring, recommendation ranking
- **Edge-triggered inference** — any workflow where a small, fast model needs to sit behind an API and scale on demand

## Project Structure

```
iris/
├── README.md
├── function.py                # <-- Yeedu entrypoint (script_path target)
├── requirements.txt           # runtime deps installed by the Yeedu Functions job
├── requirements-dev.txt       # + pytest, for local development
├── config/
│   ├── request_schema.json    # JSON Schema Yeedu validates the payload against
│   ├── example_payload.json   # shown in the generated Swagger UI
│   └── example_response.json  # shown in the generated Swagger UI
├── models/
│   └── iris_logistic_regression.pkl   # trained artifact (see scripts/train_model.py)
├── scripts/
│   └── train_model.py         # reproducible training script that produces the .pkl
├── src/
│   └── iris_classifier/
│       ├── __init__.py
│       ├── inference.py        # model load/predict, isolated from the entrypoint contract
│       ├── validation.py       # payload validation
│       ├── schemas.py          # shared feature/class name definitions
│       ├── settings.py         # env-var-driven configuration
│       └── exceptions.py       # typed errors
└── tests/
    ├── conftest.py             # puts project root + src/ on sys.path for test collection
    ├── test_function.py
    ├── test_inference.py
    └── test_validation.py
```

The Yeedu runtime loads exactly **one script** by path and pulls one named function out of it (see `yeedu_functions_job.py` in `Yeedu-Functions-Proxy`) — that's `function.py` at the project root. It imports its supporting code as `src.iris_classifier`, which resolves at deploy time via `--yeedu_functions_project_path` (or `PYTHONPATH`) pointing at the project root.

## The Yeedu Function Contract

This is not a convention we invented — it's what `yeedu_functions_job.py` actually calls:

```python
def init():
    """Optional. Called once at job startup — load models, open connections, etc."""

def classify_plant_sample(payload: dict, context: dict) -> dict:
    """Called once per request. Must return a JSON-serializable dict."""
```

The function name itself is arbitrary — it just has to match `--yeedu_functions_function_name`. This project uses `classify_plant_sample` instead of a generic `predict` to keep the entrypoint self-descriptive.

`context` is built by the runtime itself and always has this shape — don't invent your own keys for it:

```python
{
    "request_id": "<from the request_uuid header>",
    "metrics": {
        "pending_requests": 0,
        "total_requests": 42,
        "last_request_ts": "2026-07-16T10:00:00.000000"
    }
}
```

Note: there is **no `shutdown()` hook** in the current runtime — `yeedu_functions_job.py` never calls one on the user module. Earlier versions of this demo defined one; it was dead code against the real proxy, so it's been dropped rather than kept as a false convention.

## Request / Response Contract

**Request payload** (validated against `config/request_schema.json` before `predict()` ever runs):
```json
{
  "sepal_length": 5.1,
  "sepal_width": 3.5,
  "petal_length": 1.4,
  "petal_width": 0.2
}
```

**Success response:**
```json
{
  "status": "success",
  "request_id": "b6e6b3d2-1a4b-4b8a-9c1e-6b2e6f8a9d10",
  "prediction": "Iris-setosa",
  "probabilities": {
    "Iris-setosa": 0.972341,
    "Iris-versicolor": 0.021455,
    "Iris-virginica": 0.006204
  }
}
```

**Error response** (e.g. missing feature):
```json
{
  "status": "error",
  "request_id": "b6e6b3d2-1a4b-4b8a-9c1e-6b2e6f8a9d10",
  "message": "Missing required feature(s): petal_width"
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

Deploy `classify_plant_sample` from `function.py` at the project root using `yeedu_functions_job.py`, with `project_path` set to the project root so the `src.iris_classifier` import resolves:

```bash
spark-submit yeedu_functions_job.py \
    --yeedu_functions_project_path /path/to/yeedu-functions-demo/iris \
    --yeedu_functions_script_path /path/to/yeedu-functions-demo/iris/function.py \
    --yeedu_functions_function_name classify_plant_sample \
    --yeedu_functions_requirements_path /path/to/yeedu-functions-demo/iris/requirements.txt \
    --yeedu_functions_example_payload "$(cat config/example_payload.json)" \
    --yeedu_functions_example_response "$(cat config/example_response.json)" \
    --yeedu_functions_json_schema "$(cat config/request_schema.json)" \
    --yeedu_functions_max_request_concurrency 300 \
    --yeedu_functions_idle_timeout 3600 \
    --yeedu_functions_start_port 8000 \
    --yeedu_functions_uuid_token '<uuid_token>'
```

By default the model loads from `models/iris_logistic_regression.pkl` relative to the project root. To stage it elsewhere (e.g. a mounted workspace volume), set `IRIS_MODEL_PATH` in the job's environment — see `src/iris_classifier/settings.py`.

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

1. Replace `scripts/train_model.py` with your own training pipeline, writing the artifact to `models/`.
2. Update `REQUIRED_FEATURES` / `CLASS_NAMES` in `src/iris_classifier/schemas.py` and the corresponding fields in `config/request_schema.json`.
3. Adjust `IrisModelService.predict()` in `src/iris_classifier/inference.py` if your model needs different pre/post-processing.
4. `function.py`, the `init`/`classify_plant_sample` contract, and the error-response shape stay the same regardless of what's behind them.
