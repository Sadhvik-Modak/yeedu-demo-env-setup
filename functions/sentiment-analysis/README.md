# Patient/Customer Feedback Sentiment Analysis — Yeedu Functions Demo

A production-shaped example of deploying an NLP model as a low-latency inference API on **Yeedu Functions**. Mirrors the `iris/` and `fraud-risk-scoring/` demos' project structure — the model and use case are different, the deployment pattern is identical.

## Use Case: Patient Support Feedback Triage

A pharma company's patient support line and drug-review channels generate a steady stream of free-text feedback. Most of it is neutral or positive, but the negative comments — complaints, adverse reactions, dissatisfaction — often need a human follow-up and get lost in the volume if someone has to read every message to find them.

**The fix:** as feedback comes in, the support system submits the raw text to a Yeedu Function. The function returns a sentiment label and score in milliseconds; negative feedback gets automatically flagged into a follow-up queue instead of waiting for someone to notice it in a general inbox.

This is one of Yeedu's own named example use cases for Functions (`sentiment.py` / `analyze_sentiment()`), targeting the Life Sciences / Pharma vertical.

## Project Structure

```
sentiment-analysis/
├── README.md
├── function.py                # <-- Yeedu entrypoint (script_path target)
├── requirements.txt            # runtime deps installed by the Yeedu Functions job
├── requirements-dev.txt        # + pytest, for local development
├── config/
│   ├── request_schema.json     # JSON Schema Yeedu validates the payload against
│   ├── example_payload.json    # shown in the generated Swagger UI
│   └── example_response.json   # shown in the generated Swagger UI
├── src/
│   └── sentiment_analysis/
│       ├── __init__.py
│       ├── inference.py         # TextBlob-backed sentiment scoring, isolated from the entrypoint contract
│       ├── validation.py        # payload validation
│       ├── schemas.py           # label thresholds + bucketing logic
│       ├── settings.py          # env-var-driven threshold overrides
│       └── exceptions.py        # typed errors
└── tests/
    ├── conftest.py              # puts project root + src/ on sys.path for test collection
    ├── test_function.py
    ├── test_inference.py
    └── test_validation.py
```

`function.py` at the project root is the `--yeedu_functions_script_path` target. It imports its supporting code as `src.sentiment_analysis`, which resolves at deploy time via `--yeedu_functions_project_path` (or `PYTHONPATH`) pointing at this folder.

## On the Model: Zero Training, Zero Packaging

Unlike `iris/` and `fraud-risk-scoring/`, there's no `models/` or `scripts/` directory here — nothing to train, nothing to fetch. This demo uses [TextBlob](https://textblob.readthedocs.io/)'s default `PatternAnalyzer`, which ships its sentiment lexicon inside the pip package itself. `pip install textblob` is the entire setup; `init()` in `function.py` is a no-op kept only for contract symmetry with the other demos.

Trade-off worth knowing: TextBlob's lexicon is general-purpose and rule-based, not domain-tuned. It's confident on clearly emotional language ("terrible", "awful", "amazing") but can miss domain-specific complaint words (e.g. a clinical term like "nausea" alone won't register as negative). Good enough for a demo and for triaging obviously-negative feedback; a real deployment handling nuanced clinical language would likely swap in a domain-tuned classifier behind the same `analyze_sentiment()` contract.

## Request / Response Contract

**Request payload** (validated against `config/request_schema.json` before `analyze_sentiment()` ever runs):
```json
{
  "text": "This medication is terrible and made me feel awful all week."
}
```

**Success response:**
```json
{
  "status": "success",
  "request_id": "b6e6b3d2-1a4b-4b8a-9c1e-6b2e6f8a9d10",
  "sentiment": "negative",
  "polarity": -1.0,
  "subjectivity": 1.0
}
```
`sentiment` is bucketed from TextBlob's `polarity` (`-1.0` to `1.0`): `> 0.1` positive, `< -0.1` negative, else neutral. `subjectivity` ranges `0.0` (objective) to `1.0` (subjective).

**Error response** (e.g. missing text):
```json
{
  "status": "error",
  "request_id": "b6e6b3d2-1a4b-4b8a-9c1e-6b2e6f8a9d10",
  "message": "Missing required field: text (must be a non-empty string)."
}
```

## Local Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

# Run the test suite (tests/conftest.py wires up sys.path)
pytest tests/ -v
```

## Deploying on Yeedu

```bash
spark-submit yeedu_functions_job.py \
    --yeedu_functions_project_path /path/to/yeedu-functions-demo/sentiment-analysis \
    --yeedu_functions_script_path /path/to/yeedu-functions-demo/sentiment-analysis/function.py \
    --yeedu_functions_function_name analyze_sentiment \
    --yeedu_functions_requirements_path /path/to/yeedu-functions-demo/sentiment-analysis/requirements.txt \
    --yeedu_functions_example_payload "$(cat config/example_payload.json)" \
    --yeedu_functions_example_response "$(cat config/example_response.json)" \
    --yeedu_functions_json_schema "$(cat config/request_schema.json)" \
    --yeedu_functions_max_request_concurrency 300 \
    --yeedu_functions_idle_timeout 3600 \
    --yeedu_functions_start_port 8000 \
    --yeedu_functions_uuid_token '<uuid_token>'
```

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

1. Swap `SentimentService.analyze()` in `src/sentiment_analysis/inference.py` for a call into your own classifier — domain-tuned NLP model, hosted LLM, or a fine-tuned transformer — as long as it returns a comparable dict.
2. Tune `POSITIVE_THRESHOLD` / `NEGATIVE_THRESHOLD` in `src/sentiment_analysis/settings.py` (or their env var overrides) to match your model's score distribution.
3. `function.py`, the `init`/`analyze_sentiment` contract, and the error-response shape stay the same regardless of what's behind them.
