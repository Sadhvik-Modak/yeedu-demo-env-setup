# Yeedu Functions Demos

Worked examples of deploying ML models as low-latency inference APIs on **Yeedu Functions** — a Python function loaded once (`init()`), then invoked per request (`<name>(payload, context) -> dict`) behind a REST endpoint (`POST /execute`). See any demo's own README for the full deploy command against `yeedu_functions_job.py`.

Each demo is self-contained: its own `function.py` entrypoint, `src/<package>/` supporting code, `config/` (JSON Schema + Swagger examples), `tests/`, and README. The pattern is identical across all of them — only the model and use case differ.

## Demos

### [`iris/`](iris/README.md) — Iris Species Classifier
Nursery/botanical inventory use case: classify a plant sample from sepal/petal measurements. Tabular multi-class classification (scikit-learn `LogisticRegression`), trained locally and reproducibly via `scripts/train_model.py`. Function: `classify_plant_sample`.

### [`fraud-risk-scoring/`](fraud-risk-scoring/README.md) — Insurance Claim Fraud/Risk Scoring
Score insurance claims for fraud risk at intake so high-risk claims route to manual review instead of auto-approving. Tabular binary classification on an imbalanced dataset (scikit-learn `RandomForestClassifier`, trained on synthetic claims data since real claims data is proprietary). Function: `score_claim`.

### [`sentiment-analysis/`](sentiment-analysis/README.md) — Patient/Customer Feedback Sentiment Analysis
Triage patient support feedback by sentiment so negative feedback gets flagged for follow-up. Text/NLP, using TextBlob's built-in pattern-based analyzer — no model training or packaging required. Function: `analyze_sentiment`.

## Why These Three

All three mirror Yeedu's own named example use cases for Functions (from `Yeedu-Functions-Proxy` sample scripts and Yeedu's product marketing), and together cover the main shapes an ML inferencing demo needs to show: tabular multi-class classification, imbalanced binary classification, and text/NLP — with model sourcing ranging from "train from scratch" to "zero packaging, pretrained out of the box."
