"""Training script for the Insurance claim fraud/risk model.

Real claims data is proprietary, so this generates a synthetic-but-realistic
imbalanced classification dataset (~5% fraud rate) via scikit-learn, then relabels
the generated features onto the claim schema this demo uses. Fully offline and
reproducible — no external downloads required.

Usage:
    python scripts/train_model.py
"""

import pickle
from pathlib import Path

import pandas as pd
from sklearn.datasets import make_classification
from sklearn.ensemble import RandomForestClassifier

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_PATH = PROJECT_ROOT / "models" / "fraud_risk_model.pkl"

FEATURE_COLUMNS = [
    "claim_amount",
    "policy_tenure_months",
    "num_prior_claims",
    "premium_amount",
    "claim_to_premium_ratio",
    "days_to_report",
]

RANDOM_STATE = 42

# make_classification produces near-standard-normal features. Each entry rescales
# a raw column into a realistic domain range (offset, scale, min, max, round_ndigits)
# *before* the model is fit, so the model natively operates on numbers that look like
# real claims data instead of on raw floats like 0.97.
FEATURE_RANGES = {
    "claim_amount": (8000, 6000, 200, 75000, 2),
    "policy_tenure_months": (24, 15, 1, 240, 0),
    "num_prior_claims": (1.5, 1.2, 0, 12, 0),
    "premium_amount": (900, 400, 150, 6000, 2),
    "claim_to_premium_ratio": (3, 4, 0.05, 60, 2),
    "days_to_report": (10, 8, 0, 120, 0),
}


def rescale_features(X: pd.DataFrame) -> pd.DataFrame:
    """Map each raw synthetic column onto a realistic claims-data range."""
    scaled = pd.DataFrame(index=X.index)
    for column, (offset, scale, low, high, ndigits) in FEATURE_RANGES.items():
        values = (offset + scale * X[column]).clip(lower=low, upper=high)
        scaled[column] = values.round(ndigits)
    return scaled


def main() -> None:
    X, y = make_classification(
        n_samples=2000,
        n_features=len(FEATURE_COLUMNS),
        n_informative=4,
        n_redundant=1,
        n_clusters_per_class=2,
        weights=[0.95, 0.05],
        flip_y=0.01,
        random_state=RANDOM_STATE,
    )
    X = rescale_features(pd.DataFrame(X, columns=FEATURE_COLUMNS))

    model = RandomForestClassifier(
        n_estimators=200,
        class_weight="balanced",
        random_state=RANDOM_STATE,
    )
    model.fit(X, y)

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)

    print(f"Model trained and saved to {MODEL_PATH}")


if __name__ == "__main__":
    main()
