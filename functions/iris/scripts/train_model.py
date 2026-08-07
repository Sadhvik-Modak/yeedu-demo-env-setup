"""Reproducible training script for the Iris species classifier.

Regenerates models/iris_logistic_regression.pkl from scikit-learn's bundled
Iris dataset, so the model artifact isn't an opaque binary with no known origin.

Usage:
    python scripts/train_model.py
"""

import pickle
from pathlib import Path

import pandas as pd
from sklearn.datasets import load_iris
from sklearn.linear_model import LogisticRegression

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MODEL_PATH = PROJECT_ROOT / "models" / "iris_logistic_regression.pkl"

FEATURE_COLUMNS = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
CLASS_NAMES = ["Iris-setosa", "Iris-versicolor", "Iris-virginica"]


def main() -> None:
    iris = load_iris()
    X = pd.DataFrame(iris.data, columns=FEATURE_COLUMNS)
    y = pd.Series(iris.target).map(dict(enumerate(CLASS_NAMES)))

    model = LogisticRegression(max_iter=200)
    model.fit(X, y)

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(model, f)

    print(f"Model trained and saved to {MODEL_PATH}")


if __name__ == "__main__":
    main()
