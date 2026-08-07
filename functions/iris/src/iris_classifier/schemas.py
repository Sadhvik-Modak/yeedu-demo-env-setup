"""Payload field definitions shared by validation, inference, and docs/config examples."""

REQUIRED_FEATURES = ("sepal_length", "sepal_width", "petal_length", "petal_width")

CLASS_NAMES = ("Iris-setosa", "Iris-versicolor", "Iris-virginica")


def extract_features(payload: dict) -> dict:
    """Pull the required feature keys out of an arbitrary request payload."""
    return {name: payload.get(name) for name in REQUIRED_FEATURES}


def missing_features(features: dict) -> list:
    """Return the subset of required feature names that are absent (None) from `features`."""
    return [name for name, value in features.items() if value is None]
