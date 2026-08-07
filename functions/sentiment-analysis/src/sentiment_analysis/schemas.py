"""Sentiment label thresholds shared by validation, inference, and docs/config examples."""

from .settings import NEGATIVE_THRESHOLD, POSITIVE_THRESHOLD

SENTIMENT_LABELS = ("negative", "neutral", "positive")


def label_for_polarity(polarity: float) -> str:
    """Bucket a raw TextBlob polarity score (-1.0 to 1.0) into a sentiment label."""
    if polarity > POSITIVE_THRESHOLD:
        return "positive"
    if polarity < NEGATIVE_THRESHOLD:
        return "negative"
    return "neutral"
