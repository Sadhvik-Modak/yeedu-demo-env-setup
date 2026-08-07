"""Sentiment scoring logic, isolated from the Yeedu entrypoint contract.

Uses TextBlob's default pattern-based sentiment analyzer, which ships its lexicon
inside the pip package itself — no model file to load, no training pipeline, no
separate corpus download for `.sentiment` calls.
"""

from textblob import TextBlob

from .schemas import label_for_polarity


class SentimentService:
    """Wraps TextBlob's sentiment analyzer behind a small, testable interface."""

    def analyze(self, text: str) -> dict:
        """Score a single piece of feedback text.

        Args:
            text: non-empty text to analyze.

        Returns:
            {"sentiment": "negative"|"neutral"|"positive",
             "polarity": float, "subjectivity": float}
        """
        blob = TextBlob(text)
        polarity = round(blob.sentiment.polarity, 6)
        subjectivity = round(blob.sentiment.subjectivity, 6)

        return {
            "sentiment": label_for_polarity(polarity),
            "polarity": polarity,
            "subjectivity": subjectivity,
        }
