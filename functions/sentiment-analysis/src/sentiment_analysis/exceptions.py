"""Exception types for the sentiment analysis function."""


class SentimentAnalysisError(Exception):
    """Base class for errors raised by this function that should be reported to the caller."""


class InvalidPayloadError(SentimentAnalysisError):
    """Raised when the request payload is missing `text` or `text` is empty/blank."""
