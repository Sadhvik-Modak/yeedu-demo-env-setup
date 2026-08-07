"""Exception types for the fraud risk scoring function."""


class FraudScoringError(Exception):
    """Base class for errors raised by this function that should be reported to the caller."""


class ModelNotLoadedError(FraudScoringError):
    """Raised when score_claim() is invoked before init() has loaded the model."""


class InvalidPayloadError(FraudScoringError):
    """Raised when the request payload is missing one or more required claim features."""
