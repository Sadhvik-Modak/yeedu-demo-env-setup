"""Exception types for the Iris classifier function."""


class IrisClassifierError(Exception):
    """Base class for errors raised by this function that should be reported to the caller."""


class ModelNotLoadedError(IrisClassifierError):
    """Raised when predict() is invoked before init() has loaded the model."""


class InvalidPayloadError(IrisClassifierError):
    """Raised when the request payload is missing one or more required features."""
