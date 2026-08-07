"""Payload validation for the Iris classifier function.

This is a defense-in-depth check performed inside the function itself. Yeedu Functions
can additionally validate the raw payload against `config/request_schema.json` at the
proxy layer before the function is ever invoked (see README.md) — this module covers
the case where that schema isn't wired up, or is deliberately left permissive.
"""

from .exceptions import InvalidPayloadError
from .schemas import extract_features, missing_features


def validate_payload(payload: dict) -> dict:
    """Extract and validate the required features from a request payload.

    Args:
        payload: The raw request body handed to the function by the Yeedu runtime.

    Returns:
        A dict containing exactly the required feature keys.

    Raises:
        InvalidPayloadError: If `payload` isn't a JSON object, or is missing
            one or more required features.
    """
    if not isinstance(payload, dict):
        raise InvalidPayloadError("Payload must be a JSON object.")

    features = extract_features(payload)
    missing = missing_features(features)
    if missing:
        raise InvalidPayloadError(f"Missing required feature(s): {', '.join(missing)}")

    return features
