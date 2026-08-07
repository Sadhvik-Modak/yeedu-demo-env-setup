"""Payload validation for the sentiment analysis function.

This is a defense-in-depth check performed inside the function itself. Yeedu Functions
can additionally validate the raw payload against `config/request_schema.json` at the
proxy layer before the function is ever invoked (see README.md) — this module covers
the case where that schema isn't wired up, or is deliberately left permissive.
"""

from .exceptions import InvalidPayloadError


def validate_payload(payload: dict) -> str:
    """Extract and validate the `text` field from a request payload.

    Args:
        payload: The raw request body handed to the function by the Yeedu runtime.

    Returns:
        The non-empty text to analyze.

    Raises:
        InvalidPayloadError: If `payload` isn't a JSON object, or `text` is
            missing, not a string, or blank.
    """
    if not isinstance(payload, dict):
        raise InvalidPayloadError("Payload must be a JSON object.")

    text = payload.get("text")
    if not isinstance(text, str) or not text.strip():
        raise InvalidPayloadError("Missing required field: text (must be a non-empty string).")

    return text
