import pytest

from sentiment_analysis.exceptions import InvalidPayloadError
from sentiment_analysis.validation import validate_payload


def test_validate_payload_accepts_text():
    assert validate_payload({"text": "great product"}) == "great product"


def test_validate_payload_rejects_missing_text():
    with pytest.raises(InvalidPayloadError):
        validate_payload({})


def test_validate_payload_rejects_blank_text():
    with pytest.raises(InvalidPayloadError):
        validate_payload({"text": "   "})


def test_validate_payload_rejects_non_dict():
    with pytest.raises(InvalidPayloadError):
        validate_payload("not-a-dict")
