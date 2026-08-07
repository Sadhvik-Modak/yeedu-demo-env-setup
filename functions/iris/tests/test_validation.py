import pytest

from iris_classifier.exceptions import InvalidPayloadError
from iris_classifier.validation import validate_payload


def test_validate_payload_accepts_complete_payload():
    payload = {
        "sepal_length": 5.1,
        "sepal_width": 3.5,
        "petal_length": 1.4,
        "petal_width": 0.2,
    }
    assert validate_payload(payload) == payload


def test_validate_payload_rejects_missing_feature():
    payload = {"sepal_length": 5.1, "sepal_width": 3.5, "petal_length": 1.4}
    with pytest.raises(InvalidPayloadError):
        validate_payload(payload)


def test_validate_payload_rejects_non_dict():
    with pytest.raises(InvalidPayloadError):
        validate_payload("not-a-dict")
