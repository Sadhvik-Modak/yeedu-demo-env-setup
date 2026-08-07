import pytest

from fraud_scoring.exceptions import InvalidPayloadError
from fraud_scoring.validation import validate_payload

VALID_PAYLOAD = {
    "claim_amount": 18500.0,
    "policy_tenure_months": 6,
    "num_prior_claims": 3,
    "premium_amount": 850.0,
    "claim_to_premium_ratio": 21.76,
    "days_to_report": 1,
}


def test_validate_payload_accepts_complete_payload():
    assert validate_payload(VALID_PAYLOAD) == VALID_PAYLOAD


def test_validate_payload_rejects_missing_feature():
    payload = dict(VALID_PAYLOAD)
    del payload["days_to_report"]
    with pytest.raises(InvalidPayloadError):
        validate_payload(payload)


def test_validate_payload_rejects_non_dict():
    with pytest.raises(InvalidPayloadError):
        validate_payload("not-a-dict")
