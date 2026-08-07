import pytest

from fraud_scoring.exceptions import ModelNotLoadedError
from fraud_scoring.inference import FraudModelService

SAMPLE_FEATURES = {
    "claim_amount": 18500.0,
    "policy_tenure_months": 6,
    "num_prior_claims": 3,
    "premium_amount": 850.0,
    "claim_to_premium_ratio": 21.76,
    "days_to_report": 1,
}


def test_score_raises_before_load():
    service = FraudModelService()
    with pytest.raises(ModelNotLoadedError):
        service.score(SAMPLE_FEATURES)


def test_score_returns_risk_label_after_load():
    service = FraudModelService()
    service.load()
    result = service.score(SAMPLE_FEATURES)
    assert result["risk_label"] in {"fraud", "legitimate"}
    assert 0.0 <= result["fraud_probability"] <= 1.0
