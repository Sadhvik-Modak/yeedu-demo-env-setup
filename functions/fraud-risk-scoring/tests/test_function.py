import function


def test_end_to_end_scoring():
    function.init()
    context = {"request_id": "test-request", "metrics": {}}
    payload = {
        "claim_amount": 18500.0,
        "policy_tenure_months": 6,
        "num_prior_claims": 3,
        "premium_amount": 850.0,
        "claim_to_premium_ratio": 21.76,
        "days_to_report": 1,
    }
    response = function.score_claim(payload, context)
    assert response["status"] == "success"
    assert response["request_id"] == "test-request"
    assert "risk_label" in response


def test_missing_feature_returns_error():
    function.init()
    context = {"request_id": "test-request-2", "metrics": {}}
    response = function.score_claim({"claim_amount": 100.0}, context)
    assert response["status"] == "error"
    assert response["request_id"] == "test-request-2"
