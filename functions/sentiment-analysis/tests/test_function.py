import function


def test_end_to_end_negative_sentiment():
    function.init()
    context = {"request_id": "test-request", "metrics": {}}
    payload = {"text": "This medication is terrible and made me feel awful all week."}
    response = function.analyze_sentiment(payload, context)
    assert response["status"] == "success"
    assert response["request_id"] == "test-request"
    assert response["sentiment"] == "negative"


def test_missing_text_returns_error():
    function.init()
    context = {"request_id": "test-request-2", "metrics": {}}
    response = function.analyze_sentiment({}, context)
    assert response["status"] == "error"
    assert response["request_id"] == "test-request-2"
