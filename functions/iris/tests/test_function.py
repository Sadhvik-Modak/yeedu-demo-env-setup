import function


def test_end_to_end_prediction():
    function.init()
    context = {"request_id": "test-request", "metrics": {}}
    payload = {
        "sepal_length": 5.1,
        "sepal_width": 3.5,
        "petal_length": 1.4,
        "petal_width": 0.2,
    }
    response = function.classify_plant_sample(payload, context)
    assert response["status"] == "success"
    assert response["request_id"] == "test-request"
    assert "prediction" in response


def test_missing_feature_returns_error():
    function.init()
    context = {"request_id": "test-request-2", "metrics": {}}
    response = function.classify_plant_sample({"sepal_length": 5.1}, context)
    assert response["status"] == "error"
    assert response["request_id"] == "test-request-2"
