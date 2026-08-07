import pytest

from iris_classifier.exceptions import ModelNotLoadedError
from iris_classifier.inference import IrisModelService

SAMPLE_FEATURES = {
    "sepal_length": 5.1,
    "sepal_width": 3.5,
    "petal_length": 1.4,
    "petal_width": 0.2,
}


def test_predict_raises_before_load():
    service = IrisModelService()
    with pytest.raises(ModelNotLoadedError):
        service.predict(SAMPLE_FEATURES)


def test_predict_returns_species_after_load():
    service = IrisModelService()
    service.load()
    result = service.predict(SAMPLE_FEATURES)
    assert result["prediction"] in {"Iris-setosa", "Iris-versicolor", "Iris-virginica"}
    assert set(result["probabilities"]) == {"Iris-setosa", "Iris-versicolor", "Iris-virginica"}
