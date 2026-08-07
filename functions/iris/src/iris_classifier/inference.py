"""Model loading and prediction logic, isolated from the Yeedu entrypoint contract."""

import logging
import pickle

import pandas as pd

from .exceptions import ModelNotLoadedError
from .settings import MODEL_PATH

logger = logging.getLogger(__name__)


class IrisModelService:
    """Owns the lifecycle of the pretrained Iris classifier: load, predict, release."""

    def __init__(self):
        self._model = None

    @property
    def is_loaded(self) -> bool:
        return self._model is not None

    def load(self, model_path=MODEL_PATH) -> None:
        logger.info("Loading Iris model from %s", model_path)
        with open(model_path, "rb") as f:
            self._model = pickle.load(f)
        logger.info("Iris model loaded successfully.")

    def release(self) -> None:
        self._model = None
        logger.info("Iris model released.")

    def predict(self, features: dict) -> dict:
        """Run inference for a single sample.

        Args:
            features: dict with keys sepal_length, sepal_width, petal_length, petal_width.

        Returns:
            {"prediction": str, "probabilities": dict[str, float]}
        """
        if not self.is_loaded:
            raise ModelNotLoadedError("Model has not been loaded. init() must run before predict().")

        input_df = pd.DataFrame([features])
        prediction = self._model.predict(input_df)[0]

        probabilities = {}
        if hasattr(self._model, "predict_proba"):
            proba = self._model.predict_proba(input_df)[0]
            probabilities = {
                label: round(float(score), 6)
                for label, score in zip(self._model.classes_, proba)
            }

        return {
            "prediction": str(prediction),
            "probabilities": probabilities,
        }
