"""Model loading and scoring logic, isolated from the Yeedu entrypoint contract."""

import logging
import pickle

import pandas as pd

from .exceptions import ModelNotLoadedError
from .schemas import FRAUD_PROBABILITY_THRESHOLD, REQUIRED_FEATURES
from .settings import MODEL_PATH

logger = logging.getLogger(__name__)


class FraudModelService:
    """Owns the lifecycle of the fraud risk model: load, score, release."""

    def __init__(self):
        self._model = None

    @property
    def is_loaded(self) -> bool:
        return self._model is not None

    def load(self, model_path=MODEL_PATH) -> None:
        logger.info("Loading fraud risk model from %s", model_path)
        with open(model_path, "rb") as f:
            self._model = pickle.load(f)
        logger.info("Fraud risk model loaded successfully.")

    def release(self) -> None:
        self._model = None
        logger.info("Fraud risk model released.")

    def score(self, features: dict) -> dict:
        """Score a single claim.

        Args:
            features: dict with keys matching REQUIRED_FEATURES.

        Returns:
            {"risk_label": "fraud"|"legitimate", "fraud_probability": float}
        """
        if not self.is_loaded:
            raise ModelNotLoadedError("Model has not been loaded. init() must run before score_claim().")

        input_df = pd.DataFrame([features], columns=REQUIRED_FEATURES)
        fraud_probability = float(self._model.predict_proba(input_df)[0][1])
        risk_label = "fraud" if fraud_probability >= FRAUD_PROBABILITY_THRESHOLD else "legitimate"

        return {
            "risk_label": risk_label,
            "fraud_probability": round(fraud_probability, 6),
        }
