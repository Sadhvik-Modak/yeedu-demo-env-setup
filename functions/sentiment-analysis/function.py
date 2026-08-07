"""Yeedu Functions entrypoint for patient/customer feedback sentiment analysis.

This file is the `--yeedu_functions_script_path` target — Yeedu loads it directly
by path and calls the named function. It lives at the project root because that's
what the Yeedu runtime expects to point at; the supporting package
(`src/sentiment_analysis/`) is imported as `src.sentiment_analysis`, made resolvable
via `--yeedu_functions_project_path` (or PYTHONPATH) pointing at the project root.

Deploy with `--yeedu_functions_function_name analyze_sentiment`. See README.md for the
full spark-submit / python command.

Contract (enforced by yeedu_functions_job.py, not by this module):
    init()                              -> called once at job startup, no args.
    analyze_sentiment(payload, context) -> called once per request.
        payload: dict, the raw JSON request body (optionally pre-validated by
                 the Yeedu runtime against config/request_schema.json).
        context: dict, populated by the runtime as
                 {"request_id": str, "metrics": {"pending_requests": int,
                  "total_requests": int, "last_request_ts": str}}
        returns: JSON-serializable dict.
"""

import logging

from src.sentiment_analysis.exceptions import SentimentAnalysisError
from src.sentiment_analysis.inference import SentimentService
from src.sentiment_analysis.validation import validate_payload

logger = logging.getLogger(__name__)

sentiment_service = SentimentService()


def init() -> None:
    """No model to load — TextBlob's lexicon ships inside the package. Present for
    contract symmetry with other Yeedu Functions demos in this repo."""


def analyze_sentiment(payload: dict, context: dict) -> dict:
    """Score patient/customer feedback text for sentiment.

    Args:
        payload: expects `text`, a non-empty string.
        context: runtime-supplied request context (see module docstring).

    Returns:
        {"status": "success", "request_id": str, "sentiment": str,
         "polarity": float, "subjectivity": float}
        or on failure:
        {"status": "error", "request_id": str, "message": str}
    """
    request_id = context.get("request_id")

    try:
        text = validate_payload(payload)
        result = sentiment_service.analyze(text)
        return {
            "status": "success",
            "request_id": request_id,
            **result,
        }
    except SentimentAnalysisError as e:
        logger.warning("Feedback rejected for request %s: %s", request_id, e)
        return {"status": "error", "request_id": request_id, "message": str(e)}
    except Exception as e:
        logger.exception("Unexpected error handling request %s", request_id)
        return {"status": "error", "request_id": request_id, "message": str(e)}
