"""Yeedu Functions entrypoint for Insurance claim fraud/risk scoring.

This file is the `--yeedu_functions_script_path` target — Yeedu loads it directly
by path and calls the named function. It lives at the project root because that's
what the Yeedu runtime expects to point at; the supporting package
(`src/fraud_scoring/`) is imported as `src.fraud_scoring`, made resolvable via
`--yeedu_functions_project_path` (or PYTHONPATH) pointing at the project root.

Deploy with `--yeedu_functions_function_name score_claim`. See README.md for the full
spark-submit / python command.

Contract (enforced by yeedu_functions_job.py, not by this module):
    init()                            -> called once at job startup, no args.
    score_claim(payload, context)     -> called once per request.
        payload: dict, the raw JSON request body (optionally pre-validated by
                 the Yeedu runtime against config/request_schema.json).
        context: dict, populated by the runtime as
                 {"request_id": str, "metrics": {"pending_requests": int,
                  "total_requests": int, "last_request_ts": str}}
        returns: JSON-serializable dict.
"""

import logging

from src.fraud_scoring.exceptions import FraudScoringError
from src.fraud_scoring.inference import FraudModelService
from src.fraud_scoring.validation import validate_payload

logger = logging.getLogger(__name__)

model_service = FraudModelService()


def init() -> None:
    """Called once by the Yeedu Functions runtime at job startup."""
    model_service.load()


def score_claim(payload: dict, context: dict) -> dict:
    """Score an insurance claim for fraud risk at intake.

    Args:
        payload: expects claim_amount, policy_tenure_months, num_prior_claims,
                 premium_amount, claim_to_premium_ratio, days_to_report.
        context: runtime-supplied request context (see module docstring).

    Returns:
        {"status": "success", "request_id": str, "risk_label": str,
         "fraud_probability": float}
        or on failure:
        {"status": "error", "request_id": str, "message": str}
    """
    request_id = context.get("request_id")

    try:
        features = validate_payload(payload)
        result = model_service.score(features)
        return {
            "status": "success",
            "request_id": request_id,
            **result,
        }
    except FraudScoringError as e:
        logger.warning("Claim rejected for request %s: %s", request_id, e)
        return {"status": "error", "request_id": request_id, "message": str(e)}
    except Exception as e:
        logger.exception("Unexpected error handling request %s", request_id)
        return {"status": "error", "request_id": request_id, "message": str(e)}
