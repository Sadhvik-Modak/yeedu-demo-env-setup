"""Yeedu Functions entrypoint for the Iris species classifier.

This file is the `--yeedu_functions_script_path` target — Yeedu loads it directly
by path and calls the named function. It lives at the project root because that's
what the Yeedu runtime expects to point at; the supporting package
(`src/iris_classifier/`) is imported as `src.iris_classifier`, made resolvable via
`--yeedu_functions_project_path` (or PYTHONPATH) pointing at the project root.

Deploy with `--yeedu_functions_function_name classify_plant_sample`. See README.md
for the full spark-submit / python command.

Contract (enforced by yeedu_functions_job.py, not by this module):
    init()                                   -> called once at job startup, no args.
    classify_plant_sample(payload, context)  -> called once per request.
        payload: dict, the raw JSON request body (optionally pre-validated by
                 the Yeedu runtime against config/request_schema.json).
        context: dict, populated by the runtime as
                 {"request_id": str, "metrics": {"pending_requests": int,
                  "total_requests": int, "last_request_ts": str}}
        returns: JSON-serializable dict.
"""

import logging

from src.iris_classifier.exceptions import IrisClassifierError
from src.iris_classifier.inference import IrisModelService
from src.iris_classifier.validation import validate_payload

logger = logging.getLogger(__name__)

model_service = IrisModelService()


def init() -> None:
    """Called once by the Yeedu Functions runtime at job startup."""
    model_service.load()


def classify_plant_sample(payload: dict, context: dict) -> dict:
    """Classify a nursery intake sample from its sepal/petal measurements.

    Args:
        payload: expects sepal_length, sepal_width, petal_length, petal_width.
        context: runtime-supplied request context (see module docstring).

    Returns:
        {"status": "success", "request_id": str, "prediction": str,
         "probabilities": dict[str, float]}
        or on failure:
        {"status": "error", "request_id": str, "message": str}
    """
    request_id = context.get("request_id")

    try:
        features = validate_payload(payload)
        result = model_service.predict(features)
        return {
            "status": "success",
            "request_id": request_id,
            **result,
        }
    except IrisClassifierError as e:
        logger.warning("Prediction rejected for request %s: %s", request_id, e)
        return {"status": "error", "request_id": request_id, "message": str(e)}
    except Exception as e:
        logger.exception("Unexpected error handling request %s", request_id)
        return {"status": "error", "request_id": request_id, "message": str(e)}
