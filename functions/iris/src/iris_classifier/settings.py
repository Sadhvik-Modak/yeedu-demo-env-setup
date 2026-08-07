"""Environment-driven configuration for the Iris classifier function."""

import os
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_ROOT.parent.parent

DEFAULT_MODEL_PATH = PROJECT_ROOT / "models" / "iris_logistic_regression.pkl"

# Override at deploy time if the model artifact is staged elsewhere
# (e.g. mounted from a workspace volume rather than shipped alongside the code).
MODEL_PATH = Path(os.getenv("IRIS_MODEL_PATH", str(DEFAULT_MODEL_PATH)))
