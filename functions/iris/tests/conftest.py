import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_PATH = PROJECT_ROOT / "src"

for path in (SRC_PATH, PROJECT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
