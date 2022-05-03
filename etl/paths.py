import os
from pathlib import Path

BASE_DIR = Path(os.environ.get("BASE_DIR", Path(__file__).parent.parent))
DAG_FILE = BASE_DIR / "dag.yml"
DATA_DIR = BASE_DIR / "data"
STEP_DIR = BASE_DIR / "etl" / "steps"
REFERENCE_DATASET = DATA_DIR / "garden" / "reference"

# NOTE: this is useful when your steps are defined in a different package
BASE_PACKAGE = os.environ.get("BASE_PACKAGE", "etl")
