from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DAG_FILE = BASE_DIR / "dag.yml"
DATA_DIR = BASE_DIR / "data"
STEP_DIR = BASE_DIR / "etl" / "steps"
REFERENCE_DATASET = DATA_DIR / "reference"
