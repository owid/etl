import os
from pathlib import Path

BASE_DIR = Path(os.environ.get("BASE_DIR", Path(__file__).parent.parent))
DAG_DIR = BASE_DIR / "dag"
DAG_FILE = DAG_DIR / "main.yml"
DAG_ARCHIVE_FILE = DAG_DIR / "archive" / "main.yml"
DATA_DIR = BASE_DIR / "data"
SNAPSHOTS_DIR = BASE_DIR / "snapshots"
STEP_DIR = BASE_DIR / "etl" / "steps"

# Regions paths
LATEST_REGIONS_VERSION = sorted((STEP_DIR / "data/garden/regions/").glob("*/regions.yml"))[-1].parts[-2]
LATEST_REGIONS_YML = STEP_DIR / "data/garden/regions" / LATEST_REGIONS_VERSION / "regions.yml"
LATEST_REGIONS_DATASET_PATH = BASE_DIR / "data/garden/regions" / LATEST_REGIONS_VERSION / "regions"

# WB Income
LATEST_INCOME_VERSION = sorted((STEP_DIR / "data/garden/wb/").glob("*/income_groups.py"))[-1].parts[-2]
LATEST_INCOME_DATASET_PATH = BASE_DIR / "data/garden/wb" / LATEST_INCOME_VERSION / "income_groups"

# NOTE: this is useful when your steps are defined in a different package
BASE_PACKAGE = os.environ.get("BASE_PACKAGE", "etl")

# DAG file to use by default.
# Use paths.DAG_ARCHIVE_FILE to load the complete dag, with active and archive steps.
# Otherwise use paths.DAG_FILE to load only active steps, ignoring archive ones.
DEFAULT_DAG_FILE = DAG_FILE
