#
# NOTE: There should not be any etl dependency in this module.
#

import os
from pathlib import Path

BASE_DIR = Path(os.environ.get("BASE_DIR", Path(__file__).parent.parent))

# Dag
DAG_DIR = BASE_DIR / "dag"
DAG_FILE = DAG_DIR / "main.yml"
DAG_ARCHIVE_FILE = DAG_DIR / "archive" / "main.yml"
DAG_TEMP_FILE = DAG_DIR / "temp.yml"

# Lib paths (catalog, datautils)
LIB_DIR = BASE_DIR / "lib"

# Data folder (actual data)
DATA_DIR = BASE_DIR / "data"
DATA_MEADOW_DIR = DATA_DIR / "meadow"
DATA_GARDEN_DIR = DATA_DIR / "garden"
DATA_GRAPHER_DIR = DATA_DIR / "grapher"

# Export folder
EXPORT_DIR = BASE_DIR / "export"
EXPORT_MDIMS_DIR = EXPORT_DIR / "multidim"
EXPORT_EXPLORER_DIR = EXPORT_DIR / "explorers"

# Snapshots
SNAPSHOTS_DIR = BASE_DIR / "snapshots"

# ETL library
ETL_DIR = BASE_DIR / "etl"
STEP_DIR = ETL_DIR / "steps"
STEPS_DATA_DIR = STEP_DIR / "data"
STEPS_MEADOW_DIR = STEPS_DATA_DIR / "meadow"
STEPS_GARDEN_DIR = STEPS_DATA_DIR / "garden"
STEPS_GRAPHER_DIR = STEPS_DATA_DIR / "grapher"

# Apps
APPS_DIR = BASE_DIR / "apps"

# Schemas
SCHEMAS_DIR = BASE_DIR / "schemas"

# Cache
CACHE_DIR = BASE_DIR / ".cache"

# Documentation
DOCS_DIR = BASE_DIR / "docs"

# Regions paths
LATEST_REGIONS_VERSION = sorted((STEPS_GARDEN_DIR / "regions/").glob("*/regions.yml"))[-1].parts[-2]
LATEST_REGIONS_YML = STEPS_GARDEN_DIR / "regions" / LATEST_REGIONS_VERSION / "regions.yml"
LATEST_REGIONS_DATASET_PATH = BASE_DIR / "data/garden/regions" / LATEST_REGIONS_VERSION / "regions"

# WB Income groups.
LATEST_INCOME_VERSION = sorted((STEPS_GARDEN_DIR / "wb/").glob("*/income_groups.py"))[-1].parts[-2]
LATEST_INCOME_DATASET_PATH = BASE_DIR / "data/garden/wb" / LATEST_INCOME_VERSION / "income_groups"

# Population
LATEST_POPULATION_VERSION = sorted((STEPS_GARDEN_DIR / "demography/").glob("*/population"))[-1].parts[-2]
LATEST_POPULATION_DATASET_PATH = BASE_DIR / "data/garden/demography" / LATEST_POPULATION_VERSION / "population"

# NOTE: this is useful when your steps are defined in a different package
BASE_PACKAGE = os.environ.get("BASE_PACKAGE", "etl")

# DAG file to use by default.
# Use paths.DAG_ARCHIVE_FILE to load the complete dag, with active and archive steps.
# Otherwise use paths.DAG_FILE to load only active steps, ignoring archive ones.
DEFAULT_DAG_FILE = DAG_FILE

# Hidden ETL file that will keep the time it took to execute each step.
EXECUTION_TIME_FILE = BASE_DIR / ".execution_time.json"

# TODO: Remove this once we fully deprecate owid-content
# Default path to the explorers folder.
EXPLORERS_DIR = Path(os.environ.get("EXPLORERS_DIR", BASE_DIR.parent / "owid-content/explorers"))
