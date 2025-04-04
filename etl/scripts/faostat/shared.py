"""Common definitions used by scripts to create new snapshots and data steps."""

import datetime as dt
from typing import Dict, List, Tuple

from structlog import get_logger

from etl.paths import DAG_DIR

# Initialize logger.
log = get_logger()

# Version tag to assign to new folders (both in S3 bucket and in index).
VERSION = str(dt.date.today())
# Global namespace for datasets.
NAMESPACE = "faostat"
# Default snapshot file extension (to be used when creating a new snapshot).
SNAPSHOTS_FILE_EXTENSION = "zip"
# URL where FAOSTAT can be manually accessed (used in metadata, but not to actually retrieve the data).
FAO_DATA_URL = "http://www.fao.org/faostat/en/#data"
# Metadata source name.
SOURCE_NAME = "Food and Agriculture Organization of the United Nations"
# Short attribution.
ATTRIBUTION_SHORT = "FAOSTAT"
# Metadata related to license.
LICENSE_URL = "http://www.fao.org/contact-us/terms/db-terms-of-use/en"
LICENSE_NAME = "CC BY-NC-SA 3.0 IGO"
# Maximum number of characters for item_code.
# WARNING: These definitions should coincide with those given in the shared module of the garden step.
# FAOSTAT "item_code" is usually an integer number, however sometimes it has decimals and sometimes it contains letters.
# So we will convert it into a string of this number of characters (integers will be prepended with zeros).
N_CHARACTERS_ITEM_CODE = 8
# Idem for faostat_sdgb and faostat_fs (that have different, longer item codes with digits and letters).
N_CHARACTERS_ITEM_CODE_EXTENDED = 15
# Maximum number of characters for element_code (integers will be prepended with zeros).
N_CHARACTERS_ELEMENT_CODE = 6
# Codes of FAOSTAT domains to download from FAO and upload to bucket.
# This is the list that will determine the datasets (faostat_*) to be created in all further etl data steps.
INCLUDED_DATASETS_CODES = [
    # Country Investment Statistics Profile
    "cisp",
    # Food Balances: Food Balances (2010-).
    "fbs",
    # Food Balances: Food Balances (-2013, old methodology and population).
    "fbsh",
    # Food Security and Nutrition: Suite of Food Security Indicators.
    "fs",
    # Land, Inputs and Sustainability: Land Cover.
    "lc",
    # Production: Crops and livestock products.
    "qcl",
    # Production: Production Indices.
    # NOTE: This dataset is not used in grapher charts directly or in explorers, but it's used by the additional_variables dataset (and used in a chart).
    "qi",
    # Production: Value of Agricultural Production.
    "qv",
    # Land, Inputs and Sustainability: Fertilizers by Nutrient.
    "rfn",
    # Land, Inputs and Sustainability: Land Use.
    "rl",
    # Land, Inputs and Sustainability: Pesticides Use.
    "rp",
    # SDG Indicators: SDG Indicators.
    "sdgb",
    # Removed from the list (as they have not been used and were causing issues).
    # Cost and Affordability of a Healthy Diet.
    # "cahd",
    # Land, Inputs and Sustainability: Livestock Patterns.
    # "ek",
    # Climate Change: Emissions intensities.
    # "ei",
    # Land, Inputs and Sustainability: Livestock Manure.
    # "emn",
    # Land, Inputs and Sustainability: Soil nutrient budget.
    # "esb",
    # Discontinued archives and data series: Food Aid Shipments (WFP).
    # "fa",
    # Forestry: Forestry Production and Trade.
    # "fo",
    # Energy use.
    # "gn",
    # Land, Inputs and Sustainability: Fertilizers by Product.
    # "rfb",
    # Land, Inputs and Sustainability: Pesticides Trade.
    # "rt",
    # Trade: Crops and livestock products.
    # "tcl",
    # Trade: Trade Indices.
    # "ti",
    # World Census of Agriculture.
    # "wcad",
    # The following domains used to exist in FAOSTAT, but they have been removed.
    # Land, Inputs and Sustainability: Fertilizers indicators.
    # "ef",
    # Land, Inputs and Sustainability: Land use indicators.
    # "el",
    # Land, Inputs and Sustainability: Pesticides indicators.
    # "ep",
    # The following were used in the past, but they are not used anymore in charts.
    # Credit to Agriculture.
    # "ic",
    # Food Balances: Supply Utilization Accounts.
    # "scl",
]
# URL for dataset codes in FAOSTAT catalog.
# This is the URL used to get the remote location of the actual data files to be downloaded, and the date of their
# latest update.
FAO_CATALOG_URL = "http://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json"
# Base URL of API, used to download metadata (about countries, elements, items, etc.).
# NOTE: It seems that the following link doesn't work for fenixservices, but it does work for faostatservices.
# Maybe they are transitioning towards the latter?
API_BASE_URL = "https://faostatservices.fao.org/api/v1/en/definitions/domain"
# Name of additional metadata step file (without extension).
ADDITIONAL_METADATA_FILE_NAME = f"{NAMESPACE}_metadata"
# Path to dag file for FAOSTAT steps.
DAG_FILE = DAG_DIR / "faostat.yml"
# Name of shared module containing the run function (without extension).
RUN_FILE_NAME = "shared"
# Glob pattern to match version folders like "YYYY-MM-DD".
# Note: This is not a regular expression (glob does not accept them), but it works both for glob and for re.
GLOB_VERSION_PATTERN = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"
# Additional dependencies to add to each dag line of a specific channel, for new datasets (ones not in the dag).
# Give each dependency as a tuple of (namespace, channel, step_name). The latest version of that step will be assumed.
ADDITIONAL_DEPENDENCIES: Dict[str, List[Tuple[str, str, str]]] = {
    "meadow": [],
    "garden": [
        (NAMESPACE, "garden", f"{NAMESPACE}_metadata"),
        ("demography", "garden", "population"),
        ("regions", "garden", "regions"),
        ("wb", "garden", "income_groups"),
    ],
    "grapher": [],
}
# List of additional files (with extension) that, if existing, should be copied over from the latest version to the new
# (besides the files of each of the steps).
ADDITIONAL_FILES_TO_COPY = [
    RUN_FILE_NAME + ".py",
    f"{NAMESPACE}.countries.json",
    f"{NAMESPACE}.excluded_countries.json",
    "custom_datasets.csv",
    "custom_elements_and_units.csv",
    "custom_items.csv",
    "detected_anomalies.py",
    "value_amendments.csv",
]
# Note: Further custom rules are applied to the list of steps to run.
# These rules are defined in apply_custom_rules_to_list_of_steps_to_run.
