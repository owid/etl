from pathlib import Path

# Given that the most recent data may be incomplete, we will keep only data points prior to (or at) the following year.
LAST_INFORMED_YEAR = 2020
# Directory for current version.
CURRENT_DIR = Path(__file__).parent
# Version of current garden datasets to be created.
VERSION = str(CURRENT_DIR.name)
# Version of meadow datasets to be imported.
MEADOW_VERSION = VERSION
