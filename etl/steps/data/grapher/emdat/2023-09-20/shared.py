from pathlib import Path

from etl.paths import DATA_DIR

# Path to current directory.
CURRENT_DIR = Path(__file__).parent

# CURRENT_VERSION AND YEAR.
GARDEN_VERSION = "2022-11-24"
GARDEN_VERSION_YEAR = "2022"

# Path to garden dataset.
GARDEN_DATASET_PATH = DATA_DIR / f"garden/emdat/{GARDEN_VERSION}/natural_disasters"

DISASTER_TYPE_RENAMING = {
    "all_disasters": "All disasters",
    "drought": "Drought",
    "earthquake": "Earthquake",
    "extreme_temperature": "Extreme temperature",
    "flood": "Flood",
    "fog": "Fog",
    "glacial_lake_outburst": "Glacial lake outburst",
    "landslide": "Landslide",
    "dry_mass_movement": "Dry mass movement",
    "extreme_weather": "Extreme weather",
    "volcanic_activity": "Volcanic activity",
    "wildfire": "Wildfire",
}
