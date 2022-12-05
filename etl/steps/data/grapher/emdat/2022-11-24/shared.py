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
    "animal_accident": "Animal accident",
    "drought": "Drought",
    "earthquake": "Earthquake",
    "epidemic": "Epidemic",
    "extreme_temperature": "Extreme temperature",
    "flood": "Flood",
    "fog": "Fog",
    "glacial_lake_outburst": "Glacial lake outburst",
    "impact": "Impact",
    "insect_infestation": "Insect infestation",
    "landslide": "Landslide",
    "mass_movement__dry": "Dry mass movement",
    "storm": "Storm",
    "volcanic_activity": "Volcanic activity",
    "wildfire": "Wildfire",
}
