from pathlib import Path

import yaml

from etl import multidim
from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

CURRENT_DIR = Path(__file__).parent


def run(dest_dir: str) -> None:
    engine = get_engine()

    # Load YAML file
    with open(CURRENT_DIR / "causes_of_death.yml") as istream:
        config = yaml.safe_load(istream)

    # Add views for all dimensions
    table = "grapher/ihme_gbd/2024-05-20/gbd_cause/gbd_cause_deaths"
    # Individual causes
    config["views"] += multidim.expand_views_with_access_db(
        config, {"metric": "*", "age": "*", "cause": "*"}, table, engine
    )
    # Show all causes in a single view
    config["views"] += multidim.expand_views_with_access_db(
        config, {"metric": "*", "age": "*", "cause": "Side-by-side comparison of causes"}, table, engine
    )

    multidim.upsert_multidim_data_page("mdd-causes-of-death", config, engine)
