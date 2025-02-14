from pathlib import Path

from etl import multidim
from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

CURRENT_DIR = Path(__file__).parent


def run(dest_dir: str) -> None:
    engine = get_engine()

    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Add views for all dimensions
    table = paths.load_dataset("gbd_cause").read("gbd_cause_deaths", load_data=False)

    # Individual causes
    config["views"] += multidim.expand_views(config, {"metric": "*", "age": "*", "cause": "*"}, table, engine)
    # Show all causes in a single view
    config["views"] += multidim.expand_views(
        config, {"metric": "*", "age": "*", "cause": "Side-by-side comparison of causes"}, table, engine
    )

    multidim.upsert_multidim_data_page("mdd-causes-of-death", config, engine)
