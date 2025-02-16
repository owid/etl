from pathlib import Path

from etl import multidim
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
mdim_handler = multidim.MDIMHandler(paths)
CURRENT_DIR = Path(__file__).parent


def run(dest_dir: str) -> None:
    # Load configuration from adjacent yaml file.
    config = mdim_handler.load_config_from_yaml()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    table = paths.load_dataset("gbd_cause").read("gbd_cause_deaths", load_data=False)

    # Individual causes
    config["views"] += multidim.expand_views_with_access_db(
        config,
        {"metric": "*", "age": "*", "cause": "*"},
        table,
    )
    # Show all causes in a single view
    config["views"] += multidim.expand_views_with_access_db(
        config,
        {"metric": "*", "age": "*", "cause": "Side-by-side comparison of causes"},
        table,
    )

    mdim_handler.upsert_data_page("mdd-causes-of-death", config)
