from pathlib import Path

from etl import multidim
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
CURRENT_DIR = Path(__file__).parent


def run(dest_dir: str) -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

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

    multidim.upsert_multidim_data_page("mdd-causes-of-death", config, paths=paths)
