from etl import multidim
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    table = paths.load_dataset("gbd_cause").read("gbd_cause_deaths", load_data=False)

    # Get all combinations of dimensions
    config_new = multidim.expand_config(table, dimensions=["cause", "age", "metric"])

    config["dimensions"][0]["choices"] += [
        c for c in config_new["dimensions"][0]["choices"] if c["slug"] != "All causes"
    ]

    # Group age and metric views under "Side-by-side comparison of causes"
    grouped_views = multidim.group_views(config_new["views"], by=["age", "metric"])
    for view in grouped_views:
        view["dimensions"]["cause"] = "Side-by-side comparison of causes"

    # Add views to config
    config["views"] += config_new["views"]
    config["views"] += grouped_views

    multidim.upsert_multidim_data_page(
        config=config,
        paths=paths,
    )
