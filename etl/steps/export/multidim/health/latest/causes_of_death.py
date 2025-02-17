import copy
from pathlib import Path
from typing import Any

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
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    table = paths.load_dataset("gbd_cause").read("gbd_cause_deaths", load_data=False)

    # Get all combinations of dimensions
    config_new = multidim.expand_config(table, dimensions=["cause", "age", "metric"])

    config["dimensions"][0]["choices"] += [
        c for c in config_new["dimensions"][0]["choices"] if c["slug"] != "All causes"
    ]

    # Group age and metric views under "Side-by-side comparison of causes"
    grouped_views = group_views(config_new["views"], by=["age", "metric"])
    for view in grouped_views:
        view["dimensions"]["cause"] = "Side-by-side comparison of causes"

    # Add views to config
    config["views"] += config_new["views"]
    config["views"] += grouped_views

    multidim.upsert_multidim_data_page("mdd-causes-of-death", config, engine, dependencies=paths.dependencies)


def group_views(views: dict[str, Any], by: list[str]) -> list[dict[str, Any]]:
    """
    Group views by the specified dimensions. Concatenate indicators for the same group.
    """
    views = copy.deepcopy(views)

    grouped_views = {}
    for view in views:
        # Group key
        key = tuple(view["dimensions"][dim] for dim in by)  # type: ignore

        if key not in grouped_views:
            # Turn indicators into a list
            view["indicators"]["y"] = [view["indicators"]["y"]]  # type: ignore

            # Add to dictionary
            grouped_views[key] = view
        else:
            grouped_views[key]["indicators"]["y"].append(view["indicators"]["y"])  # type: ignore

    return list(grouped_views.values())
