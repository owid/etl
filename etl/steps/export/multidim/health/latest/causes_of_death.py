from etl.collection import multidim
from etl.collection.utils import group_views
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    table = paths.load_dataset("gbd_cause").read("gbd_cause_deaths", load_data=False)

    # Get all combinations of dimensions
    config_new = multidim.expand_config(table)

    # Fill choices from TableMeta and VariableMeta dimensions info
    config["dimensions"] = multidim.combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config["dimensions"],
    )

    # Group age and metric views under "Side-by-side comparison of causes"
    grouped_views = group_views(config_new["views"], by=["age", "metric"])
    for view in grouped_views:
        view["dimensions"]["cause"] = "Side-by-side comparison of causes"

    # Add views to config
    config["views"] += config_new["views"]
    config["views"] += grouped_views

    mdim = paths.create_mdim(config=config)

    mdim.sort_choices(
        {
            "age": [
                "All ages",
                "Age-standardized",
                "<5 years",
                "5-14 years",
                "15-49 years",
                "50-69 years",
                "70+ years",
            ]
        }
    )
    mdim.save()
