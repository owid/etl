from etl.config import DEFAULT_GRAPHER_SCHEMA
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
MULTIDIM_CONFIG = {
    "$schema": DEFAULT_GRAPHER_SCHEMA,
    "hasMapTab": True,
    "chartTypes": [],
    "tab": "map",
    "map": {
        "colorScale": {
            "customCategoryColors": {
                "Entire country": "#38AABA",
                "Not routinely administered": "#E77969",
                "Regions of the country": "#E9AD6F",
                "Specific risk groups": "#A2559C",
                "Demonstration projects": "#D7191C",
                "Adolescents": "#C8ADF5",
                "High risk areas": "#286BBB",
                "During outbreaks": "#398724",
            },
        },
    },
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("vaccination_introductions")
    tb = ds.read("vaccination_introductions", load_data=False)

    # Create and save collection
    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["intro"],
        dimensions=["description"],
        indicators_slug="vaccine",
        common_view_config=MULTIDIM_CONFIG,
    )
    c.save()
