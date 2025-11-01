"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COMMON_CONFIG = {}


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("religious_composition")
    tb = ds.read("religious_composition", load_data=False)

    #
    # (optional) Adjust dimensions if needed
    #

    #
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        short_name="religious_composition",
        tb=tb,
        indicator_names=["share", "count_unrounded"],
        # dimensions={},
    )

    # Comparable view
    c.group_views(
        groups=[
            {
                "dimension": "religion",
                "choices": [
                    "christians",
                    "muslims",
                    "hindus",
                    "buddhists",
                    "jews",
                    "other_religions",
                    "religiously_unaffiliated",
                ],
                "choice_new_slug": "religion_distrib",
                "view_config": {
                    "hasMapTab": False,
                    "addCountryMode": "change-country",
                    "tab": "chart",
                    "chartTypes": ["DiscreteBar"],
                    "selectedFacetStrategy": "entity",
                    "title": "Religious composition",
                },
            }
        ]
    )
    #
    # (optional) Edit views
    #
    for view in c.views:
        if view.matches(religion="religion_distrib"):
            assert view.indicators.y is not None
            for y in view.indicators.y:
                display = {}
                if "other_religions" in y.catalogPath:
                    display["name"] = "Other religions"
                elif "unaffiliated" in y.catalogPath:
                    display["name"] = "Religiously unaffiliated"
                else:
                    display["name"] = y.catalogPath.split("_")[-1].title()
                y.display = display

    #
    # Save garden dataset.
    #
    c.save()
