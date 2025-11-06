"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COMMON_CONFIG = {}
COLORS_DISTRIB = {
    "christians": "#4C6A9C",
    "muslims": "#58AC8C",
    "hindus": "#C05917",
    "buddhists": "#BC8E5A",
    "jews": "#7C4DA0",
    "other_religions": "#9A5129",
    "religiously_unaffiliated": "#81C0C9",
}


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
                    "jews",
                    "muslims",
                    "hindus",
                    "buddhists",
                    "other_religions",
                    "religiously_unaffiliated",
                ],
                "choice_new_slug": "religion_distrib",
                "view_config": {
                    "hasMapTab": False,
                    "tab": "chart",
                    "chartTypes": ["StackedDiscreteBar"],
                    "selectedFacetStrategy": "none",
                    "title": "{indicator} by religious affiliation",
                },
                "view_metadata": {
                    "presentation": {
                        "title_public": "{indicator} by religious affiliation",
                    },
                    "description_short": "Distribution of the population by religious affiliation.",
                },
            }
        ],
        params={
            "indicator": lambda view: "Share of population" if view.matches(indicator="share") else "Number of people",
        },
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
                    display["color"] = COLORS_DISTRIB["other_religions"]
                elif "unaffiliated" in y.catalogPath:
                    display["name"] = "No religion"
                    display["color"] = COLORS_DISTRIB["religiously_unaffiliated"]
                else:
                    display["name"] = y.catalogPath.split("_")[-1].title()
                    for religion, color in COLORS_DISTRIB.items():
                        if religion in y.catalogPath:
                            display["color"] = color
                y.display = display

    #
    # Save garden dataset.
    #
    c.save()
