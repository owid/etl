"""This step shows various features of Collections. It is intended as a reference, so that you can start using most of the available features!


Some of the features include:
- Load other collections as dependencies! Use `paths.load_collectionset` to do so. Note that the dependencies must be defined in the DAG (just like it happens with data steps).
- Combine multiple collections int one. The example below combines two collections into one, and adds a new dimension (dropdown) to it so that one can switch between collections.
- Tweak the metadata around dimensions (dropdowns). For that, you should use the .config.yml file next to your script, and know the dimension slugs that you want to tweak. See what we have done in dummy.config.yml!

NOTE: Run this step with command `etlr multidim/dummy/latest/dummy --export --private --instant`
"""

from etl.collection import combine_collections
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load configuration from adjacent yaml file.
    cs = paths.load_collectionset("covid")
    cols = [
        cs.read("covid_cases"),
        cs.read("covid_deaths"),
    ]

    # Combine collections into one, and add a new dimension to switch between them.
    ## In this example, each collection concerns a different indicator
    c = combine_collections(
        collections=cols,
        collection_name=paths.short_name,
        collection_dimension_slug="indicator",
        # collection_choices_names=["COVID-19 cases", "COVID-19 deaths"],
        config=paths.load_collection_config(),
        force_collection_dimension=True,
    )

    # Get common configuration from current views to add it to the new grouped view.
    # NOTE: `c.v` is equivalent to `c.views`
    common_config = c.v[0].config
    assert common_config is not None, "Common config should not be None"

    c.group_views(
        [
            {
                "dimension": "indicator",
                "choice_new_slug": "comparison",
                "view_config": common_config
                | {
                    "hasMapTab": False,
                    "chartTypes": ["ScatterPlot"],
                    "minTime": "latest",
                    "maxTime": "latest",
                    "title": lambda view: _set_title(view),
                    "subtitle": lambda view: _set_subtitle(view),
                },
                "view_metadata": {
                    "description_short": "Custom description for the grouped view.",
                    "description_key": [
                        "This is a custom description key for the grouped view.",
                        "One could also use a function here to generate dynamic descriptions based on dimensions.",
                        "I could go on and on, but you get the gist of it!",
                    ],
                    "presentation": {
                        "title_public": lambda view: _set_title(view),
                    },
                    "display": {
                        "unit": "cases or deaths",
                    },
                },
            }
        ]
    )

    adjust_grouped_view_as_scatter(c.views)

    # Save & upload
    c.save()


#####################################
# Functions to customize the chart config of the new grouped view
def _set_title(view):
    if view.dimensions["period"] == "weekly":
        return "Weekly comparison of COVID-19 Cases and Deaths"
    elif view.dimensions["period"] == "biweekly":
        return "Biweekly comparison of COVID-19 Cases and Deaths"
    else:
        raise ValueError(f"Unknown period: {view.dimensions['period']}")


def _set_subtitle(view):
    if view.dimensions["period"] == "weekly":
        subtitle = "Cumulative number of confirmed cases vs. deaths of COVID-19 over the previous week"
    elif view.dimensions["period"] == "biweekly":
        subtitle = "Cumulative number of confirmed cases vs. deaths of COVID-19 over the previous two weeks"
    else:
        raise ValueError(f"Unknown period: {view.dimensions['period']}")

    if view.dimensions["metric"] == "absolute":
        subtitle += "."
    elif view.dimensions["metric"] == "per_capita":
        subtitle += ", per million people."
    elif view.dimensions["metric"] == "change":
        subtitle = subtitle.replace("Cumulative", "Change in cumulative")
    else:
        raise ValueError(f"Unknown metric: {view.dimensions['metric']}")

    return subtitle


#####################################
# Function to make the grouped view a scatter plot!
def adjust_grouped_view_as_scatter(views):
    for v in views:
        if v.dimensions["indicator"] == "comparison":
            v.indicators.y, v.indicators.x = [v.indicators.y[0]], v.indicators.y[1]
            v.indicators.set_indicator(color="grapher/regions/2023-01-01/regions/regions#owid_region")
