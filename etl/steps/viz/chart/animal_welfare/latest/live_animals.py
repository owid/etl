"""Create the multidim of live animals."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Indicators of live animals, in total and per person.
INDICATORS = ["live_animals", "live_animals_per_capita"]

# Dimensions, in the order of the selectors.
DIMENSIONS = {"animal": "*", "metric": "*", "per_capita": "*", "estimate": "*"}

# Live land animals stacked, from bottom to top (from the most to the least numerous).
LAND_ANIMALS_STACKED = [
    "chickens",
    "cattle",
    "sheep",
    "ducks",
    "goats",
    "pigs",
    "geese",
    "turkeys",
    "buffaloes",
    "rabbits",
    "other_land_animals",
]

# Settings of the views that show several areas for one country at a time (views created by grouping do not inherit
# those of the config).
ONE_COUNTRY = {
    "originUrl": "https://ourworldindata.org/animal-welfare",
    "entityType": "country",
    "addCountryMode": "change-country",
    "selectedEntityNames": ["World"],
}

# Subtitle of all views (as in the indicator of each animal).
SUBTITLE = (
    "Number of animals alive at a given time of the year. This is different from the number of animals slaughtered in "
    "a given year."
)


def per_person(view) -> str:
    return " per person" if view.dimensions["per_capita"] == "per_capita" else ""


def run() -> None:
    #
    # Load inputs.
    #
    # Load configuration from adjacent yaml file.
    config = paths.load_config()

    # Load grapher dataset of animals used for food.
    tb = paths.load_dataset("animals_used_for_food").read("animals_used_for_food", load_data=False)

    #
    # Process data.
    #
    # Create one view for each live animal, in total and per person, with one sub-chart per indicator.
    c = paths.create_chart(
        config=config,
        tb=[tb] * len(INDICATORS),
        indicator_names=[[indicator] for indicator in INDICATORS],
        dimensions=DIMENSIONS,
    )
    # All live animals have the same metric and a single estimate, so those selectors are not needed.
    c.dimensions = [dimension for dimension in c.dimensions if dimension.slug not in ["metric", "estimate"]]
    for view in c.views:
        del view.dimensions["metric"]
        del view.dimensions["estimate"]

    # Add a view that stacks land animals.
    c.group_views(
        groups=[
            {
                "dimension": "animal",
                "choices": LAND_ANIMALS_STACKED,
                "choice_new_slug": "land_animals_stacked",
                "view_config": {
                    **ONE_COUNTRY,
                    "title": "Live land animals{per_person}",
                    "subtitle": SUBTITLE,
                    "chartTypes": ["StackedArea"],
                    "hasMapTab": False,
                    "hideRelativeToggle": False,
                    # By default, a stacked chart only shows the years when every animal has data.
                    "missingDataStrategy": "show",
                },
                "view_metadata": {
                    "presentation": {"title_public": "Live land animals{per_person}"},
                    "description_short": SUBTITLE,
                    "description_from_producer": "",
                },
            },
        ],
        params={"per_person": per_person},
    )

    # Remove the view that is only needed to build the stacked view.
    c.drop_views([{"animal": "other_land_animals"}])

    #
    # Save outputs.
    #
    c.save()
