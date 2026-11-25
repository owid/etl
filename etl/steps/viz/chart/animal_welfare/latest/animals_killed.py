"""Create the multidim of animals killed for food, counted where they are killed or where their meat is consumed."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Indicators of animals killed (each one from its own source), in total and per person.
INDICATORS = [
    f"{indicator}{suffix}"
    for indicator in [
        "land_animals_killed",
        "land_animals_killed_for_consumption",
        "chickens_killed_for_eggs",
        "chickens_killed_for_eggs_for_consumption",
        "wild_caught_fish",
        "farmed_fish",
        "farmed_crustaceans",
    ]
    for suffix in ["", "_per_capita"]
]

# Dimensions, in the order of the selectors.
DIMENSIONS = {"animal": "*", "metric": "*", "per_capita": "*", "estimate": "*"}

# Animals estimated as a range.
FISH_AND_CRUSTACEANS = ["wild_caught_fish", "farmed_fish", "farmed_crustaceans"]

# Land animals stacked, from bottom to top (as in our chart of land animals slaughtered for meat).
LAND_ANIMALS_STACKED = [
    "chickens",
    "ducks",
    "pigs",
    "geese",
    "sheep",
    "rabbits",
    "turkeys",
    "goats",
    "cattle",
    "other_land_animals",
]

# Animals shown side by side (as bars) in the view of land and aquatic animals.
# NOTE: Chickens killed for eggs are never stacked with other animals, since laying hens are also counted among the
# chickens slaughtered for meat.
LAND_AND_AQUATIC_ANIMALS = LAND_ANIMALS_STACKED[:-1] + [
    "buffaloes",
    "camels",
    "donkeys",
    "game_animals",
    "horses",
    "mules",
    "other_camelids",
    "other_rodents",
    "pigeons",
    "chickens_killed_for_eggs",
    *FISH_AND_CRUSTACEANS,
]

# Settings shared by all views (views created by grouping do not inherit those of the config).
COMMON = {"originUrl": "https://ourworldindata.org/animal-welfare", "entityType": "country"}
# Settings of the views that show several lines or areas for one country at a time.
ONE_COUNTRY = {**COMMON, "addCountryMode": "change-country", "selectedEntityNames": ["World"]}

# Titles and subtitles of the views that combine several indicators.
TITLES = {
    "wild_caught_fish": "Fishes caught from the wild",
    "farmed_fish": "Farmed fishes killed for food",
    "farmed_crustaceans": "Farmed crustaceans killed for food",
    "animals_killed": "Land animals slaughtered for meat",
    "animals_killed_for_consumption": "Land animals slaughtered for meat supplied to the country",
}
SUBTITLES = {
    "animals_killed": "Based on the country of production, not consumption.",
    "animals_killed_for_consumption": "Based on the country of consumption, not production.",
}


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
    # Create one view for each animal count, with one sub-chart per indicator.
    c = paths.create_chart(
        config=config,
        tb=[tb] * len(INDICATORS),
        indicator_names=[[indicator] for indicator in INDICATORS],
        dimensions=DIMENSIONS,
    )

    # Show the estimated range of fish and crustaceans as three lines, instead of a choice of estimate.
    c.group_views(
        groups=[
            {
                "dimension": "estimate",
                "choices": ["midpoint", "upper_limit", "lower_limit"],
                "choice_new_slug": "range",
                "replace": True,
                "view_config": {
                    **ONE_COUNTRY,
                    "title": "{title}{per_person}",
                    "subtitle": SUBTITLES["animals_killed"],
                    # The map shows the midpoint (the first indicator).
                    "hasMapTab": True,
                },
            }
        ],
        params={"title": lambda view: TITLES[view.dimensions["animal"]], "per_person": per_person},
    )
    # Each animal now has a single estimate, so the estimate selector is not needed.
    c.dimensions = [dimension for dimension in c.dimensions if dimension.slug != "estimate"]
    for view in c.views:
        del view.dimensions["estimate"]

    # Add a view that stacks land animals, and another that shows land and aquatic animals side by side.
    c.group_views(
        groups=[
            {
                "dimension": "animal",
                "choices": LAND_ANIMALS_STACKED,
                "choice_new_slug": "land_animals_stacked",
                "view_config": {
                    **ONE_COUNTRY,
                    "title": "{title}{per_person}",
                    "subtitle": "{subtitle}",
                    "chartTypes": ["StackedArea"],
                    "hasMapTab": False,
                    # By default, a stacked chart only shows the years when every animal has data, and game animals
                    # (among the other land animals) only have data between 1991 and 2017.
                    "missingDataStrategy": "show",
                },
            },
            {
                "dimension": "animal",
                "choices": LAND_AND_AQUATIC_ANIMALS,
                "choice_new_slug": "land_and_aquatic_animals",
                "view_config": {
                    **ONE_COUNTRY,
                    "title": "Animals killed for food{per_person}",
                    "subtitle": "Fish and crustaceans are shown as the midpoint of their estimated range.",
                    "hasMapTab": False,
                    # Only bars: as lines, the animals are either flat at zero (on a linear scale) or tangled (on a
                    # log scale), and each one has its own view.
                    "chartTypes": ["DiscreteBar"],
                },
            },
        ],
        params={
            "title": lambda view: TITLES[view.dimensions["metric"]],
            "subtitle": lambda view: SUBTITLES[view.dimensions["metric"]],
            "per_person": per_person,
        },
    )

    # Color the midpoint of each range red and its bounds grey, as in our charts on wild-caught and farmed fish.
    # In the view of land and aquatic animals, show only the midpoint of fish and crustaceans. Also, let each bar show the
    # latest value of up to 5 years earlier, since fish and crustaceans have no data for the latest years.
    for view in c.views:
        if view.dimensions["animal"] in FISH_AND_CRUSTACEANS:
            for indicator in view.indicators.y:
                color = "#ae2032" if "estimate_midpoint" in indicator.catalogPath else "#716b6c"
                indicator.display = {**(indicator.display or {}), "color": color}
        if view.dimensions["animal"] == "land_and_aquatic_animals":
            view.indicators.y = [
                indicator
                for indicator in view.indicators.y
                if not any(f"estimate_{bound}" in indicator.catalogPath for bound in ["upper_limit", "lower_limit"])
            ]
            for indicator in view.indicators.y:
                indicator.display = {**(indicator.display or {}), "tolerance": 5}

    # Remove the views that are only needed to build the others: land and aquatic animals based on consumption (which has
    # no fish or eggs), and other land animals on their own.
    c.drop_views(
        [
            {"animal": "land_and_aquatic_animals", "metric": "animals_killed_for_consumption"},
            {"animal": "other_land_animals"},
        ]
    )

    #
    # Save outputs.
    #
    c.save()
