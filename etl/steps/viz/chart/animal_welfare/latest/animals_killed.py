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
] + ["wild_caught_fish_for_fishmeal_and_fish_oil"]  # Only world totals, and no values per person.

# Dimensions, in the order of the selectors.
DIMENSIONS = {"animal": "*", "metric": "*", "per_capita": "*", "estimate": "*"}

# Fish and crustaceans, estimated as a range.
FISH_AND_CRUSTACEANS = ["wild_caught_fish", "farmed_fish", "farmed_crustaceans"]
# Wild fish caught for fishmeal and fish oil, also estimated as a range.
# NOTE: They are also included in wild-caught fish, so they are not shown next to other animals.
FISHMEAL_AND_FISH_OIL = "wild_caught_fish_for_fishmeal_and_fish_oil"

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

# Names of the two bars of chickens in the view of land and aquatic animals (elsewhere, each appears on its own).
CHICKEN_BAR_NAMES = {"chickens": "Chickens (meat)", "chickens_killed_for_eggs": "Chickens (eggs)"}

# Settings shared by all views (views created by grouping do not inherit those of the config).
COMMON = {"originUrl": "https://ourworldindata.org/animal-welfare", "entityType": "country"}
# Settings of the views that show several lines or areas for one country at a time.
ONE_COUNTRY = {**COMMON, "addCountryMode": "change-country", "selectedEntityNames": ["World"]}

# Titles and subtitles of the views that combine several indicators.
TITLES = {
    "wild_caught_fish": "Fishes caught from the wild",
    "farmed_fish": "Farmed fishes killed for food",
    "farmed_crustaceans": "Farmed crustaceans killed for food",
    FISHMEAL_AND_FISH_OIL: "Wild fish caught for fishmeal and fish oil",
    "animals_killed": "Land animals slaughtered for meat",
    "animals_killed_for_consumption": "Land animals slaughtered for meat supplied to the country",
}
SUBTITLES = {
    "animals_killed": "Based on the country of production, not consumption.",
    "animals_killed_for_consumption": "Based on the country of consumption, not production.",
}
SUBTITLE_FISHMEAL_AND_FISH_OIL = (
    "Fishmeal and fish oil are mostly used as feed on fish and shrimp farms. These fish are also included in the number"
    " of wild-caught fish."
)
# Text of the data pages of the stacked view and the bar chart. Otherwise, a data page shows the text of the first
# indicator of its view (here, chickens). The stacked view has the key points of the view of all land animals.
DESCRIPTION_KEYS_STACKED = {
    "animals_killed": [
        "Additional deaths that happen during meat and dairy production prior to the slaughter, for example due to"
        " disease or accidents, are not included."
    ],
    "animals_killed_for_consumption": [],
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
                    "subtitle": "{subtitle}",
                    # The map shows the midpoint (the first indicator).
                    "hasMapTab": True,
                },
            }
        ],
        params={
            "title": lambda view: TITLES[view.dimensions["animal"]],
            "subtitle": lambda view: (
                SUBTITLE_FISHMEAL_AND_FISH_OIL
                if view.dimensions["animal"] == FISHMEAL_AND_FISH_OIL
                else SUBTITLES["animals_killed"]
            ),
            "per_person": per_person,
        },
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
                    # Let readers see the share of each animal.
                    "hideRelativeToggle": False,
                    # By default, a stacked chart only shows the years when every animal has data, and game animals
                    # (among the other land animals) only have data between 1991 and 2017.
                    "missingDataStrategy": "show",
                },
                "view_metadata": {
                    "presentation": {"title_public": "{title}{per_person}"},
                    "description_short": "{subtitle}",
                    "description_from_producer": "",
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
                    # Same note as in the static chart of land animals killed for food.
                    "note": "Chickens killed in egg production include male chickens, most culled soon after hatching,"
                    " and laying hens. Some are later sold for meat, so adding up the numbers would count those chickens"
                    " twice.",
                },
                "view_metadata": {
                    "presentation": {"title_public": "Animals killed for food{per_person}"},
                    "description_short": "Fish and crustaceans are shown as the midpoint of their estimated range.",
                    "description_key": [
                        "This chart does not include wild-caught crustaceans or farmed insects. According to one estimate"
                        " by [Rethink Priorities](https://rethinkpriorities.org/research-area/shrimp-the-animals-most-commonly-used-and-killed-for-food-production/),"
                        " around 25 trillion shrimps and prawns were caught from the wild in 2020, with a range of 6.5 to"
                        " 66 trillion. According to another estimate by [Rethink Priorities](https://rethinkpriorities.org/research-area/insects-raised-for-food-and-feed/),"
                        " 1 to 1.2 trillion insects are raised on farms each year for food and animal feed."
                    ],
                    "description_from_producer": "",
                    # Chickens killed in egg production are our own estimate.
                    "processing_level": "major",
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
    # latest value of up to 5 years earlier, since fish and crustaceans have no data for the latest years. And tell the
    # two bars of chickens apart, as in the animal dropdown. Give the stacked views the key points of all land animals.
    for view in c.views:
        if view.dimensions["animal"] in FISH_AND_CRUSTACEANS + [FISHMEAL_AND_FISH_OIL]:
            for indicator in view.indicators.y:
                color = "#ae2032" if "estimate_midpoint" in indicator.catalogPath else "#716b6c"
                indicator.display = {**(indicator.display or {}), "color": color}
        if view.dimensions["animal"] == FISHMEAL_AND_FISH_OIL:
            # There are only world totals, so a map would be empty.
            view.config = {**(view.config or {}), "hasMapTab": False}
        if view.dimensions["animal"] == "land_animals_stacked":
            view.metadata = {
                **(view.metadata or {}),
                "description_key": DESCRIPTION_KEYS_STACKED[view.dimensions["metric"]],
            }
        if view.dimensions["animal"] == "land_and_aquatic_animals":
            view.indicators.y = [
                indicator
                for indicator in view.indicators.y
                if not any(f"estimate_{bound}" in indicator.catalogPath for bound in ["upper_limit", "lower_limit"])
            ]
            for indicator in view.indicators.y:
                indicator.display = {**(indicator.display or {}), "tolerance": 5}
                for animal, name in CHICKEN_BAR_NAMES.items():
                    if f"__animal_{animal}__" in indicator.catalogPath:
                        indicator.display["name"] = name
            # NOTE: A data page lists only the sources of the first indicator of its view (chickens), so list those of
            # all indicators, once each (some come in several snapshots of the same data product).
            # Remove this once https://github.com/owid/owid-grapher/issues/7626 is fixed.
            origins = {}
            for indicator in view.indicators.y:
                for origin in tb[indicator.catalogPath.split("#")[-1]].metadata.origins:
                    origins.setdefault((origin.producer, origin.title), origin)
            view.metadata = {**(view.metadata or {}), "origins": [origin.to_dict() for origin in origins.values()]}

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
