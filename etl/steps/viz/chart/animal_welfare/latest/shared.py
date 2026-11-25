"""Views shared by the animal welfare multidims.

The big animal welfare multidim (animal_welfare, shown as a data explorer) shows every view. Three smaller multidims
show a subset of those views: animals killed (animals_killed), live animals (live_animals), and animal welfare laws
(animal_welfare_laws). All views are defined here and in shared.config.yml, so each one is written only once.
"""

import re
from typing import Any

from owid.catalog import Table
from owid.catalog.utils import underscore

from etl.helpers import PathFinder

# Metrics shown in each of the smaller multidims.
METRICS_ANIMALS_KILLED = ["animals_killed", "animals_killed_for_consumption"]
METRICS_LIVE_ANIMALS = ["animals_alive", "caged_hens"]
METRICS_LAWS = ["fur_farming_status", "fur_trading_status", "bullfighting_status", "chick_culling_status"]

# Only fish and crustaceans are estimated as a range. Their views show one line for each estimate, for one country at a
# time, named and colored as in our charts on wild-caught and farmed fish. For each estimate in the data, its choice in
# the estimate dropdown (see ESTIMATE_DIMENSION), and its line.
ESTIMATES = {
    "midpoint": {"slug": "midpoint", "display": {"name": "Midpoint estimate", "color": "#ae2032"}},
    "upper limit": {"slug": "upper_limit", "display": {"name": "Upper estimate", "color": "#716b6c"}},
    "lower limit": {"slug": "lower_limit", "display": {"name": "Lower estimate", "color": "#716b6c"}},
}

# Dropdown of estimates, used instead of the lines by the big animal welfare multidim.
# NOTE: The published animal welfare multidim keeps this dropdown, since links to the old animal welfare explorer
# redirect to its views, and the id of each view includes its estimate. Drop it when those redirects are remapped.
ESTIMATE_NOT_APPLICABLE = "not_applicable"
ESTIMATE_DIMENSION = {
    "slug": "estimate",
    "name": "Estimate",
    "presentation": {"type": "dropdown"},
    "choices": [
        {"slug": "midpoint", "name": "Midpoint", "description": "Midpoint of the estimated range."},
        {"slug": "lower_limit", "name": "Lower limit", "description": "Lower end of the estimated range."},
        {"slug": "upper_limit", "name": "Upper limit", "description": "Upper end of the estimated range."},
        {
            "slug": ESTIMATE_NOT_APPLICABLE,
            "name": "Not applicable",
            "description": "Only fish and crustaceans are estimated as a range.",
        },
    ],
}

# Animal choices that are not in the animals_used_for_food data.
EXTRA_ANIMALS = [
    {
        "slug": "fur_animals",
        "name": "Fur animals",
        "description": "Animals farmed for their fur, such as minks and foxes.",
    },
    {"slug": "laying_hens", "name": "Laying hens"},
    {"slug": "chickens_killed_for_eggs", "name": "Chickens killed for eggs"},
    {"slug": "milk_cattle", "name": "Cattle used for milk"},
]

# Animal choices that show several animals at once.
LAND_ANIMALS_STACKED = {"slug": "land_animals_stacked", "name": "Land animals (stacked)"}
ALL_ANIMALS = {"slug": "all_animals", "name": "All animals (lines)"}

# Groups of the animal dropdown. Aggregates come first, in this order; the other animals follow alphabetically.
GROUP_AGGREGATES = "Aggregates"
GROUP_INDIVIDUAL_ANIMALS = "Individual animals"
AGGREGATES = ["all_land_animals", LAND_ANIMALS_STACKED["slug"], ALL_ANIMALS["slug"], "poultry"]

# Indicators of animals_killed_for_domestic_consumption shown in the multidims (by their name without the animal
# dimension). Animals killed in the country come from animals_used_for_food.
INDICATORS_KILLED_FOR_CONSUMPTION = ["killed_for_domestic_supply", "killed_for_domestic_supply_per_capita"]

# Chickens killed in egg production (hens and male chicks), from chickens_killed_for_eggs (counted where the eggs are
# produced). For each animal choice, its column in that table.
# NOTE: Hens and male chicks are shown together, because the data assumes one male chick killed per hen. Show them
# separately once the data accounts for in-ovo sexing.
# NOTE: These are not added to any total or stack: laying hens are also counted among the chickens slaughtered for meat.
ANIMALS_KILLED_FOR_EGGS = {
    "chickens_killed_for_eggs": "chickens_killed",
}

# Live animals that are not in the animals_used_for_food data, from FAOSTAT.
ANIMALS_ALIVE_FROM_FAOSTAT = {
    "laying_hens": "faostat_qcl/faostat_qcl_flat#eggs_from_hens__00001062__laying__005313__animals",
    "milk_cattle": "faostat_qcl/faostat_qcl_flat#raw_milk_of_cattle__00000882__milk_animals__005318__animals",
}

# Colors for each status of the chick culling laws.
CHICK_CULLING_STATUS_COLORS = {
    "Banned": "#759AC8",
    "Banned but not yet in effect": "#058580",
    "Not banned": "#AE2E3F",
    "Partially banned": "#A46F49",
}

# Settings of the views that show several lines or stacked areas for one country at a time.
ONE_COUNTRY = {"addCountryMode": "change-country", "selectedEntityNames": ["World"]}
# Settings of the stacked views. By default, a stacked chart only shows the years when every animal has data, and game
# animals only have data between 1991 and 2017.
STACKED = {"chartTypes": ["StackedArea"], "hasMapTab": False, "missingDataStrategy": "show"}


def _view(metric: str, animal: str, indicators: list[dict], per_capita: bool, config: dict | None = None) -> dict:
    view = {
        "dimensions": {
            "metric": metric,
            "animal": animal,
            "per_capita": "per_capita" if per_capita else "total",
        },
        "indicators": {"y": indicators},
    }
    if config:
        view["config"] = config
    return view


def _faust(tb: Table, column: str) -> dict:
    # Title and subtitle of a view with several indicators, taken from the indicator that the view is about.
    meta = tb[column].metadata
    faust = {"title": (meta.presentation.title_public if meta.presentation else None) or meta.title}
    if meta.description_short:
        faust["subtitle"] = meta.description_short
    return faust


def _world_latest(tb: Table, column: str) -> float:
    return tb[tb["country"] == "World"].sort_values("year")[column].dropna().iloc[-1]


def _check_stack_adds_up(tb: Table, columns: list[str], total_column: str) -> None:
    world = tb[tb["country"] == "World"]
    stacked = world[columns].sum(axis=1, min_count=1)
    total = world[total_column]
    informed = total.notna() & stacked.notna()
    error = f"The stacked animals do not add up to {total_column}."
    assert (abs(stacked[informed] - total[informed]) <= 1e-3 * total[informed]).all(), error


def create_config(
    paths: PathFinder,
    metrics: list[str] | None = None,
    animal_dimension: bool = True,
    estimate_dimension: bool = False,
) -> dict[str, Any]:
    """Create the config of an animal welfare multidim, keeping only the views of the given metrics (all if None).

    The adjacent config of the step gives the title and other top-level fields; shared.config.yml gives the
    dimensions and the views on laws and hens. Without the animal dimension, views are told apart by the other
    dimensions alone (each law names its animal). With the estimate dimension, each estimate of fish and crustaceans
    is a view of its own, instead of a line.
    """
    config = paths.load_config()
    shared = paths.load_config(filename="shared.config.yml")
    if metrics is None:
        metrics = [choice["slug"] for choice in shared["dimensions"][0]["choices"]]

    animals = {animal["slug"]: animal for animal in EXTRA_ANIMALS}
    views = []

    # Build the views of each animal count in animals_used_for_food, and collect the animal choices.
    tb = None
    if {"animals_killed", "animals_alive"} & set(metrics):
        tb = paths.load_dataset("animals_used_for_food").read("animals_used_for_food")
        # Columns of each (metric, animal, per capita), by estimate (None for animals not estimated as a range).
        columns = {}
        for column in tb.columns:
            if column in tb.metadata.primary_key:
                continue
            dims = tb[column].metadata.dimensions
            animal = underscore(dims["animal"])
            animals[animal] = {"slug": animal, "name": dims["animal"].capitalize()}
            key = (dims["metric"], animal, str(dims["per_capita"]).lower() == "true")
            columns.setdefault(key, {})[dims.get("estimate") or None] = column
        for (metric, animal, per_capita), by_estimate in columns.items():
            if None in by_estimate:
                indicators = [{"catalogPath": f"animals_used_for_food#{by_estimate[None]}"}]
                views.append(_view(metric, animal, indicators, per_capita))
                continue
            assert set(by_estimate) == set(ESTIMATES), f"Unexpected estimates for {animal}: {set(by_estimate)}"
            if estimate_dimension:
                for estimate, column in by_estimate.items():
                    view = _view(metric, animal, [{"catalogPath": f"animals_used_for_food#{column}"}], per_capita)
                    view["dimensions"]["estimate"] = ESTIMATES[estimate]["slug"]
                    views.append(view)
                continue
            # Show the estimated range as one line for each estimate.
            indicators = [
                {"catalogPath": f"animals_used_for_food#{by_estimate[estimate]}", "display": choice["display"]}
                for estimate, choice in ESTIMATES.items()
            ]
            view_config = {**_faust(tb, by_estimate["midpoint"]), **ONE_COUNTRY}
            views.append(_view(metric, animal, indicators, per_capita, config=view_config))

    # Add views of animals killed for the country's consumption.
    tb_consumption = None
    consumption = "animals_killed_for_domestic_consumption"
    if "animals_killed_for_consumption" in metrics:
        tb_consumption = paths.load_dataset(consumption).read(consumption)
        for column in tb_consumption.columns:
            if column in tb_consumption.metadata.primary_key:
                continue
            indicator = tb_consumption[column].metadata.original_short_name
            if indicator not in INDICATORS_KILLED_FOR_CONSUMPTION:
                continue
            animal = underscore(tb_consumption[column].metadata.dimensions["animal"])
            animals.setdefault(animal, {"slug": animal, "name": animal.replace("_", " ").capitalize()})
            indicators = [{"catalogPath": f"{consumption}#{column}"}]
            views.append(_view("animals_killed_for_consumption", animal, indicators, indicator.endswith("_per_capita")))

    # Add views of chickens killed in egg production.
    for per_capita in [False, True]:
        suffix = "_per_capita" if per_capita else ""
        for animal, column in ANIMALS_KILLED_FOR_EGGS.items():
            indicators = [{"catalogPath": f"chickens_killed_for_eggs#{column}{suffix}"}]
            views.append(_view("animals_killed", animal, indicators, per_capita))

    # Add views of live animals from FAOSTAT.
    for animal, catalog_path in ANIMALS_ALIVE_FROM_FAOSTAT.items():
        views.append(_view("animals_alive", animal, [{"catalogPath": catalog_path}], per_capita=False))

    # Add views that stack all land animals killed, and that show all animals killed as lines.
    for per_capita in [False, True]:
        if tb is not None and "animals_killed" in metrics:
            views += _views_of_all_animals(tb, per_capita)
        if tb_consumption is not None:
            views.append(_view_of_land_animals_stacked_for_consumption(tb_consumption, per_capita))
    if {view["dimensions"]["animal"] for view in views} & {LAND_ANIMALS_STACKED["slug"], ALL_ANIMALS["slug"]}:
        animals.update({choice["slug"]: choice for choice in [LAND_ANIMALS_STACKED, ALL_ANIMALS]})

    # Animal count views go first, followed by those on laws and hens.
    views += shared["views"]

    # Keep only the views of the given metrics.
    views = [view for view in views if view["dimensions"]["metric"] in metrics]

    # Sanity check: every view has a known animal.
    if animal_dimension:
        unknown = {view["dimensions"]["animal"] for view in views} - set(animals)
        assert not unknown, f"Unknown animals: {unknown}"

    # Color the chick culling status, if shown.
    if "chick_culling_status" in metrics:
        tb_chick = paths.load_dataset("chick_culling_laws").read("chick_culling_laws")
        _color_chick_culling_status(views, set(tb_chick["status"]))

    # Add the estimate dropdown, if wanted, after the animal dropdown.
    dimensions = shared["dimensions"]
    if estimate_dimension:
        for view in views:
            view["dimensions"].setdefault("estimate", ESTIMATE_NOT_APPLICABLE)
        position = [dimension["slug"] for dimension in dimensions].index("animal") + 1
        dimensions.insert(position, ESTIMATE_DIMENSION)

    # Fill the dimensions: metric choices in the given order, and animals grouped.
    for dimension in dimensions:
        if dimension["slug"] == "metric":
            choices = {choice["slug"]: choice for choice in dimension["choices"]}
            dimension["choices"] = [choices[metric] for metric in metrics]
        if dimension["slug"] == "animal":
            dimension["choices"] = _group_animals(animals)

    # Drop the animal dimension, if not wanted, and dimensions with a single choice in use (e.g. per capita, for laws).
    drop = {"animal"} if not animal_dimension else set()
    for dimension in dimensions:
        if len({view["dimensions"][dimension["slug"]] for view in views}) == 1:
            drop.add(dimension["slug"])
    dimensions = [dimension for dimension in dimensions if dimension["slug"] not in drop]
    for view in views:
        for slug in drop:
            del view["dimensions"][slug]
    config["default_dimensions"] = {
        slug: choice for slug, choice in config["default_dimensions"].items() if slug not in drop
    }

    config["dimensions"] = dimensions
    config["views"] = views

    return config


def _views_of_all_animals(tb: Table, per_capita: bool) -> list[dict]:
    # Columns of each land animal killed in the country (aggregates excluded), largest first in the world's latest year.
    land = {}
    total = None
    for column in tb.columns:
        if column in tb.metadata.primary_key:
            continue
        dims = tb[column].metadata.dimensions
        if dims["metric"] != "animals_killed" or (str(dims["per_capita"]).lower() == "true") != per_capita:
            continue
        animal = underscore(dims["animal"])
        if animal == "all_land_animals":
            total = column
        elif not dims.get("estimate") and animal not in AGGREGATES:
            land[animal] = column
    assert total is not None, "Missing the number of all land animals killed."
    land_columns = sorted(land.values(), key=lambda column: _world_latest(tb, column), reverse=True)
    _check_stack_adds_up(tb, land_columns, total)

    stacked = _view(
        "animals_killed",
        LAND_ANIMALS_STACKED["slug"],
        [{"catalogPath": f"animals_used_for_food#{column}"} for column in land_columns],
        per_capita,
        config={**_faust(tb, total), **ONE_COUNTRY, **STACKED},
    )

    # Lines of all animals: land animals, the midpoint of fish and crustaceans, and chickens killed for eggs.
    fish = [
        column
        for column in tb.columns
        if column not in tb.metadata.primary_key
        and tb[column].metadata.dimensions["metric"] == "animals_killed"
        and tb[column].metadata.dimensions.get("estimate") == "midpoint"
        and (str(tb[column].metadata.dimensions["per_capita"]).lower() == "true") == per_capita
    ]
    suffix = "_per_capita" if per_capita else ""
    indicators = [{"catalogPath": f"animals_used_for_food#{column}"} for column in land_columns + fish]
    for animal, column in ANIMALS_KILLED_FOR_EGGS.items():
        name = next(choice["name"] for choice in EXTRA_ANIMALS if choice["slug"] == animal)
        indicators.append({"catalogPath": f"chickens_killed_for_eggs#{column}{suffix}", "display": {"name": name}})
    lines = _view(
        "animals_killed",
        ALL_ANIMALS["slug"],
        indicators,
        per_capita,
        config={
            "title": "Animals killed for food per person" if per_capita else "Animals killed for food",
            "subtitle": "Fish and crustaceans are shown as the midpoint of their estimated range.",
            **ONE_COUNTRY,
            "hasMapTab": False,
            "yAxis": {"scaleType": "log", "canChangeScaleType": True},
        },
    )
    return [stacked, lines]


def _view_of_land_animals_stacked_for_consumption(tb: Table, per_capita: bool) -> dict:
    # Columns of each land animal killed for the country's consumption (aggregates excluded), largest first.
    indicator = INDICATORS_KILLED_FOR_CONSUMPTION[1 if per_capita else 0]
    land = {}
    for column in tb.columns:
        if column in tb.metadata.primary_key or tb[column].metadata.original_short_name != indicator:
            continue
        land[underscore(tb[column].metadata.dimensions["animal"])] = column
    total = land.pop("all_land_animals")
    land_columns = sorted(
        [column for animal, column in land.items() if animal not in AGGREGATES],
        key=lambda column: _world_latest(tb, column),
        reverse=True,
    )
    _check_stack_adds_up(tb, land_columns, total)
    return _view(
        "animals_killed_for_consumption",
        LAND_ANIMALS_STACKED["slug"],
        [{"catalogPath": f"animals_killed_for_domestic_consumption#{column}"} for column in land_columns],
        per_capita,
        config={**_faust(tb, total), **ONE_COUNTRY, **STACKED},
    )


def _group_animals(animals: dict[str, dict]) -> list[dict]:
    # Aggregates first, in their given order, then individual animals alphabetically.
    aggregates = [{**animals[slug], "group": GROUP_AGGREGATES} for slug in AGGREGATES if slug in animals]
    individual = sorted(
        [{**choice, "group": GROUP_INDIVIDUAL_ANIMALS} for slug, choice in animals.items() if slug not in AGGREGATES],
        key=lambda choice: choice["name"].lower(),
    )
    return aggregates + individual


def _color_chick_culling_status(views: list[dict], statuses: set[str]) -> None:
    # The chick culling status embeds each country's effective year (e.g. "Banned (2023)"). Color each value found in
    # the data by its plain status, and label it with that status, so the legend shows one entry per status.
    colors = {}
    labels = {}
    for value in sorted(statuses):
        plain = re.sub(r" \(\d{4}\)$", "", value)
        colors[value] = CHICK_CULLING_STATUS_COLORS[plain]
        if plain != value:
            labels[value] = plain
    for view in views:
        if view["dimensions"]["metric"] == "chick_culling_status":
            color_scale = view["config"]["map"]["colorScale"]
            color_scale["customCategoryColors"] = colors
            color_scale["customCategoryLabels"] = labels
