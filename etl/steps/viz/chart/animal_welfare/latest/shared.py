"""Views shared by the animal welfare multidims.

The big animal welfare multidim (animal_welfare, shown as a data explorer) shows every view. Three smaller multidims
show a subset of those views: animals killed (animals_killed), live animals (live_animals), and animal welfare laws
(animal_welfare_laws). All views are defined here and in shared.config.yml, so each one is written only once.
"""

import re
from typing import Any

from owid.catalog.utils import underscore

from etl.helpers import PathFinder

# Metrics shown in each of the smaller multidims.
METRICS_ANIMALS_KILLED = ["animals_killed", "animals_killed_for_consumption"]
METRICS_LIVE_ANIMALS = ["animals_alive", "caged_hens"]
METRICS_LAWS = ["fur_farming_status", "fur_trading_status", "bullfighting_status", "chick_culling_status"]

# Estimate choice slug for each estimate in the data (only fish and crustaceans are estimated as a range).
ESTIMATES = {
    "midpoint": "midpoint",
    "lower limit": "lower_limit",
    "upper limit": "upper_limit",
}
ESTIMATE_NOT_APPLICABLE = "not_applicable"

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

# Metric of each indicator of animals_killed_for_domestic_consumption shown in the multidims (by its name without the
# animal dimension). Animals killed in the country come from animals_used_for_food, except for the groups it lacks.
INDICATORS_KILLED_FOR_CONSUMPTION = {
    "killed_for_domestic_supply": "animals_killed_for_consumption",
    "killed_for_domestic_supply_per_capita": "animals_killed_for_consumption",
    "killed_in_country": "animals_killed",
    "killed_in_country_per_capita": "animals_killed",
}
ANIMALS_KILLED_IN_COUNTRY_GROUPS = ["cattle_and_buffaloes", "sheep_and_goats"]

# Chickens killed in egg production (hens and male chicks), from chickens_killed_for_eggs (counted where the eggs are
# produced). For each animal choice, its column in that table.
# NOTE: Hens and male chicks are shown together, because the data assumes one male chick killed per hen. Show them
# separately once the data accounts for in-ovo sexing.
# NOTE: These are not added to any total: laying hens are also counted among the chickens slaughtered for meat.
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


def _view(metric: str, animal: str, catalog_path: str, per_capita: bool, estimate: str | None = None) -> dict:
    return {
        "dimensions": {
            "metric": metric,
            "animal": animal,
            "estimate": estimate or ESTIMATE_NOT_APPLICABLE,
            "per_capita": "per_capita" if per_capita else "total",
        },
        "indicators": {"y": [{"catalogPath": catalog_path}]},
    }


def create_config(paths: PathFinder, metrics: list[str] | None = None, animal_dimension: bool = True) -> dict[str, Any]:
    """Create the config of an animal welfare multidim, keeping only the views of the given metrics (all if None).

    The adjacent config of the step gives the title and other top-level fields; shared.config.yml gives the
    dimensions and the views on laws and hens. Without the animal dimension, views are told apart by the other
    dimensions alone (each law names its animal).
    """
    config = paths.load_config()
    shared = paths.load_config(filename="shared.config.yml")
    if metrics is None:
        metrics = [choice["slug"] for choice in shared["dimensions"][0]["choices"]]

    # Build one view per animal count indicator in animals_used_for_food, and collect the animal choices.
    animals = {animal["slug"]: animal for animal in EXTRA_ANIMALS}
    views = []
    if {"animals_killed", "animals_alive"} & set(metrics):
        tb = paths.load_dataset("animals_used_for_food").read("animals_used_for_food")
        for column in tb.columns:
            if column in tb.metadata.primary_key:
                continue
            dims = tb[column].metadata.dimensions
            animal = underscore(dims["animal"])
            animals[animal] = {"slug": animal, "name": dims["animal"].capitalize()}
            views.append(
                _view(
                    metric=dims["metric"],
                    animal=animal,
                    catalog_path=f"animals_used_for_food#{column}",
                    per_capita=str(dims["per_capita"]).lower() == "true",
                    estimate=ESTIMATES[dims["estimate"]] if dims.get("estimate") else None,
                )
            )

    # Add views of animals killed for the country's consumption, and of groups of animals killed in the country.
    if {"animals_killed", "animals_killed_for_consumption"} & set(metrics):
        name = "animals_killed_for_domestic_consumption"
        tb_consumption = paths.load_dataset(name).read(name)
        for column in tb_consumption.columns:
            if column in tb_consumption.metadata.primary_key:
                continue
            indicator = tb_consumption[column].metadata.original_short_name
            animal = underscore(tb_consumption[column].metadata.dimensions["animal"])
            metric = INDICATORS_KILLED_FOR_CONSUMPTION.get(indicator)
            if metric is None or (metric == "animals_killed" and animal not in ANIMALS_KILLED_IN_COUNTRY_GROUPS):
                continue
            animals.setdefault(animal, {"slug": animal, "name": animal.replace("_", " ").capitalize()})
            views.append(_view(metric, animal, f"{name}#{column}", per_capita=indicator.endswith("_per_capita")))

    # Add views of chickens killed in egg production.
    for per_capita in [False, True]:
        suffix = "_per_capita" if per_capita else ""
        for animal, column in ANIMALS_KILLED_FOR_EGGS.items():
            views.append(_view("animals_killed", animal, f"chickens_killed_for_eggs#{column}{suffix}", per_capita))

    # Add views of live animals from FAOSTAT.
    for animal, catalog_path in ANIMALS_ALIVE_FROM_FAOSTAT.items():
        views.append(_view("animals_alive", animal, catalog_path, per_capita=False))

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

    # Fill the dimensions: metric choices in the given order, and animals sorted alphabetically.
    dimensions = shared["dimensions"]
    for dimension in dimensions:
        if dimension["slug"] == "metric":
            choices = {choice["slug"]: choice for choice in dimension["choices"]}
            dimension["choices"] = [choices[metric] for metric in metrics]
        if dimension["slug"] == "animal":
            dimension["choices"] = sorted(animals.values(), key=lambda choice: choice["name"].lower())

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
