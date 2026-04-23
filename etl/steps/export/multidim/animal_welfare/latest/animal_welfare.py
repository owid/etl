"""Create a multidimensional collection for animal welfare indicators."""

from copy import deepcopy
from typing import Any

from etl.collection import expand_config
from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Label to assign to choices of dimensions that are irrelevant for a view.
EMPTY_DIMENSION_LABEL = ""
IRRELEVANT_DIMENSION_SLUG = "not_applicable"


def _improve_dimension_names(dimension: dict[str, Any], transformation, replacements: dict[str, str]) -> None:
    for field, value in dimension.items():
        if field == "name":
            if value in replacements:
                dimension["name"] = replacements[value]
            else:
                dimension["name"] = transformation(value)
        if field == "choices":
            for choice in value:
                _improve_dimension_names(choice, transformation=transformation, replacements=replacements)


def improve_config_names(config: dict[str, Any], transformation=None, replacements: dict[str, str] | None = None):
    """Create human-readable names out of slugs."""
    if transformation is None:

        def transformation(slug):
            return slug.replace("_", " ").capitalize()

    if replacements is None:
        replacements = {}

    config_new = deepcopy(config)
    for dimension in config_new["dimensions"]:
        _improve_dimension_names(dimension, transformation=transformation, replacements=replacements)

    return config_new


def set_default_view(config: dict[str, Any], default_view: dict[str, str]):
    config_new = deepcopy(config)
    error = "Default view not found"
    assert sum([default_view == view["dimensions"] for view in config_new["views"]]) == 1, error
    for view in config_new["views"]:
        if view["dimensions"] == default_view:
            view["default_view"] = True

    return config_new


def replace_empty_dimension_slugs(config: dict[str, Any]) -> dict[str, Any]:
    """Replace empty dimension slugs, which explorers accept but MDIM schema rejects."""
    config_new = deepcopy(config)
    for dimension in config_new["dimensions"]:
        for choice in dimension["choices"]:
            if choice["slug"] == EMPTY_DIMENSION_LABEL:
                choice["slug"] = IRRELEVANT_DIMENSION_SLUG

    for view in config_new["views"]:
        for dimension, choice in view["dimensions"].items():
            if choice == EMPTY_DIMENSION_LABEL:
                view["dimensions"][dimension] = IRRELEVANT_DIMENSION_SLUG

    return config_new


def build_config(config: dict[str, Any], tb) -> dict[str, Any]:
    """Build an MDIM config that mirrors the existing animal welfare explorer."""
    config_new = expand_config(
        tb,
        indicator_names=sorted(
            set([column.split("__")[0] for column in tb.columns if column not in tb.metadata.primary_key])
        ),
        dimensions={
            "metric": ["animals_killed", "animals_alive"],
            "animal": sorted(
                set(
                    [
                        tb[column].metadata.dimensions["animal"]
                        for column in tb.columns
                        if column not in tb.metadata.primary_key
                    ]
                )
            ),
            "estimate": [EMPTY_DIMENSION_LABEL, "midpoint", "lower limit", "upper limit"],
            "per_capita": ["False", "True"],
        },
        indicator_as_dimension=True,
    )

    # `indicator_as_dimension=True` is needed to expand both value columns, but
    # the explorer exposed the indicator choice through `metric`, not separately.
    for view in config_new["views"]:
        view["dimensions"].pop("indicator", None)
    config_new["dimensions"] = [dimension for dimension in config_new["dimensions"] if dimension["slug"] != "indicator"]

    config["dimensions"] = config_new["dimensions"]
    config["views"] = config_new["views"]
    config = improve_config_names(
        config,
        replacements={
            "animals_killed": "Animals slaughtered",
            "animals_alive": "Live animals",
        },
    )

    for dimension in config["dimensions"]:
        if dimension["slug"] == "per_capita":
            dimension["presentation"] = {"type": "checkbox", "choice_slug_true": "True"}

    for dimension in config["dimensions"]:
        if dimension["slug"] == "metric":
            dimension["choices"].extend(
                [
                    {"slug": "fur_farming_status", "name": "Bans on fur farming", "description": None},
                    {"slug": "fur_trading_status", "name": "Bans on fur trading", "description": None},
                    {"slug": "bullfighting_status", "name": "Bans on bullfighting", "description": None},
                    {"slug": "chick_culling_status", "name": "Bans on chick culling", "description": None},
                    {"slug": "caged_hens", "name": "Cage and cage-free hens", "description": None},
                ]
            )
        elif dimension["slug"] == "animal":
            dimension["choices"].append(
                {"slug": IRRELEVANT_DIMENSION_SLUG, "name": EMPTY_DIMENSION_LABEL, "description": None}
            )

    config["views"].extend(
        [
            {
                "dimensions": {
                    "metric": "fur_farming_status",
                    "animal": IRRELEVANT_DIMENSION_SLUG,
                    "per_capita": "False",
                    "estimate": IRRELEVANT_DIMENSION_SLUG,
                },
                "indicators": {"y": ["fur_laws#fur_farming_status"]},
                "config": {
                    "title": "Which countries have banned fur farming?",
                    "subtitle": "Countries that have banned fur farming at a national level.",
                    "tab": "map",
                    "hasMapTab": True,
                    "chartTypes": [],
                    "map": {
                        "colorScale": {
                            "baseColorScheme": "OwidCategoricalC",
                            "customCategoryColors": {
                                "Banned": "#759AC8",
                                "Banned but not yet in effect": "#058580",
                                "No active farms reported": "#99D8C9",
                                "Not banned": "#AE2E3F",
                                "Partially banned": "#A46F49",
                                "Phased out due to stricter regulation": "#6D3E91",
                            },
                        }
                    },
                },
            },
            {
                "dimensions": {
                    "metric": "fur_trading_status",
                    "animal": IRRELEVANT_DIMENSION_SLUG,
                    "per_capita": "False",
                    "estimate": IRRELEVANT_DIMENSION_SLUG,
                },
                "indicators": {"y": ["fur_laws#fur_trading_status"]},
                "config": {
                    "title": "Which countries have banned fur trading?",
                    "subtitle": "Countries that have banned fur trading at a national level.",
                    "tab": "map",
                    "hasMapTab": True,
                    "chartTypes": [],
                    "map": {
                        "colorScale": {
                            "baseColorScheme": "OwidCategoricalC",
                            "customCategoryColors": {
                                "Banned": "#759AC8",
                                "Not banned": "#AE2E3F",
                                "Partially banned": "#A46F49",
                            },
                        }
                    },
                },
            },
            {
                "dimensions": {
                    "metric": "bullfighting_status",
                    "animal": IRRELEVANT_DIMENSION_SLUG,
                    "per_capita": "False",
                    "estimate": IRRELEVANT_DIMENSION_SLUG,
                },
                "indicators": {"y": ["bullfighting_laws#status"]},
                "config": {
                    "title": "Which countries have banned bullfighting?",
                    "subtitle": "Bullfighting is a physical contest that involves a bullfighter attempting to subdue, immobilize, or kill a bull.",
                    "tab": "map",
                    "hasMapTab": True,
                    "chartTypes": [],
                    "map": {
                        "colorScale": {
                            "baseColorScheme": "OwidCategoricalC",
                            "customCategoryColors": {
                                "Banned": "#759AC8",
                                "Not banned": "#AE2E3F",
                                "Partially banned": "#A46F49",
                            },
                        }
                    },
                },
            },
            {
                "dimensions": {
                    "metric": "chick_culling_status",
                    "animal": IRRELEVANT_DIMENSION_SLUG,
                    "per_capita": "False",
                    "estimate": IRRELEVANT_DIMENSION_SLUG,
                },
                "indicators": {"y": ["chick_culling_laws#status"]},
                "config": {
                    "title": "Which countries have banned chick culling?",
                    "subtitle": "Chick culling is the process of separating and killing unwanted male and unhealthy female chicks that cannot produce eggs in industrialized egg facilities.",
                    "tab": "map",
                    "hasMapTab": True,
                    "chartTypes": [],
                    "map": {
                        "colorScale": {
                            "baseColorScheme": "OwidCategoricalC",
                            "customCategoryColors": {
                                "Banned": "#759AC8",
                                "Banned but not yet in effect": "#058580",
                                "Not banned": "#AE2E3F",
                                "Partially banned": "#A46F49",
                            },
                        }
                    },
                },
            },
            {
                "dimensions": {
                    "metric": "caged_hens",
                    "animal": IRRELEVANT_DIMENSION_SLUG,
                    "per_capita": "False",
                    "estimate": IRRELEVANT_DIMENSION_SLUG,
                },
                "indicators": {
                    "y": [
                        {
                            "catalogPath": "global_hen_inventory#number_of_hens_in_cages",
                            "display": {
                                "tolerance": 10,
                            },
                        },
                        {
                            "catalogPath": "global_hen_inventory#number_of_hens_cage_free",
                            "display": {
                                "tolerance": 10,
                            },
                        },
                    ]
                },
                "config": {
                    "title": "Number of laying hens in cages and cage-free housing",
                    "hasMapTab": False,
                    "chartTypes": ["StackedDiscreteBar"],
                },
            },
        ]
    )

    config = replace_empty_dimension_slugs(config)

    return set_default_view(
        config=config,
        default_view={
            "metric": "animals_killed",
            "animal": "all land animals",
            "estimate": IRRELEVANT_DIMENSION_SLUG,
            "per_capita": "False",
        },
    )


def run() -> None:
    ds = paths.load_dataset("animals_used_for_food")
    tb = ds.read("animals_used_for_food")

    config = build_config(config=paths.load_collection_config(), tb=tb)

    collection = paths.create_collection(config=config)
    collection.save(tolerate_extra_indicators=True)
