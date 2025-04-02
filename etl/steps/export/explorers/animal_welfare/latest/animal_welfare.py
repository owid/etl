"""Load grapher datasets and create an explorer tsv file."""

from copy import deepcopy

from etl.collections.explorer import expand_config
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Label to assign to the list choices of a dimension when that dimension is irrelevant.
# This is what the dropdown will show, greyed out.
EMPTY_DIMENSION_LABEL = ""


def _improve_dimension_names(dimension, transformation, replacements):
    for field, value in dimension.items():
        if field == "name":
            if value in replacements:
                dimension["name"] = replacements[value]
            else:
                dimension["name"] = transformation(value)
        if field == "choices":
            for choice in value:
                _improve_dimension_names(choice, transformation=transformation, replacements=replacements)


def improve_config_names(config, transformation=None, replacements=None):
    """Create human-readable names out of slugs."""
    if transformation is None:

        def transformation(slug):
            return slug.replace("_", " ").capitalize()

    if replacements is None:
        replacements = dict()

    config_new = deepcopy(config)
    for dimension in config_new["dimensions"]:
        _improve_dimension_names(dimension, transformation=transformation, replacements=replacements)

    return config_new


def set_default_view(config, default_view):
    config_new = deepcopy(config)
    error = "Default view not found"
    assert sum([default_view == view["dimensions"] for view in config_new["views"]]) == 1, error
    for view in config_new["views"]:
        if view["dimensions"] == default_view:
            view["default_view"] = True

    return config_new


def run() -> None:
    #
    # Load inputs.
    #
    # Load dataset of animals used for food, and read its main table.
    ds = paths.load_dataset("animals_used_for_food")
    tb = ds.read("animals_used_for_food")

    # Load grapher config from YAML.
    config = paths.load_explorer_config()

    #
    # Process data.
    #
    # Create additional dimensions and views from input table.
    config_new = expand_config(
        tb,
        indicator_names=["n_animals_killed", "n_animals_alive"],
        indicators_slug="metric",
        dimensions={
            # NOTE: Here it is convenient that total meat is the first choice. If that changes, manually change the list below.
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
        # TODO: The following changes the coloring of the chart tab. How do I change the color of the map tab?
        # common_view_config={"baseColorScheme": "YlOrBr"},
    )
    
    # Update original configuration of dimensions and views.
    config["dimensions"] = config_new["dimensions"]
    config["views"] = config_new["views"]
    config = improve_config_names(
        config,
        replacements={
            "n_animals_killed": "Animals killed to produce food",
            "n_animals_alive": "Animals alive to produce food",
        },
    )

    # Make per capita a checkbox.
    for dimension in config["dimensions"]:
        if dimension["slug"] == "per_capita":
            dimension["presentation"] = {"type": "checkbox", "choice_slug_true": "True"}

    # Include additional views.
    # NOTE: First we need to create additional dimensions.
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
                {"slug": EMPTY_DIMENSION_LABEL, "name": EMPTY_DIMENSION_LABEL, "description": None}
            )

    # Add view with map chart for fur banning laws.
    config["views"].append(
        {
            "dimensions": {
                "metric": "fur_farming_status",
                "animal": EMPTY_DIMENSION_LABEL,
                "per_capita": "False",
                "estimate": EMPTY_DIMENSION_LABEL,
            },
            "indicators": {
                "y": [
                    {
                        "catalogPath": "fur_laws#fur_farming_status",
                        "display": {
                            "colorScaleScheme": "OwidCategoricalC",
                            "colorScaleCategoricalBins": "Banned,#759AC8,;Banned but not yet in effect,#058580,;No active farms reported,#99D8C9,;Not banned,#AE2E3F,;Partially banned,#A46F49,;Phased out due to stricter regulation,#6D3E91,;No data,,",
                        },
                    }
                ]
            },
            "config": {
                "title": "Which countries have banned fur farming?",
                # TODO: Why are title and subtitle not automatically fetched for indicators?
                "subtitle": "Countries that have banned fur farming at a national level.",
                "hasMapTab": True,
                # TODO: I suppose the following should be None, instead of "None", but the former doesn't work.
                "type": "None",
            },
        }
    )

    # Add view with map chart for fur trading laws.
    config["views"].append(
        {
            "dimensions": {
                "metric": "fur_trading_status",
                "animal": EMPTY_DIMENSION_LABEL,
                "per_capita": "False",
                "estimate": EMPTY_DIMENSION_LABEL,
            },
            "indicators": {
                "y": [
                    {
                        "catalogPath": "fur_laws#fur_trading_status",
                        "display": {
                            "colorScaleScheme": "OwidCategoricalC",
                            "colorScaleCategoricalBins": "Banned,#759AC8,;Not banned,#AE2E3F,;Partially banned,#A46F49,",
                        },
                    }
                ]
            },
            "config": {
                "title": "Which countries have banned fur trading?",
                "subtitle": "Countries that have banned fur trading at a national level.",
                "hasMapTab": True,
                "type": "None",
            },
        }
    )

    # Add view with map chart for bullfighting laws.
    config["views"].append(
        {
            "dimensions": {
                "metric": "bullfighting_status",
                "animal": EMPTY_DIMENSION_LABEL,
                "per_capita": "False",
                "estimate": EMPTY_DIMENSION_LABEL,
            },
            "indicators": {
                "y": [
                    {
                        "catalogPath": "bullfighting_laws#status",
                        "display": {
                            "colorScaleScheme": "OwidCategoricalC",
                            "colorScaleCategoricalBins": "Banned,#759AC8,;Not banned,#AE2E3F,;Partially banned,#A46F49,;No data,,",
                        },
                    }
                ]
            },
            "config": {
                "title": "Which countries have banned bullfighting?",
                "subtitle": "Bullfighting is a physical contest that involves a bullfighter attempting to subdue, immobilize, or kill a bull.",
                "hasMapTab": True,
                "type": "None",
            },
        }
    )

    # Add view with map chart for chick culling laws.
    config["views"].append(
        {
            "dimensions": {
                "metric": "chick_culling_status",
                "animal": EMPTY_DIMENSION_LABEL,
                "per_capita": "False",
                "estimate": EMPTY_DIMENSION_LABEL,
            },
            "indicators": {
                "y": [
                    {
                        "catalogPath": "chick_culling_laws#status",
                        "display": {
                            "colorScaleScheme": "OwidCategoricalC",
                            "colorScaleCategoricalBins": "Banned,#759AC8,;Banned but not yet in effect,#058580,;Not banned,#AE2E3F,;Partially banned,#A46F49,;No data,,",
                        },
                    }
                ]
            },
            "config": {
                "title": "Which countries have banned chick culling?",
                "subtitle": "Chick culling is the process of separating and killing unwanted male and unhealthy female chicks that cannot produce eggs in industrialized egg facilities.",
                "hasMapTab": True,
                "type": "None",
            },
        }
    )

    # Add view with bar chart on cage-free hens.
    config["views"].append(
        {
            "dimensions": {
                "metric": "caged_hens",
                "animal": EMPTY_DIMENSION_LABEL,
                "per_capita": "False",
                "estimate": EMPTY_DIMENSION_LABEL,
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
                "type": "StackedDiscreteBar",
                # TODO: How can I add the Settings button, to allow for relative?
                # "stackMode": "absolute",
            },
        }
    )

    # Set the defalt view.
    config_new = set_default_view(
        config=config,
        default_view={
            "metric": "n_animals_killed",
            "animal": "all land animals",
            "estimate": "",
            "per_capita": "False",
        },
    )

    #
    # Save outputs.
    #
    # Initialize a new explorer.
    ds_explorer = paths.create_explorer(config=config, explorer_name="animal-welfare")

    # Save explorer.
    ds_explorer.save(tolerate_extra_indicators=True)
