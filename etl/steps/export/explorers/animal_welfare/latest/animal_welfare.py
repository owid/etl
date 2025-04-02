"""Load grapher datasets and create an explorer tsv file."""

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

    config_new = config.copy()
    for dimension in config_new["dimensions"]:
        _improve_dimension_names(dimension, transformation=transformation, replacements=replacements)

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

    config_new = expand_config(
        tb,
        indicator_names=["n_animals_killed", "n_animals_alive"],
        indicators_slug="metric",
        dimensions=["animal", "estimate", "per_capita"],
        indicator_as_dimension=True,
    )
    # TODO: expand_config could ingest 'config' as well, and extend dimensions and views in it (in case there were already some in the yaml).
    config["dimensions"] = config_new["dimensions"]
    config["views"] = config_new["views"]
    # TODO: this could also happen inside expand_config.
    config = improve_config_names(
        config,
        replacements={
            "n_animals_killed": "Animals killed to produce food",
            "n_animals_alive": "Animals alive to produce food",
        },
    )

    # Make per capita a checkbox.
    # TODO: This could be part of expand_config.
    for dimension in config["dimensions"]:
        if dimension["slug"] == "per_capita":
            dimension["presentation"] = {"type": "checkbox", "choice_slug_true": "True"}

    # TODO: Is there any way to sort the elements of the dropdowns? Currently, I achieve this by sorting the corresponding data from garden, but there should be an easy way to sort dimensions from here.
    # TODO: Set the defalt view (by adding "config": {"defaultView": True} in the corresponding view, which should be animals killed for meat total).
    # Include additional views.
    # TODO: Create function add views. If dimension is not specified, create an empty choice for each unspecified dimension in "dimensions".
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
    # TODO: Instead of adding views like this, they could be defined in the yaml, and then the function to expand views would append those.
    # TODO: Why are title and subtitle not automatically fetched for indicators?
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
    #
    # Save outputs.
    #
    # Initialize a new explorer.
    ds_explorer = paths.create_explorer(config=config, explorer_name="animal-welfare")

    # Save explorer.
    ds_explorer.save(tolerate_extra_indicators=True)
