"""Load grapher datasets and create an explorer tsv file."""

from etl.collections.explorer import expand_config
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
    ds = paths.load_dataset("animals_used_for_food")
    tb = ds.read("animals_used_for_food")

    # Load grapher config from YAML.
    config = paths.load_explorer_config()

    config_new = expand_config(
        tb,
        indicator_names=["n_animals_killed", "n_animals_alive"],
        indicators_slug="metric",
        dimensions=["animal", "per_capita"],
        indicator_as_dimension=True,
    )
    # TODO: expand_config could ingest 'config' as well, and extend dimensions and views in it (in case there were already some in the yaml).
    config["dimensions"] = config_new["dimensions"]
    config["views"] = config_new["views"]
    # TODO: this could also happen inside expand_config.
    config = improve_config_names(
        config,
        replacements={
            "n_animals_killed": "Animals slaughtered for meat",
            "n_animals_alive": "Live animals used for meat",
        },
    )

    # Make per capita a checkbox.
    # TODO: This could be part of expand_config.
    for dimension in config["dimensions"]:
        if dimension["slug"] == "per_capita":
            dimension["presentation"] = {"type": "checkbox", "choice_slug_true": "True"}

    # TODO: Is there any way to sort the elements of the dropdowns?

    #
    # Save outputs.
    #
    # Initialize a new explorer.
    ds_explorer = paths.create_explorer(config=config, explorer_name="animal-welfare")

    # Save explorer.
    ds_explorer.save(tolerate_extra_indicators=True)
