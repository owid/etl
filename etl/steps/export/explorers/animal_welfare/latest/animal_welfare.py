"""Load grapher datasets and create an explorer tsv file."""

from etl.collections.explorer import expand_config
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
        indicator_names=["n_animals_killed"],
        indicators_slug="metric",
        dimensions=["animal"],
        indicator_as_dimension=True,
    )
    config["dimensions"] = config_new["dimensions"]
    config["views"] = config_new["views"]

    #
    # Save outputs.
    #
    # Initialize a new explorer.
    ds_explorer = paths.create_explorer(config=config, explorer_name="animal-welfare")

    # Save explorer.
    ds_explorer.save(tolerate_extra_indicators=True)
