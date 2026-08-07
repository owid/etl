"""Grapher step for the fossil fuels dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data.
    #
    ds_garden = paths.load_dataset("fossil_fuels")
    tb = ds_garden.read("fossil_fuels", reset_index=False)

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()
