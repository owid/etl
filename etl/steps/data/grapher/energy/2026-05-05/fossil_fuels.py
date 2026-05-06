"""Grapher step for the OWID Fossil Fuels dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("fossil_fuels")
    tb = ds_garden.read("fossil_fuels", reset_index=False)

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()
