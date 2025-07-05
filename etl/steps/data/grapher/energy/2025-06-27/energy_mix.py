"""Grapher step for BP's energy mix dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("energy_mix")
    tb_garden = ds_garden.read("energy_mix", reset_index=False)

    #
    # Save outputs.
    #
    # Create new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb_garden])
    ds_grapher.save()
