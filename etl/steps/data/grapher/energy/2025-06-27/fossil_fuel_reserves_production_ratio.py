"""Grapher step for the fossil fuel reserves-to-production ratio dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("fossil_fuel_reserves_production_ratio")
    tb_garden = ds_garden.read("fossil_fuel_reserves_production_ratio", reset_index=False)

    # Create new grapher dataset.
    dataset = paths.create_dataset(tables=[tb_garden])
    dataset.save()
