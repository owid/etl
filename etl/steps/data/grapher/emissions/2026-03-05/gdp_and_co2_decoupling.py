"""Detect countries that have decoupled per capita GDP growth from per capita consumption-based CO2 emissions."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load Global Carbon Budget dataset and read its main table.
    ds_garden = paths.load_dataset("gdp_and_co2_decoupling")
    tb_garden = ds_garden.read("gdp_and_co2_decoupling", reset_index=False)

    #
    # Save outputs.
    #
    ds = paths.create_dataset(tables=[tb_garden])
    ds.save()
