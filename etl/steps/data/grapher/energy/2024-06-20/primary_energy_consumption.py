"""Grapher step for the primary energy consumption dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("primary_energy_consumption")
    tb_garden = ds_garden.read("primary_energy_consumption")

    #
    # Process data.
    #
    # Remove unnecessary columns.
    tb = tb_garden.drop(columns=["gdp", "population", "source"], errors="raise")

    # Format table conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()
