"""Grapher step for the UK historical electricity dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("uk_historical_electricity")
    tb_garden = ds_garden.read("uk_historical_electricity", reset_index=False)

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb_garden])
    ds_grapher.save()
