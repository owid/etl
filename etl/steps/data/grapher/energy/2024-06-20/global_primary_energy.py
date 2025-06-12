"""Grapher step for the global primary energy dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("global_primary_energy")
    tb_garden = ds_garden.read("global_primary_energy", reset_index=False)

    #
    # Process data.
    #
    # Drop unnecessary columns from table.
    tb = tb_garden.drop(columns=["data_source"], errors="raise")

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()
