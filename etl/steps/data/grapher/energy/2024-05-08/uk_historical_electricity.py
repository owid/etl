"""Grapher step for the UK historical electricity dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("uk_historical_electricity")
    tb_garden = ds_garden["uk_historical_electricity"]

    #
    # Prepare data.
    #
    ####################################################################################################################
    # Temporary solution: Fill forward other renewables, which is the only missing indicator in 2023, and makes stacked
    # area chart show no data for 2023.
    import pandas as pd

    error = "Other renewables is no longer missing in 2023. Remove this temporary solution."
    assert pd.isna(tb_garden.loc["United Kingdom", 2023]["other_renewables_generation"]), error
    tb_garden["other_renewables_generation"] = tb_garden["other_renewables_generation"].ffill()
    ####################################################################################################################

    #
    # Save outputs.
    #
    ds_grapher = create_dataset(dest_dir=dest_dir, tables=[tb_garden], check_variables_metadata=True)
    ds_grapher.save()
