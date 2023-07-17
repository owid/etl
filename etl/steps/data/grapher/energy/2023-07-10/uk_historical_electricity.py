"""Grapher step for the UK historical electricity dataset.
"""
from copy import deepcopy

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden: Dataset = paths.load_dependency("uk_historical_electricity")
    tb_garden = ds_garden["uk_historical_electricity"]

    # Create variable filling missing values with zeros (to allow visualization of stacked area charts in grapher).
    for column in tb_garden.columns:
        new_column = f"{column}_zero_filled"
        tb_garden[new_column] = tb_garden[column].fillna(0)
        tb_garden[new_column].metadata = deepcopy(tb_garden[column].metadata)
        tb_garden[new_column].metadata.title = tb_garden[column].metadata.title + " (zero filled)"

    #
    # Save outputs.
    #
    ds_grapher = create_dataset(
        dest_dir=dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    ds_grapher.save()
