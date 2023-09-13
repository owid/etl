"""Grapher step for the Electricity Mix (BP & Ember) dataset.
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
    ds_garden: Dataset = paths.load_dependency("electricity_mix")
    tb_garden = ds_garden["electricity_mix"].reset_index()

    #
    # Process data.
    #
    # Drop unnecessary columns.
    tb_garden = tb_garden.drop(columns=["population"])

    # Add zero-filled variables (where missing points are filled with zeros) to avoid stacked area charts
    # showing incomplete data.
    generation_columns = [c for c in tb_garden.columns if "generation__twh" in c]
    for column in generation_columns:
        new_column = f"{column}_zero_filled"
        tb_garden[new_column] = tb_garden[column].fillna(0)
        tb_garden[new_column].metadata = deepcopy(tb_garden[column].metadata)
        tb_garden[new_column].metadata.title += " (zero filled)"

    # Set an appropriate index and sort conveniently.
    tb_garden = tb_garden.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    ds_grapher = create_dataset(dest_dir=dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata)
    ds_grapher.save()
