"""Load a meadow dataset and create a garden dataset."""

import numpy as np

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("brassley_2000")
    tb = ds_meadow["brassley_2000"].reset_index()

    #
    # Process data.
    #
    # Years are given in intervals; take the average year of each interval.
    tb["year"] = [np.array(year.split("-")).astype(int).mean().astype(int) for year in tb["year"]]

    # Rename columns.
    tb = tb.rename(columns={column: column + "_yield" for column in tb.columns if column != "year"}, errors="raise")

    # Add a country column.
    tb["country"] = "United Kingdom"

    # Set an appropriate index and sort conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
