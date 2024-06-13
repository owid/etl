"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the data, and how to rename them.
COLUMNS = {
    "date": "date",
    "phcalc_insitu": "ocean_ph",
}


def add_rolling_average(tb: Table) -> Table:
    tb_with_average = tb.copy()

    # Set date as index and sort.
    tb_with_average = tb_with_average.set_index("date").sort_index()

    # Since values are given at different days of the month, reindex to have a value for each day.
    tb_with_average = tb_with_average.reindex(
        pd.date_range(start=tb_with_average.index.min(), end=tb_with_average.index.max(), freq="1D")
    )

    # Create a rolling average with a window of one year, linearly interpolating missing values.
    tb_with_average["ocean_ph_yearly_average"] = (
        tb_with_average["ocean_ph"]
        .interpolate(method="time")
        .rolling(365)
        .mean()
        .copy_metadata(tb_with_average["ocean_ph"])
    )

    # Drop empty rows.
    tb_with_average = (
        tb_with_average.dropna(subset=["ocean_ph"]).reset_index().rename(columns={"index": "date"}, errors="raise")
    )

    # Check that the values of the original ocean ph column have not been altered.
    error = "The values of the original ocean_ph column have been altered."
    assert tb_with_average["ocean_ph"].equals(
        tb.dropna(subset=["ocean_ph"]).sort_values("date").reset_index(drop=True)["ocean_ph"]
    ), error

    return tb_with_average


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables.
    ds_meadow = paths.load_dataset("hawaii_ocean_time_series")
    tb_meadow = ds_meadow["hawaii_ocean_time_series"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb_meadow[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Add location column.
    tb["location"] = "Hawaii"

    # Improve format of date column.
    tb["date"] = pd.to_datetime(tb["date"], format="%d-%b-%y")

    # Add a column with a rolling average.
    tb = add_rolling_average(tb=tb)

    # Set an appropriate index to each table and sort conveniently.
    tb = tb.set_index(["location", "date"], verify_integrity=True).sort_index()

    # Rename table.
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
