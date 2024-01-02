"""Load a garden dataset and create a grapher dataset."""

import numpy as np
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Combine decadal and yearly data from this specific year onwards.
DECADE_TO_YEAR = 2010


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("surface_temperature_analysis")

    # Read table from garden dataset.
    tb = ds_garden["surface_temperature_analysis"].reset_index()

    #
    # Process data.
    #
    # Create a custom table that allows us to compare the historical temperature anomaly for each month.
    tb_plot = create_monthly_comparison_plot(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb_plot = tb_plot.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_plot], check_variables_metadata=True)
    ds_grapher.save()


def create_monthly_comparison_plot(tb: Table) -> Table:
    tb = tb.copy()

    # Select data for the World and drop location column.
    tb = tb[tb["location"] == "World"].reset_index(drop=True)
    tb = tb.drop(columns=["location"])

    # Create a column for year and month.
    tb["year"] = tb["date"].astype(str).str.split("-").str[0].astype(int)
    tb["month"] = tb["date"].astype(str).str.split("-").str[1].astype(int)

    # Find the latest year in the data.
    latest_year = tb["year"].max()

    # Sort by year and month.
    tb = tb.sort_values(["year", "month"]).reset_index(drop=True)

    # Count the number of months per year and assert that there are 12 months per year (except for the current one).
    tb["month_count"] = tb.groupby("year")["month"].transform("count")
    assert tb[tb["year"] != latest_year]["month_count"].unique() == 12
    # Remove the month count column.
    tb = tb.drop(columns=["month_count"])

    # Instead of computing the mean value for each decade, simply get the value for the zeroth year of each decade.
    # Select rows that correspond to the zeroth year of each decade, until DECADE_TO_YEAR.
    tb_plot = tb[(tb["year"] % 10 == 0) | (tb["year"] > DECADE_TO_YEAR)].reset_index(drop=True)

    # Adapt table to grapher format.
    # Instead of having a column per year (or decade), create a "country" column that identifies each year (or decade).
    tb_plot = tb_plot.rename(columns={"year": "country"}, errors="raise").astype(str)
    tb_plot["country"] = "Year " + tb_plot["country"]
    # The column "year" needs to be the number of days since a specific date.
    # Create an array of the cumulative number of days in each month (30 days per month, for simplicity).
    tb_plot["year"] = tb["month"].replace(
        {month + 1: n_days for month, n_days in enumerate(np.cumsum(np.repeat(30, 12)))}
    )

    # Remove unnecessary columns.
    tb_plot = tb_plot.drop(columns=["month", "date"])

    # The new chart should start on the first day of the current year.
    tb_plot["temperature_anomaly"].metadata.display = {
        "yearIsDay": True,
        "zeroDay": f"{latest_year}-01-01",
    }

    return tb_plot
