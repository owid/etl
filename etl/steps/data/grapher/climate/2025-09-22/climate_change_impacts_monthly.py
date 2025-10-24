"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr

from etl.grapher.helpers import adapt_table_with_dates_to_grapher
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its monthly table.
    ds_garden = paths.load_dataset("climate_change_impacts")
    tb = ds_garden.read("climate_change_impacts_monthly")

    #
    # Process data.
    #
    # Create a country column (required by grapher).
    tb = tb.rename(columns={"location": "country"}, errors="raise")

    # Adapt table with dates to grapher requirements.
    tb = adapt_table_with_dates_to_grapher(tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"])

    # Create a pivoted table for sea temperature anomaly with years as columns
    # Load original data before date adaptation
    tb_original = ds_garden.read("climate_change_impacts_monthly").reset_index()
    tb_original = tb_original.rename(columns={"location": "country"}, errors="raise")

    # Check if sea_temperature_anomaly column exists
    if "sea_temperature_anomaly" in tb_original.columns:
        # Extract year and month from date column
        tb_original["year"] = tb_original["date"].astype(str).str[0:4]
        tb_original["month"] = tb_original["date"].astype(str).str[5:7]

        # Keep only necessary columns and aggregate by country, year, and month (take mean for duplicates)
        tb_pivot_data = tb_original[["country", "year", "month", "sea_temperature_anomaly"]].copy()
        tb_pivot_data = tb_pivot_data.groupby(["country", "year", "month"], as_index=False)[
            "sea_temperature_anomaly"
        ].mean()

        # Pivot to have years as columns
        tb_pivot = tb_pivot_data.pivot(
            index=["country", "month"], columns="year", values="sea_temperature_anomaly", join_column_levels_with="_"
        )

        # Rename month to year for grapher compatibility
        tb_pivot = tb_pivot.rename(columns={"month": "year"})
        tb_pivot = tb_pivot.set_index(["country", "year"])

        # Convert all column names to strings
        tb_pivot.columns = [str(col) for col in tb_pivot.columns]

        # Set metadata for each column
        for column in tb_pivot.columns:
            tb_pivot[column].metadata.title = column

        tb_pivot.metadata = tb_original.metadata
        tb_pivot.metadata.short_name = "climate_change_impacts_monthly_sea_temperature"

        # Save both tables
        ds_grapher = paths.create_dataset(tables=[tb, tb_pivot], check_variables_metadata=True)
    else:
        # If the column doesn't exist, save only the original table
        ds_grapher = paths.create_dataset(tables=[tb])

    ds_grapher.save()
