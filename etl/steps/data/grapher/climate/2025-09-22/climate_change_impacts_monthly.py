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

        # Calculate decadal averages
        tb_long = tb_pivot.melt(id_vars=["month", "country"], var_name="year", value_name="value")
        tb_long["year"] = tb_long["year"].astype(int)
        tb_long["decade"] = (tb_long["year"] // 10) * 10

        # Group by month, country, and decade
        decadal_averages = tb_long.groupby(["month", "country", "decade"])["value"].mean().reset_index()
        decadal_averages["decade"] = decadal_averages["decade"].astype(str) + "s"

        # Pivot decades into columns
        pivoted_decadal = decadal_averages.pivot(
            index=["month", "country"], columns="decade", values="value"
        ).reset_index()

        # Merge decadal and yearly data
        tb_merged = pr.merge(pivoted_decadal, tb_pivot, on=["month", "country"], how="outer")

        # Rename month to year for grapher compatibility
        tb_merged = tb_merged.rename(columns={"month": "year"})
        tb_merged = tb_merged.set_index(["country", "year"])

        # Convert all column names to strings
        tb_merged.columns = [str(col) for col in tb_merged.columns]

        # Set metadata for each column
        for column in tb_merged.columns:
            tb_merged[column].metadata.title = column

        tb_merged.metadata = tb_original.metadata
        tb_merged.metadata.short_name = "climate_change_impacts_monthly_sea_temperature"

        # Save both tables
        ds_grapher = paths.create_dataset(tables=[tb, tb_merged], check_variables_metadata=True)
    else:
        # If the column doesn't exist, save only the original table
        ds_grapher = paths.create_dataset(tables=[tb])

    ds_grapher.save()
