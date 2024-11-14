"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("surface_temperature")
    tb = ds_garden["surface_temperature"].reset_index()
    tb["year"] = tb["time"].astype(str).str[0:4]
    tb["month"] = tb["time"].astype(str).str[5:7]
    origin = tb["temperature_2m"].metadata.origins

    #
    # Process data.
    #
    tb = tb.drop(columns=["time"], errors="raise")

    # Transpose the DataFrame to have a country column, a column identifying the measure, and year columns
    tb = tb[["country", "year", "month", "temperature_anomaly"]].pivot(
        index=["country", "month"], columns="year", values="temperature_anomaly", join_column_levels_with="_"
    )

    # Select columns that contain the group name and a year between 2003 (minimum value in emisssions; 2012 in area burned) and 2023 (inclusive)
    group_columns = [col for col in tb.columns if "2003" <= col <= "2023"]

    tb = tb[group_columns + ["country", "month"]]
    tb["month"] = tb["month"].astype(int)

    # Sort the group columns by year
    group_columns_sorted = sorted(group_columns)

    # Process data for each country
    for country in tb["country"].unique():
        country_rows = tb[tb["country"] == country]

        # Select rows with year 52 (actually the last week of the year)
        country_row_12 = country_rows[country_rows["month"] == 12]

        if country_row_12.empty or country_row_12[group_columns_sorted].isnull().all(axis=1).all():
            continue

        # Find the column with the maximum value at year 52 (actually the last week of the year)
        max_col = country_row_12[group_columns_sorted].idxmax(axis=1).iloc[0]

        # Find the column with the minimum value at year 52 (actually the last week of the year)
        min_col = country_row_12[group_columns_sorted].idxmin(axis=1).iloc[0]

        # Set upper and lower bounds for all rows of this country using the columns identified at year 52
        tb.loc[tb["country"] == country, "upper_bound_anomaly"] = country_rows[max_col]
        tb.loc[tb["country"] == country, "lower_bound_anomaly"] = country_rows[min_col]

    # Drop original columns as they are used in a different dataset and not needed here
    tb = tb.drop(columns=group_columns)

    # Dynamically set origins based on the group

    for col in ["upper_bound_anomaly", "lower_bound_anomaly"]:
        tb[col].origins = origin

    tb = tb.rename(columns={"month": "year"})
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.metadata.title = "Monthly surface temperature anomalies by country"
    ds_grapher.save()
