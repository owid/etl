"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_grapher = paths.load_dataset("wildfires_by_week")
    tb = ds_grapher["weekly_wildfires"].reset_index()

    #
    # Process data.
    #
    # Extract columns related to cumulative area burned and emissions
    column_groups = ["area_ha_cumulative", "share_area_ha_cumulative", "co2_cumulative", "pm2_5_cumulative"]
    # Filter the Table columns to include only those that match any of the indicators in the list
    columns = [col for col in tb.columns if any(group in col for group in column_groups)] + ["country", "year"]

    # Create a new Table with only the filtered columns
    tb = tb[columns]

    # Loop through each group of indicators and apply the same calculations
    for group in column_groups:
        # Select columns that contain the group name and a year between 2003 (minimum value in emisssions; 2012 in area burned) and 2023 (inclusive)
        group_columns = [col for col in tb.columns if group in col and "2003" <= col.split("_")[1] <= "2023"]

        # Sort the group columns by year
        group_columns_sorted = sorted(group_columns, key=lambda x: int(x.split("_")[1]))

        # Calculate the average until 2024
        tb[f"{group}_until_2024"] = tb[group_columns_sorted].sum(axis=1)
        tb[f"avg_{group}_until_2024"] = tb[f"{group}_until_2024"] / len(group_columns_sorted)

        # Identify the rows where the year is 52
        year_52_rows = tb[tb["year"] == 52]

        # Use the maximum value at year 52 as the upper bound for each country
        tb[f"upper_bound_{group}"] = year_52_rows[group_columns_sorted].max(axis=0)

        # Find the minimum value for the group as before
        tb[f"lower_bound_{group}"] = year_52_rows[group_columns_sorted].min(axis=1)

        # Drop original columns as they are used in a different dataset and not needed here
        tb = tb.drop(columns=[f"{group}_until_2024"] + group_columns)

        # Dynamically set origins based on the group
        origin_column = f"_2024_{group}"  # Dynamically set based on group name
        if origin_column in tb.columns:  # Check if the origin column exists
            for col in [f"avg_{group}_until_2024", f"upper_bound_{group}", f"lower_bound_{group}"]:
                tb[col].origins = tb[origin_column].origins

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_grapher.metadata)
    ds_grapher.save()
