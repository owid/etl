"""Load a garden dataset and create a grapher dataset."""

import numpy as np

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
    # Extract columns related to area and events
    column_groups = [
        "area_ha_cumulative",
        "share_area_ha_cumulative",
        "co2_cumulative",
        "pm2_5_cumulative",
        "area_ha",
        "events",
        "share_area_ha",
        "co2",
        "pm2_5",
    ]
    # Loop through each group and apply the same calculations
    for group in column_groups:
        # Select columns that contain the group name and a year between 2003 and 2023 (inclusive)
        group_columns = [col for col in tb.columns if group in col and "2003" <= col.split("_")[1] <= "2023"]

        group_columns_sorted = sorted(group_columns, key=lambda x: int(x.split("_")[1]))

        tb[f"{group}_until_2024"] = tb[group_columns_sorted].sum(axis=1)

        tb[f"avg_{group}_until_2024"] = tb[f"{group}_until_2024"] / len(group_columns_sorted)

        # Calculate the standard deviation
        tb["std_dev"] = tb[group_columns_sorted].max(axis=1)

        # Calculate the number of observations
        n = len(group_columns_sorted)

        # Calculate the standard error
        tb[f"std_err_{group}_until_2024"] = tb["std_dev"] / np.sqrt(n)

        # Calculate the upper and lower bounds of the standard deviation
        tb[f"upper_bound_{group}"] = tb[group_columns_sorted].max(axis=1)
        tb[f"lower_bound_{group}"] = tb[group_columns_sorted].min(axis=1)

        # Drop original columns as they are used in a different dataset
        tb = tb.drop(columns=[f"std_err_{group}_until_2024", f"{group}_until_2024", "std_dev"] + group_columns)
        # Dynamically set origins based on the group
        origin_column = f"_2024_{group}"  # Dynamically set based on group name
        if origin_column in tb.columns:  # Check if the origin column exists
            for col in [f"avg_{group}_until_2024", f"upper_bound_{group}", f"lower_bound_{group}"]:
                tb[col].origins = tb[origin_column].origins

    # Set 'country' and 'year' as the index of the DataFrame
    tb = tb.set_index(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_grapher.metadata)
    ds_grapher.save()
