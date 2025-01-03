"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset, last_date_accessed

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
    # Identify columns that include 'share_area_ha_cumulative'
    columns_to_rename = {
        col: col.replace("share_area_ha_cumulative", "share_cumulative_area")
        for col in tb.columns
        if "share_area_ha_cumulative" in col
    }

    # Rename the columns
    tb = tb.rename(columns=columns_to_rename)

    # Extract columns related to cumulative area burned and emissions.
    column_groups = ["area_ha_cumulative", "share_cumulative_area", "co2_cumulative", "pm2_5_cumulative"]
    # Filter the Table columns to include only those that match any of the indicators in the list
    columns = [col for col in tb.columns if any(group in col for group in column_groups)] + ["country", "year"]

    # Create a new Table with only the filtered columns
    tb = tb[columns]

    # Loop through each group of indicators and apply the same calculations
    for group in column_groups:
        # Select columns that contain the group name and a year between 2003 (minimum value in emisssions; 2012 in area burned) and 2023 (inclusive)
        group_columns = [col for col in tb.columns if group in col and "2003" <= col.split("_")[1] <= "2023"]

        tb_grouped = tb[group_columns + ["country", "year"]]

        # Sort the group columns by year
        group_columns_sorted = sorted(group_columns, key=lambda x: int(x.split("_")[1]))

        # Calculate the average until 2024
        tb[f"{group}_until_2024"] = tb[group_columns_sorted].sum(axis=1)
        tb[f"avg_{group}_until_2024"] = tb[f"{group}_until_2024"] / len(group_columns_sorted)

        # Process data for each country
        for country in tb["country"].unique():
            country_rows = tb_grouped[tb_grouped["country"] == country]

            # Select rows with year 52 (actually the last week of the year)
            country_row_52 = country_rows[country_rows["year"] == 52]

            if country_row_52.empty or country_row_52[group_columns_sorted].isnull().all(axis=1).all():
                continue

            # Find the column with the maximum value at year 52 (actually the last week of the year)
            max_col = country_row_52[group_columns_sorted].idxmax(axis=1).iloc[0]

            # Find the column with the minimum value at year 52 (actually the last week of the year)
            min_col = country_row_52[group_columns_sorted].idxmin(axis=1).iloc[0]

            # Set upper and lower bounds for all rows of this country using the columns identified at year 52
            tb.loc[tb["country"] == country, f"upper_bound_{group}"] = country_rows[max_col]
            tb.loc[tb["country"] == country, f"lower_bound_{group}"] = country_rows[min_col]

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
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb],
        default_metadata=ds_grapher.metadata,
        yaml_params={"date_accessed": last_date_accessed(tb), "year": last_date_accessed(tb)[-4:]},
    )
    ds_grapher.save()
