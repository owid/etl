import numpy as np
import pandas as pd

from etl.data_helpers import geo

REGIONS = {
    # Default continents.
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
}

INCOME_GROUPS = {  # Income groups.
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
}


def map_countries_and_merge_data(tb_data, ds_regions, ds_income_groups, tb_population, column_missing_data):
    """
    This function maps countries to their respective regions and income groups, merges the data with population data,
    and flags the presence of missing data in a specified column.

    Parameters:
    tb_data: Table containing the main data.
    ds_regions: Table containing the regions data.
    ds_income_groups: Table containing the income groups data.
    tb_population: Table containing the population data.
    column_missing_data (str): The column in which to flag the presence of missing data.

    Returns:
    df (DataFrame): Merged DataFrame with additional region, income group, and missing data information.
    """
    # Map countries to their regions and income groups.
    country_to_region = {
        country: region for region in REGIONS for country in geo.list_members_of_region(region, ds_regions)
    }

    country_to_income_group = {
        country: income_group
        for income_group in INCOME_GROUPS
        for country in geo.list_members_of_region(income_group, ds_regions, ds_income_groups)
    }

    # Ensure we have all the countries that are considered countries by World Bank.
    # Get all unique countries and years
    all_countries = set(country_to_income_group.keys())
    all_years = set(tb_data["year"].unique())

    # Create a DataFrame with all combinations of countries and years
    all_combinations = pd.DataFrame(
        [(country, year) for country in all_countries for year in all_years], columns=["country", "year"]
    )

    # Merge this DataFrame with tb_data
    df = pd.merge(all_combinations, tb_data, on=["country", "year"], how="left")

    # Map countries to their respective regions and income groups
    df["region"] = df["country"].map(country_to_region)
    df["income_group"] = df["country"].map(country_to_income_group)

    # Merge with population data.
    df = pd.merge(df, tb_population, on=["country", "year"]).drop(columns="source")

    # Flag for missing data in the specified column.
    df[column_missing_data] = np.where(
        df[column_missing_data].isna(),
        0,
        1,
    )

    return df


def calculate_missing_data(df, column_missing_data, group_by_column):
    """
    This function calculates the missing data for countries and population, grouped by a specified column.
    """
    # Group by year and group, then calculate total countries.
    total_countries = df.groupby(["year", group_by_column]).size().reset_index(name="total_countries")

    # Calculate missing data for countries and population.
    missing_data = (
        df[df[column_missing_data] == 0]
        .groupby(["year", group_by_column])
        .agg(missing_countries=("country", "size"), missing_population=("population", "sum"))
        .reset_index()
    )

    # Merge with total countries and calculate fractions.
    detailed_data = pd.merge(total_countries, missing_data, on=["year", group_by_column])
    detailed_data["fraction_missing_countries"] = (
        detailed_data["missing_countries"] / detailed_data["total_countries"]
    ) * 100

    # Rename columns for consistency.
    return detailed_data.rename(columns={group_by_column: "country"})


def combine_and_prepare_final_dataset(region_details, income_details, df_merged, column_missing_data):
    """
    Combine and prepare the final dataset.
    """
    df_final = pd.concat([region_details, income_details], axis=0)
    df_final["fraction_available_countries"] = 100 - df_final["fraction_missing_countries"]
    df_final = df_final.drop(columns=["total_countries"])

    combined = pd.concat(
        [df_final, df_merged[["country", "year", column_missing_data, "region", "income_group"]]],
        axis=0,
    )
    combined = combined.rename(columns={column_missing_data: column_missing_data + "_missing"})
    combined = combined.set_index(["country", "year"], verify_integrity=True)

    return combined
