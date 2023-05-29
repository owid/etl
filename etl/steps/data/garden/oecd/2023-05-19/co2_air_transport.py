"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()
# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("co2_air_transport.start")
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("co2_air_transport")
    ds_tour: Dataset = paths.load_dependency("unwto")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["co2_air_transport"]
    tb_tour = ds_tour["unwto"]
    df = pd.DataFrame(tb_meadow)  # Create a dataframe with data from the co2 transport table.
    df_tr = pd.DataFrame(tb_tour)  # Create a dataframe with data from the tourism table.

    # list of months
    month_names = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]

    #
    # Process data.
    #
    log.info("co2_air_transport.harmonize_countries")

    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)
    # Only use passenger flights and total international/domestic emissions
    df = df[df["flight_type"] == "P"]
    df.drop("flight_type", axis=1, inplace=True)
    df = df[df["emission_source"].isin(["TER_DOM", "TER_INT"])]
    df = df.reset_index(drop=True)

    # Process annual data
    pivot_table_ye = process_annual_data(df)

    # Add population data to the DataFrame
    pivot_table_ye = geo.add_population_to_dataframe(
        pivot_table_ye, country_col="country", year_col="year", population_col="population"
    )

    emissions_columns = [col for col in pivot_table_ye.columns if col not in ("country", "year", "population")]

    # Generate per capital co2 emissions data and add it do the dataframe and convert to pounds
    for col in emissions_columns:
        pivot_table_ye[f"per_capita_{col}"] = (pivot_table_ye[col] * 2204.62) / pivot_table_ye["population"]

    # Add Inbound/Outbound tourism to the dataframe (multiply international aviation emissions by international arrivals/departures)
    pivot_outb = add_inbound_outbound_tour(pivot_table_ye, df_tr)

    # Process monthly data
    pivot_df, pivot_table_mn = process_monthly_data(df, month_names)

    concatenated_df = pd.merge(pivot_outb, pivot_table_mn, on=["year", "country"], how="outer")
    merge_df = pd.merge(concatenated_df, pivot_df, on=["year", "country"], how="outer")

    merge_df = ukraine_fill_war_for_reg_agg(merge_df)
    merge_df = merge_df.drop("TER_INT_m", axis=1)

    merge_df = merge_df.rename(columns={"ter_int_m_filled_ukraine": "TER_INT_m"})

    regions_ = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]

    for region in regions_:
        merge_df = geo.add_region_aggregates(
            df=merge_df,
            country_col="country",
            countries_that_must_have_data=[],
            year_col="year",
            region=region,
            frac_allowed_nans_per_year=0.9,
        )

    merge_df["TER_INT_m"] = merge_df["TER_INT_m"].replace(0, np.nan)
    merge_df = merge_df[merge_df["year"] != 2023]

    # Apply the function to each row using apply()
    merge_df.loc[~merge_df.country.isin(regions_), month_names] = np.nan
    merge_df.reset_index(inplace=True, drop=True)

    # Melt the DataFrame to have months as rows
    df_melted = merge_df.melt(id_vars=["country", "year"], value_vars=month_names, var_name="month", value_name="value")
    filtered_df = df_melted.dropna(subset=["value"])

    # Pivot the melted DataFrame to reshape it with countries as columns
    df_pivoted = (
        filtered_df.pivot_table(index=["month", "year"], columns="country", values="value")
        .reset_index()
        .rename(columns={"month": "country"})
    )

    merged_df = pd.merge(df_pivoted, merge_df, on=["country", "year"], how="outer")

    # Create a new table with the processed data.
    tb_garden = Table(merged_df, short_name="co2_air_transport")
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("co2_air_transport.end")


def ukraine_fill_war_for_reg_agg(df):
    """
    Fill missing values for 'Ukraine' in the DataFrame with zero values for the 'TER_INT_m'
    column for concatenating regional emissions (then reset back to NaN) (2023 war so technically zero emissions and shouldn't be misleading for regional aggregates)

    Args:
        df (pd.DataFrame): Input DataFrame containing CO2 transport data.

    Returns:
        pd.DataFrame: DataFrame with missing values filled for 'Ukraine' in the 'TER_INT_m' column.

    """
    # Get a list of unique countries
    unique_countries = df["country"].unique()

    # Get the range of years that exist in the 'year' column
    all_years = df["year"].unique()  # Inclusive range of years

    # Create a DataFrame with all possible combinations of years and countries
    new_index = pd.MultiIndex.from_product([all_years, unique_countries], names=["year", "country"])
    new_df = pd.DataFrame(index=new_index).reset_index()

    # Merge the new DataFrame with the original DataFrame
    merged_df = pd.merge(new_df, df, on=["year", "country"], how="left")

    # Optionally, sort the resulting DataFrame by the 'year' and 'country' columns
    merged_df = merged_df.sort_values(["year", "country"]).reset_index(drop=True)

    # Set values to NaN for missing years
    merged_df.loc[merged_df["year"] < merged_df["year"].min(), "TER_INT_m"] = float("nan")
    merged_df.loc[merged_df["year"] > merged_df["year"].max(), "TER_INT_m"] = float("nan")

    # Create a Boolean mask to identify rows where the year is outside the desired range
    year_mask = (merged_df["year"] < 2015) | (merged_df["year"] > 2023)

    # Create a Boolean mask to identify rows where the country is 'Ukraine'
    country_mask = merged_df["country"] == "Ukraine"

    # Combine the masks using the logical AND operator
    combined_mask = year_mask & country_mask

    # Create the 'ter_int_m_filled_ukraine' column and fill it with the original 'TER_INT_m' values
    merged_df["ter_int_m_filled_ukraine"] = merged_df["TER_INT_m"]

    # Fill NaN values with 0s in the 'ter_int_m_filled_ukraine' column where the year is outside the range and the country is 'Ukraine'
    merged_df.loc[combined_mask, "ter_int_m_filled_ukraine"] = merged_df.loc[
        combined_mask, "ter_int_m_filled_ukraine"
    ].fillna(0)

    return merged_df


def process_annual_data(df):
    """
    Process annual data from the DataFrame.

    Parameters:
    df (pandas.DataFrame): The dataframe to be preprocessed.

    Returns:
    pandas.DataFrame: The preprocessed dataframe.
    """

    df_an = df[df["frequency"] == "Annual"]
    df_an = df_an.drop(["frequency", "month"], axis=1)

    df_an["emission_source"] = df_an["emission_source"].apply(lambda x: x + "_a")
    df_an.set_index(["country", "year", "emission_source"], inplace=True)

    assert df_an.index.is_unique, "Index is not well constructed"
    df_an.reset_index(inplace=True)

    pivot_table_ye = pd.pivot_table(df_an, values="value", index=["country", "year"], columns=["emission_source"])
    pivot_table_ye.reset_index(inplace=True)

    return pivot_table_ye


def process_monthly_data(df, month_names):
    """
    Process monthly data from the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame containing monthly data.
        month_names (list): List of month names.

    Returns:
        tuple: A tuple containing two DataFrames - pivot_df and pivot_table_mn.

    """
    # Filter rows with 'Monthly' frequency
    df_mn = df[df["frequency"] == "Monthly"]
    df_mn = df_mn.drop(["frequency"], axis=1)

    # Create a new 'date' column separately
    date_column = pd.to_datetime(df_mn["year"].astype(str) + "-" + df_mn["month"].astype(str) + "-15")
    df_mn["date"] = date_column
    df_mn["emission_source"] = df_mn["emission_source"].apply(lambda x: x + "_m")

    # Split by month
    pivot_df = pd.pivot_table(
        df_mn[df_mn["emission_source"] == "TER_INT_m"], values="value", index=["country", "year"], columns="month"
    )
    pivot_df.reset_index(inplace=True)
    pivot_df.columns = ["country", "year"] + month_names

    # Calculate the number of days since 2019
    df_mn["days_since_2019"] = (df_mn["date"] - pd.to_datetime("2019-01-01")).dt.days
    df_mn.drop(["month", "year", "date"], axis=1, inplace=True)
    df_mn.rename(columns={"days_since_2019": "year"}, inplace=True)

    # Pivot the table for monthly data
    pivot_table_mn = pd.pivot_table(df_mn, values="value", index=["country", "year"], columns=["emission_source"])
    pivot_table_mn.reset_index(inplace=True)

    return pivot_df, pivot_table_mn


def add_inbound_outbound_tour(df, df_tr):
    """
    Add inbound and outbound tourism data to the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame containing CO2 transport data.
        df_tr (pd.DataFrame): DataFrame containing tourism data.

    Returns:
        pd.DataFrame: DataFrame with added inbound and outbound tourism data.

    """
    # Extract relevant columns from the tourism DataFrame
    just_inb_ratio = df_tr[["country", "year", "inb_outb_tour"]]

    # Merge the CO2 transport DataFrame with the tourism DataFrame based on 'year' and 'country'
    df = pd.merge(df, just_inb_ratio, on=["year", "country"])

    # Calculate the interaction between TER_INT_a and inb_outb_tour
    df["int_inb_out_per_capita"] = df["per_capita_TER_INT_a"] * df["inb_outb_tour"]
    df["int_inb_out"] = df["TER_INT_a"] * df["inb_outb_tour"]

    # Drop the 'inb_outb_tour' column
    df = df.drop(["inb_outb_tour"], axis=1)

    return df
