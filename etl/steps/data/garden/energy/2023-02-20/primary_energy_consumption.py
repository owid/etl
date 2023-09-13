"""Garden step for Primary energy consumption dataset (part of the OWID Energy dataset), based on a combination of BP's
Statistical Review dataset and EIA data on energy consumption.

"""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from shared import add_population

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9

# Countries whose data have to be removed since they were identified as outliers.
OUTLIERS = ["Gibraltar"]


def prepare_bp_data(tb_bp: Table) -> Table:
    """Prepare BP data.

    Parameters
    ----------
    tb_bp : Table
        BP data.

    Returns
    -------
    tb_bp : Table
        BP data as a table with metadata.

    """
    tb_bp = tb_bp.reset_index()

    bp_columns = {
        "country": "country",
        "year": "year",
        "primary_energy_consumption__twh": "Primary energy consumption (TWh)",
    }
    tb_bp = tb_bp[list(bp_columns)].rename(columns=bp_columns)

    # Drop rows with missing values.
    tb_bp = tb_bp.dropna(how="any").reset_index(drop=True)

    return cast(Table, tb_bp)


def prepare_eia_data(tb_eia: Table) -> Table:
    """Prepare EIA data.

    Parameters
    ----------
    tb_eia : Table
        EIA data.

    Returns
    -------
    eia_table : Table
        EIA data as a table with metadata.

    """
    tb_eia = tb_eia.reset_index()

    eia_columns = {
        "country": "country",
        "year": "year",
        "energy_consumption": "Primary energy consumption (TWh)",
    }
    tb_eia = tb_eia[list(eia_columns)].rename(columns=eia_columns)

    # Drop rows with missing values.
    tb_eia = tb_eia.dropna(how="any").reset_index(drop=True)

    return cast(Table, tb_eia)


def prepare_ggdc_data(tb_ggdc: Table) -> Table:
    """Prepare GGDC data.

    Parameters
    ----------
    tb_ggdc : Table
        GGDC data.

    Returns
    -------
    ggdc_table : Table
        GGDC data as a table with metadata.

    """
    tb_ggdc = tb_ggdc.reset_index()

    ggdc_columns = {
        "country": "country",
        "year": "year",
        "gdp": "GDP",
    }
    tb_ggdc = tb_ggdc[list(ggdc_columns)].rename(columns=ggdc_columns)

    # Drop rows with missing values.
    tb_ggdc = tb_ggdc.dropna(how="any").reset_index(drop=True)

    return cast(Table, tb_ggdc)


def combine_bp_and_eia_data(tb_bp: Table, tb_eia: Table) -> pd.DataFrame:
    """Combine BP and EIA data.

    Parameters
    ----------
    tb_bp : Table
        Table from BP Statistical Review dataset.
    tb_eia : Table
        Table from EIA energy consumption dataset.

    Returns
    -------
    combined : pd.DataFrame
        Combined data.

    """
    # Check that there are no duplicated rows in any of the two datasets.
    assert tb_bp[tb_bp.duplicated(subset=["country", "year"])].empty, "Duplicated rows in BP data."
    assert tb_eia[tb_eia.duplicated(subset=["country", "year"])].empty, "Duplicated rows in EIA data."

    tb_bp["source"] = "bp"
    tb_eia["source"] = "eia"
    # Combine EIA data (which goes further back in the past) with BP data (which is more up-to-date).
    # On coincident rows, prioritise BP data.
    index_columns = ["country", "year"]
    combined = cast(pd.DataFrame, pd.concat([tb_eia, tb_bp], ignore_index=True)).drop_duplicates(
        subset=index_columns, keep="last"
    )

    # Convert to conventional dataframe, and sort conveniently.
    combined = pd.DataFrame(combined).sort_values(index_columns).reset_index(drop=True)

    return cast(pd.DataFrame, combined)


def add_annual_change(df: pd.DataFrame) -> pd.DataFrame:
    """Add annual change variables to combined BP & EIA dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Combined BP & EIA dataset.

    Returns
    -------
    combined : pd.DataFrame
        Combined BP & EIA dataset after adding annual change variables.

    """
    combined = df.copy()

    # Calculate annual change.
    combined = combined.sort_values(["country", "year"]).reset_index(drop=True)
    combined["Annual change in primary energy consumption (%)"] = (
        combined.groupby("country")["Primary energy consumption (TWh)"].pct_change() * 100
    )
    combined["Annual change in primary energy consumption (TWh)"] = combined.groupby("country")[
        "Primary energy consumption (TWh)"
    ].diff()

    return combined


def add_per_capita_variables(df: pd.DataFrame, population: pd.DataFrame) -> pd.DataFrame:
    """Add a population column and add per-capita variables.

    Parameters
    ----------
    df : pd.DataFrame
        Data.
    population : pd.DataFrame
        Population data.

    Returns
    -------
    df : pd.DataFrame
        Data after adding population and per-capita variables.

    """
    df = df.copy()

    # Add population to data.
    df = add_population(
        df=df,
        population=population,
        country_col="country",
        year_col="year",
        population_col="Population",
        warn_on_missing_countries=False,
    )

    # Calculate consumption per capita.
    df["Primary energy consumption per capita (kWh)"] = (
        df["Primary energy consumption (TWh)"] / df["Population"] * TWH_TO_KWH
    )

    return df


def add_per_gdp_variables(df: pd.DataFrame, ggdc_table: Table) -> pd.DataFrame:
    """Add a GDP column and add per-gdp variables.

    Parameters
    ----------
    df : pd.DataFrame
        Data.
    ggdc_table : Table
        GDP data from the GGDC Maddison dataset.

    Returns
    -------
    df : pd.DataFrame
        Data after adding GDP and per-gdp variables.

    """
    df = df.copy()

    # Add population to data.
    df = pd.merge(df, ggdc_table, on=["country", "year"], how="left")

    # Calculate consumption per GDP.
    df["Primary energy consumption per GDP (kWh per $)"] = (
        df["Primary energy consumption (TWh)"] / df["GDP"] * TWH_TO_KWH
    )

    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove infinity values and data that has been identified as spurious outliers.

    Parameters
    ----------
    df : pd.DataFrame
        Data.

    Returns
    -------
    df : pd.DataFrame
        Data after removing spurious data.

    """
    df = df.copy()

    # Remove spurious values.
    df = df.replace(np.inf, np.nan)

    # Remove indexes of outliers from data.
    df = df[~df["country"].isin(OUTLIERS)].reset_index(drop=True)

    return df


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load BP statistical review dataset.
    ds_bp: Dataset = paths.load_dependency("statistical_review")
    # Read main table from dataset.
    tb_bp = ds_bp["statistical_review"]

    # Load EIA dataset on energy consumption.
    ds_eia: Dataset = paths.load_dependency("energy_consumption")
    # Read main table from dataset.
    tb_eia = ds_eia["energy_consumption"]

    # Load GGDC Maddison data on GDP.
    ds_ggdc: Dataset = paths.load_dependency("ggdc_maddison")
    # Read main table from dataset.
    tb_ggdc = ds_ggdc["maddison_gdp"]

    # Load population dataset from garden.
    ds_population: Dataset = paths.load_dependency("population")
    # Get table from dataset.
    tb_population = ds_population["population"]
    # Make a dataframe out of the data in the table, with the required columns.
    df_population = pd.DataFrame(tb_population)

    #
    # Process data.
    #
    # Prepare BP data.
    tb_bp = prepare_bp_data(tb_bp=tb_bp)

    # Prepare EIA data.
    tb_eia = prepare_eia_data(tb_eia=tb_eia)

    # Prepare GGDC data.
    tb_ggdc = prepare_ggdc_data(tb_ggdc=tb_ggdc)

    # Combine BP and EIA data.
    df = combine_bp_and_eia_data(tb_bp=tb_bp, tb_eia=tb_eia)

    # Add annual change.
    df = add_annual_change(df=df)

    # Add per-capita variables.
    df = add_per_capita_variables(df=df, population=df_population)

    # Add per-GDP variables.
    df = add_per_gdp_variables(df=df, ggdc_table=tb_ggdc)

    # Remove outliers.
    df = remove_outliers(df=df)

    # Create an appropriate index and sort conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create new table.
    table = Table(df, short_name="primary_energy_consumption")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[table])
    ds_garden.save()
