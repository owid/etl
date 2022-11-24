"""Garden step for Primary energy consumption dataset (part of the OWID Energy dataset), based on a combination of BP's
Statistical Review dataset and EIA data on energy consumption.

"""

from typing import cast

import numpy as np
import pandas as pd
from owid import catalog
from owid.catalog.utils import underscore_table
from shared import CURRENT_DIR, add_population
from structlog import get_logger

from etl.paths import DATA_DIR

log = get_logger()

# Namespace and dataset short name for output dataset.
NAMESPACE = "energy"
DATASET_SHORT_NAME = "primary_energy_consumption"
# Metadata file.
METADATA_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"
# Path to EIA energy consumption dataset.
EIA_DATASET_PATH = DATA_DIR / "garden" / "eia" / "2022-07-27" / "energy_consumption"
# Path to BP statistical review dataset.
BP_DATASET_PATH = DATA_DIR / "garden" / "bp" / "2022-07-14" / "statistical_review"
# Path to GGDC Maddison 2020 GDP dataset.
GGDC_DATASET_PATH = DATA_DIR / "garden" / "ggdc" / "2020-10-01" / "ggdc_maddison"

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9

# Countries whose data have to be removed since they were identified as outliers.
OUTLIERS = ["Gibraltar"]


def load_bp_data() -> catalog.Table:
    """Load BP data from the local catalog, and rename columns conveniently.

    Returns
    -------
    bp_table : catalog.Table
        BP data as a table with metadata.

    """
    # Load BP Statistical Review dataset.
    bp_dataset = catalog.Dataset(BP_DATASET_PATH)

    # Get table.
    bp_table = bp_dataset[bp_dataset.table_names[0]].reset_index()
    bp_columns = {
        "country": "country",
        "year": "year",
        "primary_energy_consumption__twh": "Primary energy consumption (TWh)",
    }
    bp_table = bp_table[list(bp_columns)].rename(columns=bp_columns)

    # Drop rows with missing values.
    bp_table = bp_table.dropna(how="any").reset_index(drop=True)

    return cast(catalog.Table, bp_table)


def load_eia_data() -> catalog.Table:
    """Load EIA data from the local catalog, and rename columns conveniently.

    Returns
    -------
    eia_table : catalog.Table
        EIA data as a table with metadata.

    """
    # Load EIA energy consumption dataset.
    eia_dataset = catalog.Dataset(EIA_DATASET_PATH)

    # Get table.
    eia_table = eia_dataset[eia_dataset.table_names[0]].reset_index()
    eia_columns = {
        "country": "country",
        "year": "year",
        "energy_consumption": "Primary energy consumption (TWh)",
    }
    eia_table = eia_table[list(eia_columns)].rename(columns=eia_columns)

    # Drop rows with missing values.
    eia_table = eia_table.dropna(how="any").reset_index(drop=True)

    return cast(catalog.Table, eia_table)


def load_ggdc_data() -> catalog.Table:
    """Load GGDC data on GDP from the local catalog, and rename columns conveniently.

    Returns
    -------
    ggdc_table : catalog.Table
        GGDC data as a table with metadata.

    """
    # Load GGDC Maddison 2020 dataset on GDP.
    ggdc_dataset = catalog.Dataset(GGDC_DATASET_PATH)

    # Get table.
    ggdc_table = ggdc_dataset[ggdc_dataset.table_names[0]].reset_index()
    ggdc_columns = {
        "country": "country",
        "year": "year",
        "gdp": "GDP",
    }
    ggdc_table = ggdc_table[list(ggdc_columns)].rename(columns=ggdc_columns)

    # Drop rows with missing values.
    ggdc_table = ggdc_table.dropna(how="any").reset_index(drop=True)

    return cast(catalog.Table, ggdc_table)


def combine_bp_and_eia_data(bp_table: catalog.Table, eia_table: catalog.Table) -> pd.DataFrame:
    """Combine BP and EIA data.

    Parameters
    ----------
    bp_table : catalog.Table
        Table from BP Statistical Review dataset.
    eia_table : catalog.Table
        Table from EIA energy consumption dataset.

    Returns
    -------
    combined : pd.DataFrame
        Combined data.

    """
    # Check that there are no duplicated rows in any of the two datasets.
    assert bp_table[bp_table.duplicated(subset=["country", "year"])].empty, "Duplicated rows in BP data."
    assert eia_table[eia_table.duplicated(subset=["country", "year"])].empty, "Duplicated rows in EIA data."

    bp_table["source"] = "bp"
    eia_table["source"] = "eia"
    # Combine EIA data (which goes further back in the past) with BP data (which is more up-to-date).
    # On coincident rows, prioritise BP data.
    index_columns = ["country", "year"]
    combined = cast(pd.DataFrame, pd.concat([eia_table, bp_table], ignore_index=True)).drop_duplicates(
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


def add_per_capita_variables(df: pd.DataFrame) -> pd.DataFrame:
    """Add a population column and add per-capita variables.

    Parameters
    ----------
    df : pd.DataFrame
        Data.

    Returns
    -------
    df : pd.DataFrame
        Data after adding population and per-capita variables.

    """
    df = df.copy()

    # Add population to data.
    df = add_population(
        df=df,
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


def add_per_gdp_variables(df: pd.DataFrame, ggdc_table: catalog.Table) -> pd.DataFrame:
    """Add a GDP column and add per-gdp variables.

    Parameters
    ----------
    df : pd.DataFrame
        Data.
    ggdc_table : catalog.Table
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
    log.info(f"{DATASET_SHORT_NAME}.start")

    #
    # Load data.
    #
    # Load BP statistical review dataset.
    bp_table = load_bp_data()

    # Load EIA data on energy_consumption.
    eia_table = load_eia_data()

    # Load GGDC Maddison data on GDP.
    ggdc_table = load_ggdc_data()

    #
    # Process data.
    #
    # Combine BP and EIA data.
    df = combine_bp_and_eia_data(bp_table=bp_table, eia_table=eia_table)

    # Add annual change.
    df = add_annual_change(df=df)

    # Add per-capita variables.
    df = add_per_capita_variables(df=df)

    # Add per-GDP variables.
    df = add_per_gdp_variables(df=df, ggdc_table=ggdc_table)

    # Remove outliers.
    df = remove_outliers(df=df)

    #
    # Save outputs.
    #
    # Initialize new garden dataset.
    dataset = catalog.Dataset.create_empty(dest_dir)
    # Add metadata to dataset.
    dataset.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")
    # Create new dataset in garden.
    dataset.save()

    # Create new table and add it to new dataset.
    tb_garden = underscore_table(catalog.Table(df))

    tb_garden = tb_garden.set_index(["country", "year"])
    tb_garden.update_metadata_from_yaml(METADATA_PATH, DATASET_SHORT_NAME)
    dataset.add(tb_garden)

    log.info(f"{DATASET_SHORT_NAME}.end")
