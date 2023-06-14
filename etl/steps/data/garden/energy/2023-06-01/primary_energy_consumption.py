"""Garden step for Primary energy consumption dataset (part of the OWID Energy dataset), based on a combination of BP's
Statistical Review dataset and EIA data on energy consumption.

"""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers.geo import add_population_to_dataframe
from etl.helpers import PathFinder, create_dataset_with_combined_metadata

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


def combine_bp_and_eia_data(tb_bp: Table, tb_eia: Table) -> Table:
    """Combine BP and EIA data.

    Parameters
    ----------
    tb_bp : Table
        Table from BP Statistical Review dataset.
    tb_eia : Table
        Table from EIA energy consumption dataset.

    Returns
    -------
    combined : Table
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
    combined = Table(pd.concat([tb_eia, tb_bp], ignore_index=True)).drop_duplicates(subset=index_columns, keep="last")

    # Sort conveniently.
    combined = combined.sort_values(index_columns).reset_index(drop=True)

    return combined


def add_annual_change(tb: Table) -> Table:
    """Add annual change variables to combined BP & EIA data.

    Parameters
    ----------
    tb : Table
        Combined BP & EIA data.

    Returns
    -------
    combined : Table
        Combined BP & EIA data after adding annual change variables.

    """
    combined = tb.copy()

    # Calculate annual change.
    combined = combined.sort_values(["country", "year"]).reset_index(drop=True)
    combined["Annual change in primary energy consumption (%)"] = (
        combined.groupby("country")["Primary energy consumption (TWh)"].pct_change() * 100
    )
    combined["Annual change in primary energy consumption (TWh)"] = combined.groupby("country")[
        "Primary energy consumption (TWh)"
    ].diff()

    return combined


def add_per_capita_variables(tb: Table, ds_population: Dataset) -> Table:
    """Add a population column and add per-capita variables.

    Parameters
    ----------
    tb : Table
        Data.
    ds_population : Dataset
        Population dataset.

    Returns
    -------
    tb : Table
        Data after adding population and per-capita variables.

    """
    tb = tb.copy()

    # Add population to data.
    tb = add_population_to_dataframe(
        df=tb,
        ds_population=ds_population,
        country_col="country",
        year_col="year",
        population_col="Population",
        warn_on_missing_countries=False,
    )

    # Calculate consumption per capita.
    tb["Primary energy consumption per capita (kWh)"] = (
        tb["Primary energy consumption (TWh)"] / tb["Population"] * TWH_TO_KWH
    )

    return tb


def add_per_gdp_variables(tb: Table, ggdc_table: Table) -> Table:
    """Add a GDP column and add per-gdp variables.

    Parameters
    ----------
    tb : Table
        Data.
    ggdc_table : Table
        GDP data from the GGDC Maddison dataset.

    Returns
    -------
    tb : Table
        Data after adding GDP and per-gdp variables.

    """
    tb = tb.copy()

    # Add population to data.
    tb = pd.merge(tb, ggdc_table, on=["country", "year"], how="left")

    # Calculate consumption per GDP.
    tb["Primary energy consumption per GDP (kWh per $)"] = (
        tb["Primary energy consumption (TWh)"] / tb["GDP"] * TWH_TO_KWH
    )

    return tb


def remove_outliers(tb: Table) -> Table:
    """Remove infinity values and data that has been identified as spurious outliers.

    Parameters
    ----------
    tb : Table
        Data.

    Returns
    -------
    tb : Table
        Data after removing spurious data.

    """
    tb = tb.copy()

    # Remove spurious values.
    tb = tb.replace(np.inf, np.nan)

    # Remove indexes of outliers from data.
    tb = tb[~tb["country"].isin(OUTLIERS)].reset_index(drop=True)

    return tb


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load BP statistical review dataset and read its main table.
    ds_bp: Dataset = paths.load_dependency("statistical_review")
    tb_bp = ds_bp["statistical_review"]

    # Load EIA dataset on energy consumption and read its main table.
    ds_eia: Dataset = paths.load_dependency("energy_consumption")
    tb_eia = ds_eia["energy_consumption"]

    # Load GGDC Maddison data on GDP and read its main table.
    ds_ggdc: Dataset = paths.load_dependency("ggdc_maddison")
    tb_ggdc = ds_ggdc["maddison_gdp"]

    # Load population dataset.
    ds_population: Dataset = paths.load_dependency("population")

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
    tb = combine_bp_and_eia_data(tb_bp=tb_bp, tb_eia=tb_eia)

    # Add annual change.
    tb = add_annual_change(tb=tb)

    # Add per-capita variables.
    tb = add_per_capita_variables(tb=tb, ds_population=ds_population)

    # Add per-GDP variables.
    tb = add_per_gdp_variables(tb=tb, ggdc_table=tb_ggdc)

    # Remove outliers.
    tb = remove_outliers(tb=tb)

    # Create an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Update table short name.
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset_with_combined_metadata(dest_dir, datasets=[ds_bp, ds_eia], tables=[tb])
    ds_garden.save()
