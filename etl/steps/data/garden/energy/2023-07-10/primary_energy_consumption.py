"""Garden step for Primary energy consumption dataset (part of the OWID Energy dataset), based on a combination of the
Energy Institute's Statistical Review of World Energy dataset and EIA data on energy consumption.

"""

from typing import cast

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers.geo import add_gdp_to_table, add_population_to_table
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9

# Countries whose data have to be removed since they were identified as outliers.
OUTLIERS = ["Gibraltar"]


def prepare_statistical_review_data(tb_review: Table) -> Table:
    """Prepare Statistical Review of World Energy data.

    Parameters
    ----------
    tb_review : Table
        Statistical Review of World Energy data.

    Returns
    -------
    tb_review : Table
        Selected data as a table with metadata.

    """
    tb_review = tb_review.reset_index()

    columns = {
        "country": "country",
        "year": "year",
        "primary_energy_consumption_equivalent_twh": "Primary energy consumption (TWh)",
    }
    tb_review = tb_review[list(columns)].rename(columns=columns)

    # Drop rows with missing values.
    tb_review = tb_review.dropna(how="any").reset_index(drop=True)

    return cast(Table, tb_review)


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


def combine_statistical_review_and_eia_data(tb_review: Table, tb_eia: Table) -> Table:
    """Combine Statistical Review and EIA data.

    Parameters
    ----------
    tb_review : Table
        Table from the Statistical Review dataset.
    tb_eia : Table
        Table from EIA energy consumption dataset.

    Returns
    -------
    combined : Table
        Combined data.

    """
    # Check that there are no duplicated rows in any of the two datasets.
    assert tb_review[
        tb_review.duplicated(subset=["country", "year"])
    ].empty, "Duplicated rows in Statistical Review data."
    assert tb_eia[tb_eia.duplicated(subset=["country", "year"])].empty, "Duplicated rows in EIA data."

    tb_review["source"] = "ei"
    tb_eia["source"] = "eia"
    # Combine EIA data (which goes further back in the past) with Statistical Review data (which is more up-to-date).
    # On coincident rows, prioritize Statistical Review data.
    index_columns = ["country", "year"]
    combined = pr.concat([tb_eia, tb_review], ignore_index=True, short_name=paths.short_name).drop_duplicates(
        subset=index_columns, keep="last"
    )

    # Add metadata to the new "source" column.
    combined["source"].metadata.sources = combined["Primary energy consumption (TWh)"].metadata.sources
    combined["source"].metadata.licenses = combined["Primary energy consumption (TWh)"].metadata.licenses

    # Sort conveniently.
    combined = combined.sort_values(index_columns).reset_index(drop=True)

    return combined


def add_annual_change(tb: Table) -> Table:
    """Add annual change variables to combined Statistical Review & EIA data.

    Parameters
    ----------
    tb : Table
        Combined data.

    Returns
    -------
    combined : Table
        Combined data after adding annual change variables.

    """
    combined = tb.copy()

    # Calculate annual change.
    combined = combined.sort_values(["country", "year"]).reset_index(drop=True)
    # NOTE: Currently, groupby pct_change doesn't propagate metadata properly. This has to be done manually.
    combined["Annual change in primary energy consumption (%)"] = (
        combined.groupby("country")["Primary energy consumption (TWh)"].pct_change() * 100
    ).copy_metadata(combined["Primary energy consumption (TWh)"])
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
    tb_with_population = tb.copy()

    # Add population to data.
    tb_with_population = add_population_to_table(
        tb=tb_with_population,
        ds_population=ds_population,
        population_col="Population",
        warn_on_missing_countries=False,
    )

    # Calculate consumption per capita.
    tb_with_population["Primary energy consumption per capita (kWh)"] = (
        tb_with_population["Primary energy consumption (TWh)"] / tb_with_population["Population"] * TWH_TO_KWH
    )

    return tb_with_population


def add_per_gdp_variables(tb: Table, ds_gdp: Dataset) -> Table:
    """Add a GDP column and add per-gdp variables.

    Parameters
    ----------
    tb : Table
        Data.
    ds_gdp : Dataset
        GDP dataset.

    Returns
    -------
    tb : Table
        Data after adding GDP and per-gdp variables.

    """
    tb = tb.copy()

    # Add GDP column to table.
    tb = add_gdp_to_table(tb=tb, ds_gdp=ds_gdp, gdp_col="GDP")

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
    # Load Statistical Review dataset and read its main table.
    ds_review: Dataset = paths.load_dependency("statistical_review_of_world_energy")
    tb_review = ds_review["statistical_review_of_world_energy"]

    # Load EIA dataset on energy consumption and read its main table.
    ds_eia: Dataset = paths.load_dependency("energy_consumption")
    tb_eia = ds_eia["energy_consumption"]

    # Load GDP dataset.
    ds_gdp: Dataset = paths.load_dependency("ggdc_maddison")

    # Load population dataset.
    ds_population: Dataset = paths.load_dependency("population")

    #
    # Process data.
    #
    # Prepare Statistical Review data.
    tb_review = prepare_statistical_review_data(tb_review=tb_review)

    # Prepare EIA data.
    tb_eia = prepare_eia_data(tb_eia=tb_eia)

    # Combine Statistical Review and EIA data.
    tb = combine_statistical_review_and_eia_data(tb_review=tb_review, tb_eia=tb_eia)

    # Add annual change.
    tb = add_annual_change(tb=tb)

    # Add per-capita variables.
    tb = add_per_capita_variables(tb=tb, ds_population=ds_population)

    # Add per-GDP variables.
    tb = add_per_gdp_variables(tb=tb, ds_gdp=ds_gdp)

    # Remove outliers.
    tb = remove_outliers(tb=tb)

    # Create an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_review.metadata, check_variables_metadata=True
    )
    ds_garden.save()
