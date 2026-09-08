"""Load a snapshot and create a meadow dataset.

It combines the following snapshots:
- GCP's Fossil CO2 emissions (long-format csv).
- GCP's official GCB global emissions (excel file) containing global bunker fuel and land-use change emissions.
- GCP's official GCB national emissions (excel file) containing consumption-based emissions for each country.
  - Production-based emissions from this file are also used, but just to include total emissions of regions
    according to GCP (e.g. "Africa (GCP)") and for sanity checks.
- GCP's official GCB national land-use change emissions (excel file) with land-use change emissions for each country.
  - This file has one sheet per bookkeeping model (BLUE, OSCAR, LUCE). All of them are loaded here; they are averaged
    in the garden step, following GCP's own approach.

"""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Bookkeeping models with a sheet of national land-use change emissions in GCB's land-use change data file.
# GCB's global land-use change emissions are the average of these three models.
LAND_USE_CHANGE_MODELS = ["BLUE", "OSCAR", "LUCE"]


def prepare_fossil_co2(tb_fossil_co2: Table) -> Table:
    # Set an appropriate index and sort conveniently.
    tb_fossil_co2 = tb_fossil_co2.format(["country", "year"], sort_columns=True)

    return tb_fossil_co2


def prepare_historical_budget(tb_historical: Table) -> Table:
    """Select variables and prepare the historical budget sheet of GCB's raw global data file.

    Parameters
    ----------
    tb_historical : Table
        Historical budget sheet of GCB's raw global data file.

    Returns
    -------
    tb_historical : Table
        Historical budget after selecting variables and processing them.

    """
    # Sanity check.
    error = "'Historical Budget' sheet in global data file has changed (consider changing 'skiprows')."
    assert tb_historical.columns[0] == "Year", error

    # Columns to select in historical budget and how to rename them.
    columns = {
        "Year": "year",
        "fossil emissions excluding carbonation": "global_fossil_emissions",
        "land-use change emissions": "global_land_use_change_emissions",
    }
    tb_historical = tb_historical[list(columns)].rename(columns=columns)

    # Add column for country (to be able to combine this with the national data).
    tb_historical["country"] = "World"

    # Set an index and sort row and columns conveniently.
    tb_historical = tb_historical.format(["country", "year"], sort_columns=True)

    # Rename table.
    tb_historical.metadata.short_name = "global_carbon_budget_historical_budget"

    return tb_historical


def prepare_land_use_emissions(tb_land_use: Table, model: str) -> Table:
    """Prepare data from the sheet of one bookkeeping model of the land-use change data file.

    Parameters
    ----------
    tb_land_use : Table
        Data from the sheet of one bookkeeping model of the land-use change emissions data file.
    model : str
        Name of the bookkeeping model, which is also the name of the sheet the data comes from.

    Returns
    -------
    tb_land_use : Table
        Processed land-use change emissions data for that model, in long format.

    """
    tb_land_use = tb_land_use.copy()

    # Sanity check.
    error = f"'{model}' sheet in national land-use change data file has changed (consider changing 'skiprows')."
    assert tb_land_use.columns[1] == "Afghanistan", error

    # Rename year column.
    tb_land_use = tb_land_use.rename(columns={tb_land_use.columns[0]: "year"})

    # Ignore countries that have no data.
    tb_land_use = tb_land_use.dropna(axis=1, how="all")

    # Remove rows that are either empty, or have some other additional operation (e.g. 2013-2022).
    tb_land_use = tb_land_use[tb_land_use["year"].astype(str).str.match(r"^\d{4}$")].reset_index(drop=True)

    # Restructure data to have a column for country and another for emissions.
    tb_land_use = tb_land_use.melt(id_vars="year", var_name="country", value_name="emissions")

    # Add a column for the bookkeeping model the data comes from.
    tb_land_use["model"] = model

    return tb_land_use


def prepare_national_emissions(tb: Table, column_name: str) -> Table:
    """Select variables and prepare the territorial emissions (or the consumption emissions) sheet of GCB's raw national
    data file.

    Parameters
    ----------
    tb : Table
        Territorial emissions (or consumption emissions) sheet of GCB's raw national data file.
    column_name : str
        Name to assign to emissions column to be generated.

    Returns
    -------
    tb_national : Table
        Processed territorial (or consumption) emissions sheet of GCB's raw national data file.

    """
    tb = tb.copy()

    error = f"Sheet in national data file for {column_name} has changed (consider changing 'skiprows')."
    assert tb.columns[1] == "Afghanistan", error

    # The zeroth column is expected to be year.
    tb = tb.rename(columns={tb.columns[0]: "year"})

    # Each column represents a country; then the final columns are regions, "Bunkers", and "Statistical Difference".
    # Keep "Bunkers", but remove "Statistical Difference" (which is almost completely empty).
    # In fact "Bunkers" is a global variable (I don't know why it is included at the national level), but this will be
    # handled at the garden step.

    # Remove unnecessary column.
    tb = tb.drop(columns=["Statistical Difference"])

    # Convert from wide to long format dataframe.
    tb = tb.melt(id_vars=["year"]).rename(columns={"variable": "country", "value": column_name})

    # Set an index and sort row and columns conveniently.
    tb = tb.format(["country", "year"], sort_columns=True)

    # Rename table.
    tb.metadata.short_name = f"global_carbon_budget_{column_name}"

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_fossil_co2 = paths.load_snapshot("global_carbon_budget_fossil_co2_emissions.csv")
    snap_global = paths.load_snapshot("global_carbon_budget_global_emissions.xlsx")
    snap_national = paths.load_snapshot("global_carbon_budget_national_emissions.xlsx")
    snap_land_use = paths.load_snapshot("global_carbon_budget_land_use_change_emissions.xlsx")

    # Load data from fossil CO2 emissions.
    tb_fossil_co2 = snap_fossil_co2.read()

    # Load historical budget from the global emissions file.
    tb_historical = snap_global.read(sheet_name="Historical Budget", skiprows=15)

    # The land-use change file is expected to have one sheet per bookkeeping model, plus a summary sheet.
    error = "Sheets in the national land-use change data file have changed (consider revising LAND_USE_CHANGE_MODELS)."
    assert pd.ExcelFile(snap_land_use.path).sheet_names == ["Summary"] + LAND_USE_CHANGE_MODELS, error

    # Load land-use emissions, from the sheet of each bookkeeping model.
    tb_land_use_per_model = {
        model: snap_land_use.read(sheet_name=model, skiprows=7) for model in LAND_USE_CHANGE_MODELS
    }

    # Load production-based national emissions.
    tb_production = snap_national.read(sheet_name="Territorial Emissions", skiprows=11)

    # Load consumption-based national emissions.
    tb_consumption = snap_national.read(sheet_name="Consumption Emissions", skiprows=8)

    #
    # Process data.
    #
    # Prepare data for fossil CO2 emissions.
    tb_fossil_co2 = prepare_fossil_co2(tb_fossil_co2=tb_fossil_co2)

    # Prepare data for historical emissions.
    tb_historical = prepare_historical_budget(tb_historical=tb_historical)

    # Prepare data for land-use emissions, and combine all bookkeeping models into one table.
    tb_land_use = pr.concat(
        [
            prepare_land_use_emissions(tb_land_use=tb_land_use, model=model)
            for model, tb_land_use in tb_land_use_per_model.items()
        ],
        ignore_index=True,
    ).format(["model", "country", "year"], sort_columns=True, short_name="global_carbon_budget_land_use_change")

    # Prepare data for production-based emissions, from the file of national emissions.
    tb_production = prepare_national_emissions(tb=tb_production, column_name="production_emissions")

    # Prepare data for consumption-based emissions, from the file of national emissions.
    tb_consumption = prepare_national_emissions(tb=tb_consumption, column_name="consumption_emissions")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=[tb_fossil_co2, tb_historical, tb_land_use, tb_production, tb_consumption],
        default_metadata=snap_fossil_co2.metadata,
    )
    ds_meadow.save()
