"""Load a snapshot and create a meadow dataset.

It combines the following snapshots:
- GCP's Fossil CO2 emissions (long-format csv).
- GCP's official GCB global emissions (excel file) containing global bunker fuel and land-use change emissions.
- GCP's official GCB national emissions (excel file) containing consumption-based emissions for each country.
  - Production-based emissions from this file are also used, but just to include total emissions of regions
    according to GCP (e.g. "Africa (GCP)") and for sanity checks.
- GCP's official GCB national land-use change emissions (excel file) with land-use change emissions for each country.

"""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_fossil_co2(df_fossil_co2: pd.DataFrame) -> Table:
    # Set an appropriate index and sort conveniently.
    df_fossil_co2 = df_fossil_co2.set_index(["Country", "Year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Create a new table and ensure all columns are snake-case.
    tb_fossil_co2 = Table(df_fossil_co2, short_name="global_carbon_budget_fossil_co2_emissions", underscore=True)

    return tb_fossil_co2


def prepare_historical_budget(df_historical_budget: pd.DataFrame) -> Table:
    """Select variables and prepare the historical budget sheet of GCB's raw global data file.

    Parameters
    ----------
    df_historical_budget : pd.DataFrame
        Historical budget sheet of GCB's raw global data file.

    Returns
    -------
    tb_historical_budget : Table
        Historical budget after selecting variables and processing them.

    """
    # Sanity check.
    error = "'Historical Budget' sheet in global data file has changed (consider changing 'skiprows')."
    assert df_historical_budget.columns[0] == "Year", error

    # Columns to select in historical budget and how to rename them.
    columns = {
        "Year": "year",
        "fossil emissions excluding carbonation": "global_fossil_emissions",
        "land-use change emissions": "global_land_use_change_emissions",
    }
    df_historical_budget = df_historical_budget[list(columns)].rename(columns=columns)

    # Add column for country (to be able to combine this with the national data).
    df_historical_budget["country"] = "World"

    # Set an index and sort row and columns conveniently.
    df_historical_budget = (
        df_historical_budget.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)
    )

    # Create a table with the generated data.
    tb_historical_budget = Table(
        df_historical_budget, short_name="global_carbon_budget_historical_budget", underscore=True
    )

    return tb_historical_budget


def prepare_land_use_emissions(df_land_use: pd.DataFrame) -> Table:
    """Prepare data from a specific sheet of the land-use change data file.

    Parameters
    ----------
    df_land_use : pd.DataFrame
        Data from a specific sheet of the land-use change emissions data file.

    Returns
    -------
    tb_land_use : Table
        Processed land-use change emissions data.

    """
    df_land_use = df_land_use.copy()

    # Sanity check.
    error = "'BLUE' sheet in national land-use change data file has changed (consider changing 'skiprows')."
    assert df_land_use.columns[1] == "Afghanistan", error

    # Extract quality flag from the zeroth row of the data.
    # Ignore nans (which happen when a certain country has no data).
    quality_flag = (
        df_land_use.drop(columns=df_land_use.columns[0])
        .loc[0]
        .dropna()
        .astype(int)
        .to_frame("quality_flag")
        .reset_index()
        .rename(columns={"index": "country"})
    )

    # Drop the first row, which is for quality factor (which we have already extracted).
    df_land_use = df_land_use.rename(columns={df_land_use.columns[0]: "year"}).drop(0)

    # Ignore countries that have no data.
    df_land_use = df_land_use.dropna(axis=1, how="all")

    # Restructure data to have a column for country and another for emissions.
    df_land_use = df_land_use.melt(id_vars="year", var_name="country", value_name="emissions")

    error = "Countries with emissions data differ from countries with quality flag."
    assert set(df_land_use["country"]) == set(quality_flag["country"]), error

    # Add quality factor as an additional column.
    df_land_use = pd.merge(df_land_use, quality_flag, how="left", on="country")

    # Set an index and sort row and columns conveniently.
    df_land_use = df_land_use.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Create a table with the generated data.
    tb_land_use = Table(df_land_use, short_name="global_carbon_budget_land_use_change", underscore=True)

    return tb_land_use


def prepare_national_emissions(df: pd.DataFrame, column_name: str) -> Table:
    """Select variables and prepare the territorial emissions (or the consumption emissions) sheet of GCB's raw national
    data file.

    Parameters
    ----------
    df : pd.DataFrame
        Territorial emissions (or consumption emissions) sheet of GCB's raw national data file.
    column_name : str
        Name to assign to emissions column to be generated.

    Returns
    -------
    tb_national : Table
        Processed territorial (or consumption) emissions sheet of GCB's raw national data file.

    """
    df = df.copy()

    error = f"Sheet in national data file for {column_name} has changed (consider changing 'skiprows')."
    assert df.columns[1] == "Afghanistan", error

    # The zeroth column is expected to be year.
    df = df.rename(columns={df.columns[0]: "year"})

    # Each column represents a country; then the final columns are regions, "Bunkers", and "Statistical Difference".
    # Keep "Bunkers", but remove "Statistical Difference" (which is almost completely empty).
    # In fact "Bunkers" is a global variable (I don't know why it is included at the national level), but this will be
    # handled at the garden step.

    # Remove unnecessary column.
    df = df.drop(columns=["Statistical Difference"])

    # Convert from wide to long format dataframe.
    df = df.melt(id_vars=["year"]).rename(columns={"variable": "country", "value": column_name})

    # Set an index and sort row and columns conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Create a table with the generated data.
    tb_national = Table(df, short_name=f"global_carbon_budget_{column_name}", underscore=True)

    return tb_national


def run(dest_dir: str) -> None:
    log.info("global_carbon_budget.start")

    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_fossil_co2: Snapshot = paths.load_dependency("global_carbon_budget_fossil_co2_emissions.csv")
    snap_global: Snapshot = paths.load_dependency("global_carbon_budget_global_emissions.xlsx")
    snap_national: Snapshot = paths.load_dependency("global_carbon_budget_national_emissions.xlsx")
    snap_land_use: Snapshot = paths.load_dependency("global_carbon_budget_land_use_change_emissions.xlsx")

    # Load data from fossil CO2 emissions.
    df_fossil_co2 = pd.read_csv(snap_fossil_co2.path)

    # Load historical budget from the global emissions file.
    df_historical = pd.read_excel(snap_global.path, sheet_name="Historical Budget", skiprows=15)

    # Load land-use emissions.
    df_land_use = pd.read_excel(snap_land_use.path, sheet_name="BLUE", skiprows=7)

    # Load production-based national emissions.
    df_production = pd.read_excel(snap_national.path, sheet_name="Territorial Emissions", skiprows=11)

    # Load consumption-based national emissions.
    df_consumption = pd.read_excel(snap_national.path, sheet_name="Consumption Emissions", skiprows=8)

    #
    # Process data.
    #
    # Prepare data for fossil CO2 emissions.
    tb_fossil_co2 = prepare_fossil_co2(df_fossil_co2=df_fossil_co2)

    # Prepare data for historical emissions.
    tb_historical = prepare_historical_budget(df_historical_budget=df_historical)

    # Prepare data for land-use emissions.
    tb_land_use = prepare_land_use_emissions(df_land_use=df_land_use)

    # Prepare data for production-based emissions, from the file of national emissions.
    tb_production = prepare_national_emissions(df=df_production, column_name="production_emissions")

    # Prepare data for consumption-based emissions, from the file of national emissions.
    tb_consumption = prepare_national_emissions(df=df_consumption, column_name="consumption_emissions")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb_fossil_co2, tb_historical, tb_land_use, tb_production, tb_consumption],
        default_metadata=snap_fossil_co2.metadata,
    )

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("global_carbon_budget.end")
