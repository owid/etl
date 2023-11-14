"""Load a snapshot and create a meadow dataset.

It combines the following snapshots:
- GCP's Fossil CO2 emissions (long-format csv).
- GCP's official GCB global emissions (excel file) containing global bunker fuel and land-use change emissions.
- GCP's official GCB national emissions (excel file) containing consumption-based emissions for each country.
  - Production-based emissions from this file are also used, but just to include total emissions of regions
    according to GCP (e.g. "Africa (GCP)") and for sanity checks.
- GCP's official GCB national land-use change emissions (excel file) with land-use change emissions for each country.

"""

from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_fossil_co2(tb_fossil_co2: Table) -> Table:
    # Set an appropriate index and sort conveniently.
    tb_fossil_co2 = tb_fossil_co2.set_index(["Country", "Year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Ensure all columns are snake-case.
    tb_fossil_co2 = tb_fossil_co2.underscore()

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
    tb_historical = tb_historical.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Rename table.
    tb_historical.metadata.short_name = "global_carbon_budget_historical_budget"

    return tb_historical


def prepare_land_use_emissions(tb_land_use: Table) -> Table:
    """Prepare data from a specific sheet of the land-use change data file.

    Parameters
    ----------
    tb_land_use : Table
        Data from a specific sheet of the land-use change emissions data file.

    Returns
    -------
    tb_land_use : Table
        Processed land-use change emissions data.

    """
    tb_land_use = tb_land_use.copy()

    # Sanity check.
    error = "'BLUE' sheet in national land-use change data file has changed (consider changing 'skiprows')."
    assert tb_land_use.columns[1] == "Afghanistan", error

    # Extract quality flag from the zeroth row of the data.
    # Ignore nans (which happen when a certain country has no data).
    quality_flag = (
        tb_land_use.drop(columns=tb_land_use.columns[0])
        .loc[0]
        .dropna()
        .astype(int)
        .to_frame("quality_flag")
        .reset_index()
        .rename(columns={"index": "country"})
    )

    # Drop the first row, which is for quality factor (which we have already extracted).
    tb_land_use = tb_land_use.rename(columns={tb_land_use.columns[0]: "year"}).drop(0)

    # Ignore countries that have no data.
    tb_land_use = tb_land_use.dropna(axis=1, how="all")

    # Remove rows that are either empty, or have some other additional operation (e.g. 2013-2022).
    tb_land_use = tb_land_use[tb_land_use["year"].astype(str).str.match(r"^\d{4}$")].reset_index(drop=True)

    # Restructure data to have a column for country and another for emissions.
    tb_land_use = tb_land_use.melt(id_vars="year", var_name="country", value_name="emissions")

    # In the latest version of the file, there are three new regions.
    # For now, remove them (in the future, consider adding a sanity check for the World).
    tb_land_use = tb_land_use[~tb_land_use["country"].isin(["World", "EU27", "ROW"])].reset_index(drop=True)

    error = "Countries with emissions data differ from countries with quality flag."
    assert set(tb_land_use["country"]) == set(quality_flag["country"]), error

    # Add quality factor as an additional column.
    tb_land_use = tb_land_use.merge(quality_flag, how="left", on="country")

    # Copy metadata from another existing variable to the new quality flag.
    tb_land_use["quality_flag"] = tb_land_use["quality_flag"].copy_metadata(tb_land_use["emissions"])

    # Set an index and sort row and columns conveniently.
    tb_land_use = tb_land_use.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Rename table.
    tb_land_use.metadata.short_name = "global_carbon_budget_land_use_change"

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
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Rename table.
    tb.metadata.short_name = f"global_carbon_budget_{column_name}"

    return tb


def run(dest_dir: str) -> None:
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

    # Load land-use emissions.
    tb_land_use = snap_land_use.read(sheet_name="BLUE", skiprows=7)

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

    # Prepare data for land-use emissions.
    tb_land_use = prepare_land_use_emissions(tb_land_use=tb_land_use)

    # Prepare data for production-based emissions, from the file of national emissions.
    tb_production = prepare_national_emissions(tb=tb_production, column_name="production_emissions")

    # Prepare data for consumption-based emissions, from the file of national emissions.
    tb_consumption = prepare_national_emissions(tb=tb_consumption, column_name="consumption_emissions")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb_fossil_co2, tb_historical, tb_land_use, tb_production, tb_consumption],
        default_metadata=snap_fossil_co2.metadata,
        check_variables_metadata=True,
    )
    ds_meadow.save()
