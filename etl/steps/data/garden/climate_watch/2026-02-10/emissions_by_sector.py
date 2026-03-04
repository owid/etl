"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table, utils
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# All sectors expected in the data, and how to rename them.
SECTORS = {
    "Agriculture": "Agriculture emissions",
    "Building": "Buildings emissions",
    "Bunker Fuels": "Aviation and shipping emissions",
    "Electricity/Heat": "Electricity and heat emissions",
    "Energy": "Energy emissions",
    "Fugitive Emissions": "Fugitive emissions",
    "Industrial Processes": "Industry emissions",
    "Land Use, Land-Use Change and Forestry": "Land-use change and forestry emissions",
    "Manufacturing/Construction": "Manufacturing and construction emissions",
    "Other Fuel Combustion": "Other fuel combustion emissions",
    "Total excluding LULUCF": "Total emissions excluding LUCF",
    "Total including LULUCF": "Total emissions including LUCF",
    "Transportation": "Transport emissions",
    "Waste": "Waste emissions",
}

# Suffix to add to the name of per capita variables.
PER_CAPITA_SUFFIX = "_per_capita"

# Mapping of gas name (as given in Climate Watch data) to the name of the corresponding output table.
TABLE_NAMES = {
    "All GHG": "Greenhouse gas emissions by sector",
    "CH4": "Methane emissions by sector",
    "CO2": "Carbon dioxide emissions by sector",
    "F-Gas": "Fluorinated gas emissions by sector",
    "N2O": "Nitrous oxide emissions by sector",
}

# Aggregate regions to add, following OWID definitions.
REGIONS = {
    # Continents.
    "Africa": {},
    "Asia": {},
    "Europe": {},
    # The EU27 is already included in the original data, and after inspection the data coincides with our aggregate.
    # So we simply keep the original data for EU27 given in the data.
    "North America": {},
    "Oceania": {},
    "South America": {},
    # Income groups.
    "Low-income countries": {},
    "Upper-middle-income countries": {},
    "Lower-middle-income countries": {},
    "High-income countries": {},
}

# Convert million tonnes to tonnes (for non per capita indicators).
MT_TO_T = 1e6

# Convert million tonnes to kilograms (for per capita indicators).
MT_TO_KG = 1e9


def create_table_for_gas(tb: Table, gas: str) -> Table:
    """Extract data for a particular gas and create a table with variables' metadata.

    Parameters
    ----------
    tb : Table
    gas : str
        Name of gas to consider (as called in "gas" column of the original data).

    Returns
    -------
    table_gas : Table
        Table with data for considered gas, and metadata for each variable.

    """
    # Select data for current gas.
    tb_gas = tb[tb["gas"] == gas].drop(columns="gas").reset_index(drop=True)

    # Pivot table to have a column for each sector.
    tb_gas = tb_gas.pivot(index=["country", "year"], columns="sector", values="value", join_column_levels_with="_")

    # Create region aggregates for all columns (with a simple sum) except for the column of efficiency factors.
    aggregations = {
        column: "sum" for column in tb_gas.columns if column not in ["country", "year", "efficiency_factor"]
    }
    tb_gas = paths.regions.add_aggregates(
        tb=tb_gas,
        aggregations=aggregations,
        regions=REGIONS,
        min_frac_countries_informed=0.7,
        min_num_values_per_year=1,
    )

    # Add population to data.
    tb_gas = paths.regions.add_per_capita(tb=tb_gas, regions=REGIONS, suffix=PER_CAPITA_SUFFIX)

    # List columns with emissions data.
    emissions_columns = [column for column in tb_gas.columns if column not in ["country", "year"]]

    for variable in emissions_columns:
        if PER_CAPITA_SUFFIX in variable:
            # Adapt units of per capita indicators from million tonnes to kilograms.
            tb_gas[variable] *= MT_TO_KG
        else:
            # Adapt non per capita indicators from million tonnes to tonnes.
            tb_gas[variable] *= MT_TO_T

    # Remove rows and columns that only have nans.
    tb_gas = tb_gas.dropna(how="all", axis=1)
    tb_gas = tb_gas.dropna(subset=emissions_columns, how="all").reset_index(drop=True)

    # Adapt table title and short name.
    tb_gas.metadata.title = TABLE_NAMES[gas]
    tb_gas.metadata.short_name = utils.underscore(TABLE_NAMES[gas])

    # Adapt column names.
    tb_gas = tb_gas.rename(
        columns={
            column: utils.underscore(column)
            .replace("emissions", f"{utils.underscore(gas)}_emissions")
            .replace("all_ghg", "ghg")
            .replace("f_gas", "fgas")
            for column in tb_gas.columns
            if column not in ["country", "year"]
        }
    )

    # Improve table format.
    tb_gas = tb_gas.format(sort_columns=True)

    return tb_gas


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("emissions_by_sector")
    tb = ds_meadow.read("emissions_by_sector")

    #
    # Process data.
    #
    # Select only one data source (Climate Watch).
    tb = tb[tb["data_source"] == "Climate Watch"].reset_index(drop=True)

    # Check that there is only one unit in dataset.
    assert set(tb["unit"]) == {"MtCO₂e"}, "Unknown units in dataset"

    # Remove unnecessary columns.
    tb = tb.drop(columns=["unit", "id", "data_source", "iso_code3"], errors="raise")

    # Rename sectors.
    tb["sector"] = map_series(
        series=tb["sector"],
        mapping=SECTORS,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
        show_full_warning=True,
    )

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Create one table for each gas, and one for all gases combined.
    tables = [create_table_for_gas(tb=tb, gas=gas) for gas in tb["gas"].unique()]

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=tables)
    ds_garden.save()
