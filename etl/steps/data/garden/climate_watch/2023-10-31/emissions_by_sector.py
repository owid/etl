"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table, utils
from owid.datautils.dataframes import map_series

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

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
    "Land-Use Change and Forestry": "Land-use change and forestry emissions",
    "Manufacturing/Construction": "Manufacturing and construction emissions",
    "Other Fuel Combustion": "Other fuel combustion emissions",
    "Total excluding LUCF": "Total emissions excluding LUCF",
    "Total including LUCF": "Total emissions including LUCF",
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

# Convert million tonnes to tonnes.
MT_TO_T = 1e6


def add_region_aggregates(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    tb_regions = tb.copy()

    # Create region aggregates for all columns (with a simple sum) except for the column of efficiency factors.
    aggregations = {
        column: "sum" for column in tb_regions.columns if column not in ["country", "year", "efficiency_factor"]
    }

    # Add region aggregates.
    for region in REGIONS:
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
            additional_members=REGIONS[region].get("additional_members"),
            include_historical_regions_in_income_groups=True,
        )
        tb_regions = geo.add_region_aggregates(
            df=tb_regions,
            region=region,
            aggregations=aggregations,
            countries_in_region=members,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.9999,
        )
    # Copy metadata of original table.
    tb_regions = tb_regions.copy_metadata(from_table=tb)

    return tb_regions


def create_table_for_gas(
    tb: Table, gas: str, ds_regions: Dataset, ds_population: Dataset, ds_income_groups: Dataset
) -> Table:
    """Extract data for a particular gas and create a table with variables' metadata.

    Parameters
    ----------
    tb : Table
    gas : str
        Name of gas to consider (as called in "gas" column of the original data).
    ds_regions : Dataset
        Regions dataset.
    ds_population : Dataset
        Population dataset.
    ds_income_groups : Dataset
        Income groups dataset.

    Returns
    -------
    table_gas : Table
        Table with data for considered gas, and metadata for each variable.

    """
    # Select data for current gas.
    tb_gas = tb[tb["gas"] == gas].drop(columns="gas").reset_index(drop=True)

    # Pivot table to have a column for each sector.
    tb_gas = tb_gas.pivot(index=["country", "year"], columns="sector", values="value").reset_index()

    # Create region aggregates.
    tb_gas = add_region_aggregates(tb=tb_gas, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    # Add population to data.
    tb_gas = geo.add_population_to_table(tb=tb_gas, ds_population=ds_population)

    # List columns with emissions data.
    emissions_columns = [column for column in tb_gas.columns if column not in ["country", "year", "population"]]

    # Add per capita variables.
    for variable in emissions_columns:
        tb_gas[variable + PER_CAPITA_SUFFIX] = MT_TO_T * tb_gas[variable] / tb_gas["population"]

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

    # Set an appropriate index and sort conveniently.
    tb_gas = tb_gas.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return tb_gas


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("emissions_by_sector")
    tb = ds_meadow["emissions_by_sector"].reset_index()

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load population dataset.
    ds_population = paths.load_dataset("population")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Select only one data source (Climate Watch).
    tb = tb[tb["data_source"] == "Climate Watch"].reset_index(drop=True)

    # Check that there is only one unit in dataset.
    assert set(tb["unit"]) == {"MtCO₂e"}, "Unknown units in dataset"

    # Remove unnecessary columns.
    tb = tb.drop(columns=["unit", "id", "data_source", "iso_code3"])

    # Rename sectors.
    tb["sector"] = map_series(
        series=tb["sector"],
        mapping=SECTORS,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )
    tb["sector"] = tb["sector"].copy_metadata(tb["gas"])

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        warn_on_missing_countries=True,
        warn_on_unused_countries=True,
    )

    # Create one table for each gas, and one for all gases combined.
    tables = [
        create_table_for_gas(
            tb=tb, gas=gas, ds_regions=ds_regions, ds_population=ds_population, ds_income_groups=ds_income_groups
        )
        for gas in tb["gas"].unique()
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=tables, check_variables_metadata=True)
    ds_garden.save()
