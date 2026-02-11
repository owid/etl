"""
TODO: Update this docstring.
We need to have the share of emissions for the following custom list of broad sectors. Unfortunately, I haven't found a perfect mapping onto those categories from publicly available data.

Climate Watch (where we get our data for emissions by sector) has the following sectors:
Agriculture
Building
Bunker Fuels
Electricity/Heat
Fugitive Emissions
Industrial Processes
Land-Use Change and Forestry
Manufacturing/Construction
Other Fuel Combustion
Transportation
Waste

These don't map well to our desired categories (especially Electricity/Heat).

Climate Watch's data does have more granularity than this, but they don't provide access to the more granular data. What I will do is manually extract the percentages from this page:
https://www.wri.org/data/world-greenhouse-gas-emissions-sector-2021-sunburst-chart
which refers to 2021 (even though they have data for 2022, they haven't updated these visualizations yet).
Then, figure out a reasonable mapping of subsectors onto my custom categories.

"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Year that Climate Watch data refers to.
YEAR = 2021


# Custom remapping of Climate Watch subsectors into our custom categories.
SECTOR_MAPPING = {
    "Growing food": [
        "agriculture",
        "land_use_change_and_forestry",
    ],
    "Getting around": [
        "transport",
        "aviation_and_shipping",
    ],
    "Keeping warm and cool": [
        "buildings",
    ],
    "Electricity": [
        "electricity_and_heat",
    ],
    "Making things": ["manufacturing_and_construction", "industry", "fugitive", "other_fuel_combustion", "waste"],
}

# Create a list of expected sectors to be found in the data.
EXPECTED_SECTORS = sum(SECTOR_MAPPING.values(), [])


def sanity_check_inputs(tb_ghg, tb_co2):
    # List all columns sector emissions in the table of GHG emissions.
    columns_sectors = [
        column for column in tb_ghg.columns if "per_capita" not in column if column not in ["country", "year"]
    ]
    # Remove energy sector, since it's a group of subsectors; idem for total columns.
    columns_sectors = [
        column for column in columns_sectors if column not in ["energy", "total_excluding_lucf", "total_including_lucf"]
    ]
    error = "Unexpected list of sectors."
    assert set(columns_sectors) == set(EXPECTED_SECTORS), error

    error = "All sectors with GHG emissions should be included in the CO2 emissions table, except for agriculture and waste (for which there is no data on CO2 emissions)."
    assert set(EXPECTED_SECTORS) - set(tb_co2.columns) == {"agriculture", "waste"}, error

    # Ensure that the sum of all sector emissions yields the total of emissions (within a few percent).
    for gas in ["ghg", "co2"]:
        _tb = tb_ghg if gas == "ghg" else tb_co2
        _columns_sectors = [column for column in columns_sectors if column in _tb.columns]
        _tb["sum"] = _tb[_columns_sectors].sum(axis=1)
        # NOTE: It seems that the sum is systematically a few percent higher than the total.
        # This may be a problem in the original data (possibly due to rounding).
        error = "Sum of emissions differs from total more than a few percent"
        assert ((100 * abs(_tb["sum"] - _tb["total_including_lucf"]) / _tb["total_including_lucf"]) < 4).all(), error
        # Uncomment to compare visually.
        # import plotly.express as px
        # px.line(_tb[["year", "sum", "total_including_lucf"]].melt(id_vars=["year"]), x="year", y="value", color="variable", title=gas, markers=True).show()


def run() -> None:
    #
    # Load inputs.
    #
    # Load Climate Watch's emissions by sector and read its table on CO2 and GHG emissions.
    ds = paths.load_dataset("emissions_by_sector")
    tb_co2 = ds.read("carbon_dioxide_emissions_by_sector")
    tb_ghg = ds.read("greenhouse_gas_emissions_by_sector")

    #
    # Process data.
    #
    # Select only global data.
    tb_co2 = tb_co2[tb_co2["country"] == "World"].reset_index(drop=True)
    tb_ghg = tb_ghg[tb_ghg["country"] == "World"].reset_index(drop=True)

    # Rename columns in both tables, for convenience.
    tb_co2 = tb_co2.rename(
        columns={column: column.replace("_co2_emissions", "") for column in tb_co2.columns}, errors="raise"
    )
    tb_ghg = tb_ghg.rename(
        columns={column: column.replace("_ghg_emissions", "") for column in tb_ghg.columns}, errors="raise"
    )

    # Sanity checks.
    sanity_check_inputs(tb_ghg=tb_ghg, tb_co2=tb_co2)

    # Add missing agriculture and waste sectors to CO2 table (with zeros).
    tb_co2["agriculture"] = 0
    tb_co2["waste"] = 0

    # Keep only columns of sector emissions.
    tb_co2 = tb_co2[["country", "year"] + EXPECTED_SECTORS]
    tb_ghg = tb_ghg[["country", "year"] + EXPECTED_SECTORS]

    for sector, subsectors in SECTOR_MAPPING.items():
        # Create aggregates of custom sectors for CO2 emissions.
        tb_co2[sector] = tb_co2[subsectors].sum(axis=1)
        tb_co2 = tb_co2.drop(columns=subsectors, errors="raise")

        # Create aggregates of custom sectors for GHG emissions.
        tb_ghg[sector] = tb_ghg[subsectors].sum(axis=1)
        tb_ghg = tb_ghg.drop(columns=subsectors, errors="raise")

    # For convenience, transpose tables.
    tb_co2 = tb_co2.melt(id_vars=["country", "year"], value_name="co2_emissions", var_name="sector")
    tb_ghg = tb_ghg.melt(id_vars=["country", "year"], value_name="ghg_emissions", var_name="sector")

    # Combine both tables.
    tb = tb_ghg.merge(tb_co2, on=["country", "year", "sector"], how="outer")

    # Add an explanation to the metadata of which subsectors are included in each category.
    # TODO: Update this description.
    description_processing = "Each category is made up of the following emission subcategories, based on IPCC definitions (as reported by World Resources Institute):"
    for sector, subsectors in SECTOR_MAPPING.items():
        _subsectors = (
            ", ".join([s.replace("fugitive", "fugitive emissions").replace("_", " ") for s in subsectors]) + "."
        )
        description_processing += f"\n- {sector}: {_subsectors}"
    print(description_processing)
    tb["co2_emissions"].metadata.description_processing = description_processing
    tb["ghg_emissions"].metadata.description_processing = description_processing

    # Improve table format.
    tb = tb.format(keys=["country", "year", "sector"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
