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

import owid.catalog.processing as pr

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


def separate_electricity_and_heat(tb, tb_ember):
    # Select electricity (which includes some heat) and heat sectors.
    tb_electricity_and_heat = tb[tb["sector"] == "Electricity"].reset_index(drop=True)
    tb_heat = tb[tb["sector"] == "Keeping warm and cool"].reset_index(drop=True)

    # TODO: Note that Ember's emissions are lifecycle emissions. Consider calculating direct emissions by converting total generation of coal, gas and oil into CO2 emissions, with some conversion factors.
    # I could get emission factors from the existing garden step, and multiply them by total generation from coal gas and oil; for now I could use these factors here:
    # GHG per kWh (direct, in CO2e; CO2 + CH4 + N2O):
    # •	Coal: ~1035 gCO2e/kWh
    # •	Oil: ~857 gCO2e/kWh
    # •	Gas: ~452 gCO2e/kWh
    tb_ember = tb_ember[tb_ember["country"] == "World"][
        ["year", "emissions__lifecycle__total_emissions__mtco2"]
    ].rename(columns={"emissions__lifecycle__total_emissions__mtco2": "emissions_electricity"})
    # Convert from million tonnes to tonnes of CO2.
    tb_ember["emissions_electricity"] *= 1e6
    # Add Ember's emissions to the original electricity and heat emissions table.
    tb_electricity_and_heat = tb_electricity_and_heat.merge(tb_ember, on="year", how="inner")
    # NOTE: Ember's emissions are GHG emissions in CO2 equivalents. However, non-CO2 emissions of electricity and heat are probably less than ~1%, so we can safely assume that roughly all electricity and heat emissions are CO2 emissions.
    # This assumption can be easily confirmed by looking at the current Climate Watch data.
    assert (
        (
            100
            * (tb_electricity_and_heat["ghg_emissions"] - tb_electricity_and_heat["co2_emissions"])
            / tb_electricity_and_heat["ghg_emissions"]
        )
        < 1
    ).all()

    # Uncomment to plot GHG, CO2 electricity and heat emissions from Climate Watch, and Ember's emissions.
    # import plotly.express as px
    # px.line(tb_electricity_and_heat.drop(columns=["country", "sector"]).melt(id_vars="year"), x="year", y="value", color="variable", markers=True).update_yaxes(range=[0, None])

    # Calculate additional CO2 and GHG emissions related to heat.
    additional_heat = tb_electricity_and_heat.copy()
    with pr.ignore_warnings():
        additional_heat["ghg_emissions"] -= tb_electricity_and_heat["emissions_electricity"]
        additional_heat["co2_emissions"] -= tb_electricity_and_heat["emissions_electricity"]
    additional_heat = additional_heat.drop(columns=["country", "sector", "emissions_electricity"])

    # Create a new pair of series of CO2 and GHG emissions from only electricity (from Ember).
    tb_electricity = tb_electricity_and_heat[["country", "year", "sector", "emissions_electricity"]].rename(
        columns={"emissions_electricity": "ghg_emissions"}
    )
    tb_electricity["co2_emissions"] = tb_electricity["ghg_emissions"].copy()

    # Create a new "Keeping warm and cool" series that includes the heat removed from the electricity and heat sector.
    tb_heat = tb_heat.merge(additional_heat, on=["year"], how="inner", suffixes=("", "_additional"))

    # Uncomment to plot GHG, CO2 heat emissions from Climate Watch, and derived additional heat emissions.
    # import plotly.express as px
    # px.line(tb_heat.drop(columns=["country", "sector"]).melt(id_vars="year"), x="year", y="value", color="variable", markers=True).update_yaxes(range=[0, None])

    tb_heat["ghg_emissions"] += tb_heat["ghg_emissions_additional"]
    tb_heat["co2_emissions"] += tb_heat["co2_emissions_additional"]
    tb_heat = tb_heat.drop(columns=["ghg_emissions_additional", "co2_emissions_additional"], errors="raise")

    # Replace "Electricity" and "Keeping warm and cool") with the new estimates.
    with pr.ignore_warnings():
        tb_corrected = pr.concat(
            [tb[~tb["sector"].isin(["Electricity", "Keeping warm and cool"])], tb_electricity, tb_heat],
            ignore_index=True,
        )

    # Fix metadata.
    for column in ["co2_emissions", "ghg_emissions"]:
        tb_corrected[column].metadata.unit = tb[column].metadata.unit
        tb_corrected[column].metadata.short_unit = tb[column].metadata.short_unit

    # Now electricity and heat sectors have data only since 2000. Drop previous years, to avoid incomplete data.
    assert tb_corrected[tb_corrected["sector"] == "Electricity"]["year"].min() == 2000
    tb_corrected = tb_corrected[tb_corrected["year"] >= 2000].reset_index(drop=True)

    # Sort conveniently.
    tb_corrected = tb_corrected.sort_values(["country", "year", "sector"]).reset_index(drop=True)

    # Uncomment to plot the old and new electricity and heat emissions.
    # tb_comparison = tb.copy()
    # tb_comparison["sector"] += " old"
    # tb_comparison = pr.concat([tb_comparison, tb_corrected], ignore_index=True)
    # px.line(tb_comparison, x="year", y="ghg_emissions", color="sector", markers=True)
    # px.line(tb_comparison, x="year", y="co2_emissions", color="sector", markers=True)

    return tb_corrected


def run() -> None:
    #
    # Load inputs.
    #
    # Load Climate Watch's emissions by sector and read its table on CO2 and GHG emissions.
    ds = paths.load_dataset("emissions_by_sector")
    tb_co2 = ds.read("carbon_dioxide_emissions_by_sector")
    tb_ghg = ds.read("greenhouse_gas_emissions_by_sector")

    # Load Ember's yearly electricity and read its main table.
    ds_ember = paths.load_dataset("yearly_electricity")
    tb_ember = ds_ember.read("yearly_electricity")

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

    # Create a new corrected table, where heat has been moved from the "Electricity" sector to the "Keeping warm and cool" sector.
    tb_corrected = separate_electricity_and_heat(tb=tb, tb_ember=tb_ember)

    # Combine original and corrected data.
    tb = tb.merge(tb_corrected, on=["country", "year", "sector"], how="outer", suffixes=("", "_corrected"))

    # Add an explanation to the metadata of which subsectors are included in each category.
    # TODO: Update this description.
    description_processing = "Each category is made up of the following emission subcategories, based on IPCC definitions (as reported by Climate Watch):"
    for sector, subsectors in SECTOR_MAPPING.items():
        _subsectors = (
            ", ".join([s.replace("fugitive", "fugitive emissions").replace("_", " ") for s in subsectors]) + "."
        )
        description_processing += f"\n- {sector}: {_subsectors}"
    for column in ["co2_emissions", "ghg_emissions", "co2_emissions_corrected", "ghg_emissions_corrected"]:
        tb[column].metadata.description_processing = description_processing
        tb[column].metadata.description_processing = description_processing

    # Improve table format.
    tb = tb.format(keys=["country", "year", "sector"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
