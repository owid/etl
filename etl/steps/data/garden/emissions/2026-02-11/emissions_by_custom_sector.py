"""
We want to create a visualization showing the share of emissions that are produced by a custom list of sectors, namely:
- Growing food
- Getting around
- Keeping warm and cool
- Electricity
- Making things

Unfortunately, the most useful data to be able to create these custom categories is the IEA, which is under a heavy paywall. I haven't found a perfect mapping onto those categories from publicly available data.

Climate Watch (where we get our data for emissions by sector) has the following sectors:
- Agriculture
- Building
- Bunker Fuels
- Electricity/Heat
- Fugitive Emissions
- Industrial Processes
- Land-Use Change and Forestry
- Manufacturing/Construction
- Other Fuel Combustion
- Transportation
- Waste

These don't map perfectly well to our desired categories (especially Electricity/Heat); but an approximate mapping is possible.
Climate Watch's original data does have more granularity than this, but they don't provide access to the more granular data (because it indeed comes from IEA).

"""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Year that Climate Watch data refers to.
YEAR = 2021


# Custom remapping of Climate Watch subsectors into our custom categories.
# See WRI's documentation for the meaning of each sector:
# https://wri-sites.s3.us-east-1.amazonaws.com/climatewatch.org/www.climatewatch.org/climate-watch/wri_metadata/CW_GHG_Method_Note.pdf
# Also see distribution of subsectors (as of 2026-02-11, the latest data is still for 2021!):
# https://www.wri.org/data/world-greenhouse-gas-emissions-sector-2021-sunburst-chart
SECTOR_MAPPING = {
    "Growing food": [
        # Agriculture sector contains emissions from following activities:
        # - CH4 emissions from Enteric fermentation (livestock)
        # - CH4 and N2O emissions from Manure management (livestock)
        # - CH4 emissions from Rice cultivation
        # - N2O emissions from Agriculture soils
        # - Crop residues
        # - Drained organic soils
        # - Manure applied to soils
        # - Manure left on pasture
        # - Synthetic fertilizers
        # - CH4 and N2O emissions from Other agricultural sources (burning of crop residues and savanna)
        # Please note that emissions associated with agriculture related energy use are reported under Energy sector, and thus not included here.
        "agriculture",
        # Land-Use Change and Forestry sector contains emissions from following activities:
        # - CO2 emissions from Forest land and Net forest conversion (forestland converted to cropland and grassland)
        # - CO2 emissions from Drained organic soils
        # - CO2 and CH4 emissions from Fires in organic soils
        # - CH4 and N2O emissions from Forest fires
        # Please note that the forest land emissions data reflects emissions from changes in forest land area between reported years of Forest Resource Assessment (FRA) submitted by countries. The data is published every 5 years, and emissions values are estimated by interpolating data over those 5-year periods.
        # Please note recent change of FAO's approach for reporting emissions from fires in organic soils (part of the “Burning Biomass”): only values from Southern-east Asia countries are included in country, regional and global aggregates (of burning biomass and subsequently land use total).
        "land_use_change_and_forestry",
        # Other fuel combustion subsector contains emissions from following activities:
        # - CO2, CH4, and N2O emissions from Agriculture/forestry, fishing, and other fuel consumption
        # Other fuel consumption includes emissions from military fuel use.
        "other_fuel_combustion",
    ],
    "Getting around": [
        # Transportation subsector contains CO2, CH4 and N2O emissions from following activities:
        # - Road
        # - Rail
        # - Domestic aviation
        # - Pipeline transport
        # - Domestic navigation
        # - Non-specified transport (all emissions from transport not specified elsewhere)
        # Please note that transport emissions for world total includes international marine bunkers and international aviation bunkers, which are not included in transportation at a national or regional level.
        "transport",
        # Bunker fuels contain CO2 emissions from international marine and aviation bunkers. The split of domestic and international are determined by the departure and landing locations, and not by the nationality of the ship/airline.
        # Bunker Fuels are shown as a sector, but excluded from national totals for Energy (including energy subsector Transport) and Total GHG emissions, in accordance with IPCC Guidelines. In other words, except at World level, Total GHG emissions (and accordingly Energy sector, and Transport sub-sector emissions) do not include bunker fuel emissions.
        # NOTE: We should not add aviation and shipping to transport. It is already included in World's transport, and it should be ignored at the country level.
        # "aviation_and_shipping",
    ],
    "Keeping warm and cool": [
        # Building subsector contains CO2, CH4 and N2O emissions from following activities:
        # - Residential
        # - Commercial and public services
        # Please note that only on-site fuel combustion is covered here. Emissions associated with use of electricity are reported under electricity/heat.
        "buildings",
    ],
    "Electricity": [
        # Electricity/heat subsector contains CO2, CH4 and N2O emissions from following activities:
        # - Main activity producer of electricity and heat (electricity plants, combined heat
        # and power plants, heat plants)
        # - Unallocated autoproducers
        # - Other energy industry own use
        # Please note that part of the emissions might be reallocated to industrial processes and product use category under the 2006 IPCC GLs.
        "electricity_and_heat",
    ],
    "Making things": [
        # Manufacturing/Construction subsector contains CO2, CH4 and N2O emissions from following activities:
        # - Mining and quarrying
        # - Construction
        # - Manufacturing
        # - Iron and Steel
        # - Chemical and petrochemical
        # - Non-ferrous metals
        # - Non-metallic minerals
        # - Transport equipment
        # - Machinery
        # - Food and tobacco
        # - Paper, pulp and printing
        # - Wood and wood products
        # - Textile and leather
        # - Non-specified industry
        # Please note that part of the emissions might be reallocated to industrial processes and product use category under the 2006 IPCC GLs.
        "manufacturing_and_construction",
        # Industry sector contains emissions from following activities:
        # - CO2 emissions from Cement Manufacture
        # - N2O emissions from Adipic and Nitric Acid Production
        # - F-Gases from Electronics Manufacturing (semiconductor, flat panel display (FPD) and photovoltaic (PV))
        # - SF6 from Electric Power Systems
        # - PFCs and SF6 from Metal Production (PFCs as by-product of aluminum production, SF6 from magnesium
        # production)
        # - HFCs from Uses of Substitutes for Ozone-Depleting Substances (ODS)
        # - HFCs from HCFC-22 Production
        # - N2O and CH4 emissions from Other Industrial activities (non-agriculture)
        # Please note that for the purpose of Climate Watch dataset, all fluorinated gases are reported as aggregated F-gas.
        "industry",
        # Fugitive Emissions subsector contains fugitive CO2 and CH4 emissions from following activities:
        # - CO2 from Flaring
        # - CH4 from Coal mining
        # - CH4 from Natural gas and oil systems
        # - Production
        # - Faring and venting
        # - Transmission and distribution
        # - CH4 and N2O from Other energy sources (solid fuels, oil and natural gas, incineration and open burning of waste)
        "fugitive",
        # Waste sector contains emissions from following activities:
        # - CH4 from Landfills (including industrial and municipal solid waste)
        # - CH4 and N2O from Wastewater treatment (rural and urban)
        # - CH4 and N2O from Other waste sources
        "waste",
    ],
}

# Create a list of expected sectors to be found in the data.
EXPECTED_SECTORS = sum(SECTOR_MAPPING.values(), [])


def sanity_check_inputs(tb_ghg, tb_co2):
    # List all columns sector emissions in the table of GHG emissions.
    columns_sectors = [
        column for column in tb_ghg.columns if "per_capita" not in column if column not in ["country", "year"]
    ]
    # Remove energy sector, since it's a group of subsectors; idem for bunker fuels and total columns.
    columns_sectors = [
        column
        for column in columns_sectors
        if column not in ["energy", "aviation_and_shipping", "total_excluding_lucf", "total_including_lucf"]
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

    # Get total emissions of electricity production from Ember.
    # I could get them directly from their yearly electricity data.
    # tb_ember = tb_ember[tb_ember["country"] == "World"][
    #     ["year", "emissions__lifecycle__total_emissions__mtco2"]
    # ].rename(columns={"emissions__lifecycle__total_emissions__mtco2": "emissions_electricity"})
    # However, Ember's emissions are given as lifecycle emissions, which we can't use here.
    # Instead, we'll calculate direct emissions ourselves, by converting total generation of coal, gas, and other fossil, into CO2 emissions, with some conversion factors.
    COLUMNS_EMBER = {
        "country": "country",
        "year": "year",
        "generation__coal__twh": "coal_generation",
        "generation__gas__twh": "gas_generation",
        "generation__other_fossil__twh": "oil_generation",
        # Just for sanity checking, keep original lifecycle emissions.
        "emissions__lifecycle__total_emissions__mtco2": "lifecycle_emissions",
    }
    tb_emissions = tb_ember[tb_ember["country"] == "World"][list(COLUMNS_EMBER)].rename(
        columns=COLUMNS_EMBER, errors="raise"
    )
    # NOTE: Instead of hardcoding these factors, I could get emission factors from the existing garden step.
    # Emission factors are given in g/kWh, which is the same as t/GWh; they need to be multiplied by 1e3 to convert to t/TWh (since generation is in TWh).
    tb_emissions["coal_emissions"] = tb_emissions["coal_generation"] * 760 * 1e3
    tb_emissions["gas_emissions"] = tb_emissions["gas_generation"] * 370 * 1e3
    tb_emissions["oil_emissions"] = tb_emissions["oil_generation"] * 279 * 1e3
    tb_emissions["emissions_electricity"] = tb_emissions[["coal_emissions", "gas_emissions", "oil_emissions"]].sum(
        axis=1
    )
    # Convert from million tonnes to tonnes of CO2.
    tb_emissions["lifecycle_emissions"] *= 1e6
    # Uncomment to compare Ember's original (lifecycle) emissions with the ones we just calculated.
    # px.line(tb_emissions[["year", "lifecycle_emissions", "emissions_electricity"]].melt(id_vars=["year"]), x="year", y="value", color="variable", markers=True).update_yaxes(range=[0, None])
    # Remove unnecessary columns.
    tb_emissions = tb_emissions.drop(
        columns=[
            "country",
            "coal_emissions",
            "gas_emissions",
            "oil_emissions",
            "coal_generation",
            "gas_generation",
            "oil_generation",
            "lifecycle_emissions",
        ],
        errors="raise",
    )

    # Add Ember's emissions to the original electricity and heat emissions table.
    tb_electricity_and_heat = tb_electricity_and_heat.merge(tb_emissions, on="year", how="inner")
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
