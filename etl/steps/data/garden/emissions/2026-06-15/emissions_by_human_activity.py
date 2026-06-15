"""Greenhouse gas emissions by human activity.

This step produces two complementary tables, each categorizing emissions by the end use of human
activities, but treating electricity differently.

1. ``emissions_by_human_activity`` (electricity shown as its own activity).
   Global emissions split into a custom list of broad activities:
   - Growing food
   - Getting around
   - Keeping warm and cool
   - Electricity
   - Making things
   This table uses WRI's Climate Watch data (emissions by sector). The "Electricity" activity keeps
   electricity and heat together, exactly as Climate Watch reports them in its "Electricity/Heat" sector.

2. ``emissions_by_human_activity_including_electricity`` (electricity distributed across activities).
   Country-level emissions split into IPCC (AR6 WG3) style sectors:
   - agriculture, transport, buildings, industry, other
   Here electricity is not shown as a separate activity; instead, electricity emissions are
   reallocated as indirect emissions to the sectors that consume the electricity, using UNdata's
   Energy Statistics Database (final electricity consumption by sector).

The ideal data for these custom categories comes from the IEA, but it is under a heavy paywall, so
both tables are built from publicly available sources as an approximation.

This IPCC chart helps understand the logic behind the second method:
https://www.ipcc.ch/report/ar6/wg3/downloads/figures/IPCC_AR6_WGIII_Figure_2_12.png
"""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


########################################################################################################################
# Table 1: emissions_by_human_activity (electricity shown as its own activity).
########################################################################################################################

# Custom remapping of Climate Watch subsectors into our custom categories.
# See WRI's documentation for the meaning of each sector:
# https://files.wri.org/d8/s3fs-public/2024-05/climate-watch-country-greenhouse-gas-emissions-data-methodology.pdf?VersionId=1geU96keSmqZlUjv41FNGB4CLpHQbruN
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
        # - Flaring and venting
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


def build_emissions_by_human_activity(ds_cw):
    # Read Climate Watch's tables on CO2 and GHG emissions.
    tb_co2 = ds_cw.read("carbon_dioxide_emissions_by_sector")
    tb_ghg = ds_cw.read("greenhouse_gas_emissions_by_sector")

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
    description_processing = "Each category is made up of the following emission subcategories, based on IPCC definitions (as reported by Climate Watch):"
    for sector, subsectors in SECTOR_MAPPING.items():
        _subsectors = (
            ", ".join([s.replace("fugitive", "fugitive emissions").replace("_", " ") for s in subsectors]) + "."
        )
        description_processing += f"\n- {sector}: {_subsectors}"
    for column in ["co2_emissions", "ghg_emissions"]:
        tb[column].metadata.description_processing = description_processing

    # Improve table format.
    tb = tb.format(keys=["country", "year", "sector"], short_name="emissions_by_human_activity")

    return tb


########################################################################################################################
# Table 2: emissions_by_human_activity_including_electricity (electricity distributed across activities).
########################################################################################################################

# Custom remapping of Climate Watch subsectors into intermediate sectors.
# We added (in comments) the description of each sector, taken from WRI's documentation:
# https://files.wri.org/d8/s3fs-public/2024-05/climate-watch-country-greenhouse-gas-emissions-data-methodology.pdf?VersionId=1geU96keSmqZlUjv41FNGB4CLpHQbruN
# You can also see the distribution of subsectors (as of 2026-02-11, the latest data is still for 2021) here:
# https://www.wri.org/data/world-greenhouse-gas-emissions-sector-2021-sunburst-chart
SECTOR_CW_MAPPING = {
    "agriculture": [
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
    ],
    "transport": [
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
    "buildings": [
        # Building subsector contains CO2, CH4 and N2O emissions from following activities:
        # - Residential
        # - Commercial and public services
        # Please note that only on-site fuel combustion is covered here. Emissions associated with use of electricity are reported under electricity/heat.
        "buildings",
    ],
    "electricity": [
        # Electricity/heat subsector contains CO2, CH4 and N2O emissions from following activities:
        # - Main activity producer of electricity and heat (electricity plants, combined heat
        # and power plants, heat plants)
        # - Unallocated autoproducers
        # - Other energy industry own use
        # Please note that part of the emissions might be reallocated to industrial processes and product use category under the 2006 IPCC GLs.
        "electricity_and_heat",
    ],
    "industry": [
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
    ],
    "other": [
        # Other fuel combustion subsector contains emissions from following activities:
        # - CO2, CH4, and N2O emissions from Agriculture/forestry, fishing, and other fuel consumption
        # Other fuel consumption includes emissions from military fuel use.
        "other_fuel_combustion",
        # Waste sector contains emissions from following activities:
        # - CH4 from Landfills (including industrial and municipal solid waste)
        # - CH4 and N2O from Wastewater treatment (rural and urban)
        # - CH4 and N2O from Other waste sources
        "waste",
        # Fugitive Emissions subsector contains fugitive CO2 and CH4 emissions from following activities:
        # - CO2 from Flaring
        # - CH4 from Coal mining
        # - CH4 from Natural gas and oil systems
        # - Production
        # - Flaring and venting
        # - Transmission and distribution
        # - CH4 and N2O from Other energy sources (solid fuels, oil and natural gas, incineration and open burning of waste)
        "fugitive",
    ],
}

# List of expected sectors to be found in the Climate Watch data.
SECTORS_CW = sum(SECTOR_CW_MAPPING.values(), [])

# Mapping of UNdata Energy Statistics Database sectors of final electricity consumption and our list of final sectors.
SECTOR_UN_MAPPING = {
    "industry": ["Consumption by manufacturing, construction and non-fuel industry"],
    "agriculture": ["Consumption by agriculture, forestry and fishing"],
    "transport": ["Consumption by transport"],
    "buildings": [
        "Consumption by households",
        "Consumption by commerce and public services",
    ],
    "other": ["Consumption not elsewhere specified (other)"],
}

# List of UNdata sectors.
SECTORS_UN = sum(SECTOR_UN_MAPPING.values(), [])

# Name of UNdata sector for final energy consumption (we check that it coincides with the sum of final electricity consumption sectors).
COLUMN_UN_FINAL_ENERGY = "Final energy consumption"


def sanity_check_cw_data(tb_ghg):
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
    assert set(columns_sectors) == set(SECTORS_CW), error

    # Ensure that the sum of all sector emissions yields the total of emissions (within a few percent).
    _tb = tb_ghg[tb_ghg["country"] == "World"].reset_index(drop=True)
    _tb["sum"] = _tb[columns_sectors].sum(axis=1)
    # NOTE: It seems that the sum is systematically a few percent higher than the total.
    # This may be a problem in the original data (possibly due to rounding).
    error = "Sum of emissions differs from total more than a few percent"
    assert ((100 * abs(_tb["sum"] - _tb["total_including_lucf"]) / _tb["total_including_lucf"]) < 4).all(), error
    # Uncomment to compare visually.
    # import plotly.express as px
    # px.line(_tb[["year", "sum", "total_including_lucf"]].melt(id_vars=["year"]), x="year", y="value", color="variable", markers=True).update_yaxes(range=[0, None]).show()


def sanity_check_un_data(tb_un):
    error = "Unexpected units."
    assert set(tb_un["unit"]) == {"Gigawatt-hours"}, error
    # Ensure the total final energy consumption equals the sum of all sectors.
    tb_un_sum = tb_un[tb_un["sector"].isin(SECTORS_UN)].groupby("year", as_index=False).agg({"value": "sum"})
    tb_un_total = tb_un[tb_un["sector"] == COLUMN_UN_FINAL_ENERGY].groupby("year", as_index=False).agg({"value": "sum"})
    tb_compared = tb_un_sum.merge(tb_un_total, on=["year"], how="inner", suffixes=("", "_sum"))
    error = "Expected UN's final energy consumption to agree with the sum of the electricity consumption of each sector, within 0.01%."
    assert (100 * (abs(tb_compared["value"] - tb_compared["value_sum"]) / tb_compared["value"]) < 0.01).all(), error
    # Uncomment to show total final energy consumption compared to the sum of the electricity consumption of each sector.
    # import plotly.express as px
    # px.line(pd.concat([tb_un_total.assign(**{"source": "total"}), tb_un_sum.assign(**{"source": "sum"})]), x="year", y="value", color="source", markers=True)


def fix_limited_data_coverage(tb_un):
    # The data coverage of the final year in the data is clearly incomplete; one can see that global data drops suddenly.
    tb_un_totals = (
        tb_un[tb_un["sector"] == COLUMN_UN_FINAL_ENERGY]
        .groupby("year", as_index=False)
        .agg({"value": "sum"})
        .sort_values("year")
    )
    latest_value = tb_un_totals["value"].iloc[-1]
    previous_value = tb_un_totals["value"].iloc[-2]
    error = "Expected a sharp drop in the latest year of UN final energy data (likely incomplete coverage)."
    assert latest_value < 0.9 * previous_value, error

    # Drop the latest year in the data.
    tb_un = tb_un[tb_un["year"] < tb_un["year"].max()].reset_index(drop=True)

    return tb_un


def fix_issue_with_switzerland_liechtenstein(tb_ghg, tb_un):
    # UNdata has data for Switzerland-Liechtenstein, as well as for Liechtenstein.
    # However, the latter country only has data for sector "other", and the full electricity share goes there.
    error = "Expected Switzerland to be missing in UNdata (since it contains Switzerland-Liechtenstein)."
    assert set(tb_ghg["country"]) - set(tb_un["country"]) == {"Switzerland"}, error
    error = "Expected Liechtenstein to only have data for 'other' sector, with the full share of electricity."
    liech_nonzero = tb_un[(tb_un["country"] == "Liechtenstein") & (tb_un["electricity_share"] > 0)]
    assert (liech_nonzero["sector"] == "other").all() and (liech_nonzero["electricity_share"] == 1).all(), error
    # I don't understand the details of this split.
    # For the purpose of this step (where we calculate shares of electricity) for now we can simply assume that the shares of final electricity consumption of both countries are the same.
    # So, I'll drop Liechtenstein data, then rename "Switzerland-Liechtenstein" -> "Switzerland", and then repeat Switzerland's data, and assign it to Liechtenstein.
    tb_un.loc[tb_un["country"] == "Switzerland-Liechtenstein", "country"] = "Switzerland"
    tb_un = pr.concat(
        [
            tb_un[tb_un["country"] != "Liechtenstein"],
            tb_un[tb_un["country"] == "Switzerland"].assign(**{"country": "Liechtenstein"}),
        ],
        ignore_index=True,
    )

    return tb_un


def sanity_check_outputs(tb):
    assert set(tb.dropna(subset="ghg_emissions_direct")["sector"]) == set(SECTOR_UN_MAPPING) | set(["electricity"])
    assert set(tb.dropna(subset="ghg_emissions_indirect")["sector"]) == set(SECTOR_UN_MAPPING)
    assert set(tb.dropna(subset="ghg_emissions")["sector"]) == set(SECTOR_UN_MAPPING)


def build_emissions_by_human_activity_including_electricity(ds_cw, ds_un):
    # Read Climate Watch's table on GHG emissions.
    tb_ghg = ds_cw.read("greenhouse_gas_emissions_by_sector")

    # Read UN energy statistics database main table.
    tb_un = ds_un.read("energy_statistics_database")

    # Rename columns in Climate Watch table, for convenience.
    tb_ghg = tb_ghg.rename(
        columns={column: column.replace("_ghg_emissions", "") for column in tb_ghg.columns}, errors="raise"
    )

    # Sanity check Climate Watch data.
    sanity_check_cw_data(tb_ghg=tb_ghg)

    # Keep only columns of sector emissions.
    tb_ghg = tb_ghg[["country", "year"] + SECTORS_CW]

    # Create aggregates of custom sectors for GHG emissions.
    for sector, subsectors in SECTOR_CW_MAPPING.items():
        tb_ghg[sector] = tb_ghg[subsectors].sum(axis=1)
    # Keep only new aggregate sector columns.
    tb_ghg = tb_ghg[["country", "year"] + list(SECTOR_CW_MAPPING)]

    # For convenience, transpose table.
    tb_ghg = tb_ghg.melt(id_vars=["country", "year"], value_name="ghg_emissions", var_name="sector")

    # Select relevant UN commodity.
    tb_un = tb_un[(tb_un["commodity"] == "Total Electricity")].drop(columns=["commodity"]).reset_index(drop=True)
    error = "Expected UN energy sectors not found."
    assert set(SECTORS_UN) < set(tb_un["transaction"]), error
    # Select relevant UN transactions.
    tb_un = (
        tb_un[(tb_un["transaction"].isin([COLUMN_UN_FINAL_ENERGY] + SECTORS_UN))]
        .rename(columns={"transaction": "sector"}, errors="raise")
        .reset_index(drop=True)
    )

    # Sanity checks.
    sanity_check_un_data(tb_un=tb_un)

    # For convenience, adapt units, from GWh to TWh.
    tb_un["value"] *= 1e-3
    tb_un = tb_un.drop(columns=["unit"], errors="raise")

    # Fix limited data coverage.
    tb_un = fix_limited_data_coverage(tb_un=tb_un)

    # Create shares of final electricity consumption by custom sectors.
    tb_un = tb_un.pivot(index=["country", "year"], columns=["sector"], join_column_levels_with="_")
    tb_un = tb_un.rename(columns={column: column.replace("value_", "") for column in tb_un.columns}, errors="raise")
    tb_un = tb_un.rename(columns={COLUMN_UN_FINAL_ENERGY: "total"}, errors="raise")
    for sector, subsectors in SECTOR_UN_MAPPING.items():
        tb_un[sector] = tb_un[subsectors].sum(axis=1) / tb_un["total"]
    # Keep only new aggregate sector columns.
    tb_un = tb_un[["country", "year"] + list(SECTOR_UN_MAPPING)]

    # For convenience, transpose table.
    tb_un = tb_un.melt(id_vars=["country", "year"], value_name="electricity_share", var_name="sector")

    # Fix known issue with Switzerland-Liechtenstein.
    tb_un = fix_issue_with_switzerland_liechtenstein(tb_ghg=tb_ghg, tb_un=tb_un)

    # Combine both tables.
    tb = tb_ghg.merge(tb_un, on=["country", "year", "sector"], how="outer")

    # Add total emissions from electricity as a new column.
    tb = tb.merge(
        tb[tb["sector"] == "electricity"][["country", "year", "ghg_emissions"]],
        on=["country", "year"],
        how="left",
        suffixes=("", "_electricity_total"),
    )

    # Rename existing column of emissions as "direct emissions".
    tb = tb.rename(columns={"ghg_emissions": "ghg_emissions_direct"}, errors="raise")

    # Calculate indirect emissions from electricity in each sector.
    tb["ghg_emissions_indirect"] = tb["electricity_share"] * tb["ghg_emissions_electricity_total"]

    # Create a new column of total emissions, which includes direct sector emissions and indirect emissions from electricity.
    tb["ghg_emissions"] = tb["ghg_emissions_direct"] + tb["ghg_emissions_indirect"]

    # Drop unnecessary columns.
    tb = tb.drop(columns=["electricity_share", "ghg_emissions_electricity_total"], errors="raise")

    # Sanity check outputs.
    sanity_check_outputs(tb=tb)

    # Improve table format.
    tb = tb.format(keys=["country", "year", "sector"], short_name="emissions_by_human_activity_including_electricity")

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load Climate Watch's emissions by sector.
    ds_cw = paths.load_dataset("emissions_by_sector")

    # Load UN energy statistics database.
    ds_un = paths.load_dataset("energy_statistics_database")

    #
    # Process data.
    #
    # Build the table where electricity is shown as its own activity.
    tb = build_emissions_by_human_activity(ds_cw=ds_cw)

    # Build the table where electricity emissions are distributed across the activities that consume them.
    tb_including_electricity = build_emissions_by_human_activity_including_electricity(ds_cw=ds_cw, ds_un=ds_un)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_including_electricity])
    ds_garden.save()
