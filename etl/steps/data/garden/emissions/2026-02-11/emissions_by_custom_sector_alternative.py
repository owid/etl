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
from pathlib import Path
import pandas as pd

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Custom remapping of Climate Watch subsectors into our custom categories.
# See WRI's documentation for the meaning of each sector:
# https://wri-sites.s3.us-east-1.amazonaws.com/climatewatch.org/www.climatewatch.org/climate-watch/wri_metadata/CW_GHG_Method_Note.pdf
# Also see distribution of subsectors (as of 2026-02-11, the latest data is still for 2021!):
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
        "aviation_and_shipping",
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
        # - Faring and venting
        # - Transmission and distribution
        # - CH4 and N2O from Other energy sources (solid fuels, oil and natural gas, incineration and open burning of waste)
        "fugitive",
    ],
}

# Create a list of expected sectors to be found in the Climate Watch data.
SECTORS_CW = sum(SECTOR_CW_MAPPING.values(), [])

COLUMNS_UN = {
    "Country or Area": "country",
    "Year": "year",
    "Commodity - Transaction": "sector",
    "Unit": "unit",
    "Quantity": "value",
}

SECTOR_TITLES = {
    "industry": "Making things",
    "agriculture": "Growing food",
    "buildings": "Powering and heating buildings",
    "transport": "Getting around",
    "other": "Other emissions",
    "electricity": "Electricity",
}
SECTORS = sorted(SECTOR_TITLES)
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
SECTORS_UN = sum(SECTOR_UN_MAPPING.values(), [])

COLUMN_UN_FINAL_ENERGY = "Final energy consumption"


def sanity_check_inputs(tb_ghg):
    # List all columns sector emissions in the table of GHG emissions.
    columns_sectors = [
        column for column in tb_ghg.columns if "per_capita" not in column if column not in ["country", "year"]
    ]
    # Remove energy sector, since it's a group of subsectors; idem for total columns.
    columns_sectors = [
        column for column in columns_sectors if column not in ["energy", "total_excluding_lucf", "total_including_lucf"]
    ]
    error = "Unexpected list of sectors."
    assert set(columns_sectors) == set(SECTORS_CW), error

    # Ensure that the sum of all sector emissions yields the total of emissions (within a few percent).
    tb_ghg["sum"] = tb_ghg[columns_sectors].sum(axis=1)
    # NOTE: It seems that the sum is systematically a few percent higher than the total.
    # This may be a problem in the original data (possibly due to rounding).
    error = "Sum of emissions differs from total more than a few percent"
    assert (
        (100 * abs(tb_ghg["sum"] - tb_ghg["total_including_lucf"]) / tb_ghg["total_including_lucf"]) < 4
    ).all(), error
    # Uncomment to compare visually.
    # import plotly.express as px
    # px.line(tb_ghg[["year", "sum", "total_including_lucf"]].melt(id_vars=["year"]), x="year", y="value", color="variable", markers=True).show()


def run() -> None:
    #
    # Load inputs.
    #
    # Load Climate Watch's emissions by sector and read its table on GHG emissions.
    ds = paths.load_dataset("emissions_by_sector")
    tb_ghg = ds.read("greenhouse_gas_emissions_by_sector")

    # Load UN energy statistics database, and read its main table.
    ds_un = paths.load_dataset("energy_statistics_database")
    tb_un = ds_un.read("energy_statistics_database")

    #
    # Process data.
    #
    # Select only global data.
    tb_ghg = tb_ghg[tb_ghg["country"] == "World"].reset_index(drop=True)

    # Rename columns in Climate Watch table, for convenience.
    tb_ghg = tb_ghg.rename(
        columns={column: column.replace("_ghg_emissions", "") for column in tb_ghg.columns}, errors="raise"
    )

    # Sanity checks.
    sanity_check_inputs(tb_ghg=tb_ghg)

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
    # TODO: Make a function.
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

    # For convenience, adapt units, from GWh to TWh.
    tb_un["value"] *= 1e-3
    tb_un = tb_un.drop(columns=["unit"], errors="raise")

    # TODO: The final year is clearly incomplete (it drops suddenly). Assert this drop in the data and then remove the latest year.
    tb_un = tb_un[tb_un["year"] < tb_un["year"].max()].reset_index(drop=True)
    # TODO: Instead of selecting global emissions and global shares and then combining, it would be more accurate to calculate shares at the country level, get their emissions, and then add them up. But this involves harmonization and a few more subtleties.
    # For now, work at the global level.
    tb_un = tb_un.groupby(["year", "sector"], as_index=False).agg({"value": "sum"})

    # Create shares of final electricity consumption by custom sectors.
    # TODO: Fix missing origins in the following operation.
    tb_un = tb_un.rename(columns={"value": ""}).pivot(index=["year"], columns=["sector"], join_column_levels_with="")
    tb_un = tb_un.rename(columns={COLUMN_UN_FINAL_ENERGY: "total"}, errors="raise")
    for sector, subsectors in SECTOR_UN_MAPPING.items():
        tb_un[sector] = tb_un[subsectors].sum(axis=1) / tb_un["total"]
    # Keep only new aggregate sector columns.
    tb_un = tb_un[["year"] + list(SECTOR_UN_MAPPING)].assign(**{"country": "World"})

    # For convenience, transpose table.
    tb_un = tb_un.melt(id_vars=["country", "year"], value_name="electricity_share", var_name="sector")

    # Combine both tables.
    tb = tb_ghg.merge(tb_un, on=["country", "year", "sector"], how="outer")

    # Add total emissions from electricity as a new column.
    tb = tb.merge(
        tb[tb["sector"] == "electricity"][["year", "ghg_emissions"]],
        on=["year"],
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
    # TODO: Make a function.
    assert set(tb.dropna(subset="ghg_emissions_direct")["sector"]) == set(SECTORS)
    assert set(tb.dropna(subset="ghg_emissions_indirect")["sector"]) == set(SECTORS) - set(["electricity"])
    assert set(tb.dropna(subset="ghg_emissions")["sector"]) == set(SECTORS) - set(["electricity"])

    # Rename sectors using the custom titles.
    from owid.datautils.dataframes import map_series

    tb["sector"] = map_series(
        tb["sector"], mapping=SECTOR_TITLES, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )

    # Add an explanation to the metadata of which subsectors are included in each category.
    description_processing = "Each category is made up of the following emission subcategories, based on IPCC definitions (as reported by Climate Watch):"
    for sector, subsectors in SECTOR_CW_MAPPING.items():
        _subsectors = (
            ", ".join([s.replace("fugitive", "fugitive emissions").replace("_", " ") for s in subsectors]) + "."
        )
        description_processing += f"\n- {sector}: {_subsectors}"
    for column in ["ghg_emissions", "ghg_emissions_direct", "ghg_emissions_indirect"]:
        tb[column].metadata.description_processing = description_processing
        tb[column].metadata.description_processing = description_processing

    # Improve table format.
    tb = tb.format(keys=["country", "year", "sector"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
