"""
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

from owid.catalog import Table
from owid.datautils.io import load_json

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Year that Climate Watch data refers to.
YEAR = 2021


# Custom remapping of Climate Watch subsectors into our custom categories.
CUSTOM_MAPPING = {
    "Growing food": [
        # 5.9% — Enteric fermentation and manure management are core farm sources of CH4 (and some N2O).
        ("Agriculture", "Livestock and manure", "Methane from enteric fermentation and CH4/N2O from manure handling."),
        # 4.1% — Directly tied to fertilizing and managing cropland/grassland soils.
        (
            "Agriculture",
            "Agriculture soils",
            "N2O from synthetic/organic fertilizers, manure applied to soils, crop residues, and soil processes.",
        ),
        # 1.2% — A classic agricultural source (anaerobic decomposition in flooded fields).
        ("Agriculture", "Rice cultivation", "Methane released from flooded rice paddies (anaerobic decomposition)."),
        # 0.5% — Open burning of agricultural residues.
        ("Agriculture", "Burning", "Open burning of agricultural residues (emits CH4 and N2O)."),
        # 0.9% — Tractor/irrigation/vessel fuel is farm (and fishing) activity.
        (
            "Other fuel combustion",
            "Agriculture and fishing energy use",
            "On-farm and fishing fuel use for tractors, pumps, irrigation, and vessels (non-electric).",
        ),
        # 1.7% — Peat/organic soil drainage/cultivation is strongly linked to agricultural expansion and use.
        (
            "Land-use change and forestry",
            "Drained organic soils",
            "CO2 (and some N2O) from drained/cultivated organic soils (e.g., peatlands).",
        ),
        # 0.0% — Peat/organic soil fires are typically tied to land clearance/use.
        (
            "Land-use change and forestry",
            "Fires in organic soils",
            "Emissions from peat/organic soil fires (often large CH4 and CO2 pulses).",
        ),
        # 1.0% — Electricity for pumps, cold chains, hatcheries, etc. is part of primary food production demand (counted here by purpose).
        (
            "Electricity and heat",
            "Agriculture and fishing energy use",
            "Power/steam used by agriculture and fisheries (allocated by end-use rather than by the power sector).",
        ),
        # Cross-category, related but not exclusively food-driven:
        # 0.6% — Mostly driven by agricultural expansion (pasture/cropland), but also logging/settlements.
        (
            "Land-use change and forestry",
            "Forest land",
            "Net CO2 from forest-land change/management. Note: this subsector is largely driven by agricultural expansion (cropland/pasture), but also logging and settlement expansion.",
        ),
        # 0.4% — Many fires are linked to clearing for agriculture, but not all.
        (
            "Land-use change and forestry",
            "Forest fires",
            "Fires on forest land. Note: this subsector is often linked to land clearing for agriculture, but not exclusively.",
        ),
        # 2.0% — Landfill CH4 largely from organics/food waste, but includes paper/wood and non-food streams.
        (
            "Waste",
            "Landfills",
            "Methane from the decomposition of solid waste in landfills. Note: much of this comes from food and other organic waste, but it also includes paper, wood, and non-food materials.",
        ),
        # 1.3% — Wastewater CH4/N2O from domestic + industrial sources; substantial share is food-related organics.
        (
            "Waste",
            "Wastewater",
            "Methane and nitrous oxide from the treatment and discharge of domestic and industrial wastewater. Note: a significant share is linked to food-related organic matter, but not exclusively.",
        ),
    ],
    "Getting around": [
        # 12.1% — Road fuels.
        (
            "Transportation",
            "Road",
            "Road vehicles using liquid/gaseous fuels (tailpipe emissions; EV electricity counted under power unless reallocated).",
        ),
        # 0.7% — Domestic aviation fuels.
        ("Transportation", "Air", "Domestic aviation fuel combustion within national inventories."),
        # 0.3% — Domestic navigation.
        ("Transportation", "Ship", "Domestic navigation (inland/coastal) fuel combustion within national inventories."),
        # 0.3% — Fuels for pipeline compressors/pumps.
        ("Transportation", "Pipeline", "Fuel used to power pipeline transport (e.g., compressor stations and pumps)."),
        # 0.2% — Diesel rail traction.
        (
            "Transportation",
            "Rail",
            "Rail traction fuel use (diesel rail; electric rail demand reallocated here by purpose).",
        ),
        # 0.1% — Miscellaneous transport fuels.
        ("Transportation", "Other transportation", "Miscellaneous smaller transport categories not listed elsewhere."),
        # 1.3% — International shipping.
        ("International bunker", "Ship", "International marine bunkers (fuel for international shipping)."),
        # 0.7% — International aviation.
        ("International bunker", "Air", "International aviation bunkers (fuel for international flights)."),
        # 0.3% — Electricity used by rail systems (reallocated from power to transport by purpose).
        ("Electricity and heat", "Rail", "Electricity used to power rail systems (allocated by transport end-use)."),
        # 0.1% — Electricity used in road transport (EV charging, small share).
        (
            "Electricity and heat",
            "Road",
            "Electricity used in road transport (EV charging and related; allocated by transport end-use).",
        ),
        # 0.0% — Electricity for pipeline compressors/pumps.
        (
            "Electricity and heat",
            "Pipeline",
            "Electricity used to power pipeline compressors/pumps (allocated by transport end-use).",
        ),
        # 0.0% — Other electric transport uses.
        (
            "Electricity and heat",
            "Other transportation",
            "Miscellaneous electricity used by transport (allocated by transport end-use).",
        ),
    ],
    "Keeping warm and cool": [
        # 5.0% — Onsite fuels in homes (space/water heating, cooking when not electric).
        (
            "Buildings",
            "Residential buildings",
            "Direct onsite fuel use in homes (space/water heating, cooking when not electric).",
        ),
        # 1.6% — Onsite fuels in commercial buildings (space/water heating, cooking).
        (
            "Buildings",
            "Commercial buildings",
            "Direct onsite fuel use in commercial buildings (space/water heating, cooking).",
        ),
        # 3.5% — Generic stationary fuel use not allocated elsewhere; mostly heat/boilers.
        (
            "Other fuel combustion",
            "Unallocated fuel combustion",
            "Stationary fuel use not allocated elsewhere (generic boilers/heating and similar uses).",
        ),
    ],
    "Electricity": [
        # 7.5% — Household electricity/heat demand (kept in electricity to reflect production-based accounting).
        (
            "Electricity and heat",
            "Residential buildings",
            "Emissions from generating electricity and central heat for residential buildings. Note: while much of this power is used for appliances, lighting, and other non-heating needs, part also supports heating and cooling, which overlaps with 'Keeping warm and cool'.",
        ),
        # 4.8% — Commercial electricity/heat demand.
        (
            "Electricity and heat",
            "Commercial buildings",
            "Emissions from generating electricity and central heat for commercial buildings. Note: while much of this power is used for appliances, lighting, and other non-heating needs, part also supports heating and cooling, which overlaps with 'Keeping warm and cool'.",
        ),
        # 4.4% — Upstream venting in oil/gas/coal supply (supports power and other fuel uses).
        (
            "Fugitive emissions",
            "Vented",
            "Intentional venting (mostly CH4) from fossil supply chains; kept with electricity as part of the energy system.",
        ),
        # 1.0% — Upstream flaring in oil/gas production.
        (
            "Fugitive emissions",
            "Flared",
            "Flaring of associated gas in fossil supply; kept with electricity as part of the energy system.",
        ),
        # 0.7% — Upstream production leaks.
        ("Fugitive emissions", "Production", "Fugitive releases from fossil fuel production/processing."),
        # 0.4% — Gas leakage in transmission and distribution.
        (
            "Fugitive emissions",
            "Transmission and distribution",
            "Fossil gas leakage during transmission and distribution.",
        ),
        # 0.1% — Small unallocated stationary fuel slice in energy operations.
        ("Fugitive emissions", "Unallocated fuel combustion", "Small residual in fossil energy operations."),
        # Cross-category:
        # 2.8% — Power/heat generation not allocated to an end-use in the source split.
        (
            "Electricity and heat",
            "Unallocated fuel combustion",
            "Emissions from electricity and heat generation that could not be assigned to a specific end-use sector in the original dataset. Note: this is a residual category, not a distinct activity.",
        ),
        # 0.1% — SF6 and related F-gases from grid equipment. Even though IPCC places it under Industrial processes, in practice these emissions occur because we run an electricity system.
        (
            "Industrial processes",
            "Electric power systems",
            "Emissions of fluorinated gases (mainly SF₆) from transmission and distribution equipment such as switchgear and circuit breakers. Note: although IPCC classifies this under industrial processes, we include it here because it arises from running the electricity grid.",
        ),
    ],
    "Making things": [
        # Direct onsite fuel use in industry/construction (not purchased electricity/heat):
        # 4.3%
        (
            "Manufacturing and construction",
            "Iron and steel",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat).",
        ),
        # 2.4%
        (
            "Manufacturing and construction",
            "Other industry",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat).",
        ),
        # 2.3%
        (
            "Manufacturing and construction",
            "Non-metallic minerals",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat).",
        ),
        # 1.5%
        (
            "Manufacturing and construction",
            "Chemical and petrochemical",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat).",
        ),
        # 0.4%
        (
            "Manufacturing and construction",
            "Non-ferrous metals",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat).",
        ),
        # 0.3%
        (
            "Manufacturing and construction",
            "Construction",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat).",
        ),
        # 0.3%
        (
            "Manufacturing and construction",
            "Mining and quarrying",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat).",
        ),
        # 0.3%
        (
            "Manufacturing and construction",
            "Paper, pulp and printing",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat).",
        ),
        # 0.2%
        (
            "Manufacturing and construction",
            "Machinery",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat).",
        ),
        # 0.1%
        (
            "Manufacturing and construction",
            "Textile and leather",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat).",
        ),
        # 0.1%
        (
            "Manufacturing and construction",
            "Transport equipment",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat).",
        ),
        # 0.0%
        (
            "Manufacturing and construction",
            "Wood and wood products",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat).",
        ),
        # Process (non-energy) industrial emissions:
        # 3.4%
        (
            "Industrial processes",
            "Cement",
            "Process CO2 from clinker production (calcination), independent of fuel use.",
        ),
        # 2.6%
        (
            "Industrial processes",
            "Chemical and petrochemical (ip)",
            "Non-energy process emissions from chemical/petrochemical production.",
        ),
        # 0.1%
        ("Industrial processes", "Other industry (ip)", "Other non-energy industrial process emissions."),
        # 0.1%
        ("Industrial processes", "Electronics (ip)", "Process and F-gas emissions from electronics manufacturing."),
        # 0.1%
        (
            "Industrial processes",
            "Non-ferrous metals (ip)",
            "Process emissions in non-ferrous metals (e.g., PFCs from aluminum).",
        ),
        # Electricity/heat used by industry (allocated here by purpose: making products):
        # 2.3%
        (
            "Electricity and heat",
            "Chemical and petrochemical",
            "Power/steam used by chemicals manufacturing (allocated by industrial end-use).",
        ),
        # 2.2%
        (
            "Electricity and heat",
            "Other industry",
            "Power/steam used by other industries (allocated by industrial end-use).",
        ),
        # 1.8%
        (
            "Electricity and heat",
            "Iron and steel",
            "Power/steam used by iron and steel (allocated by industrial end-use).",
        ),
        # 1.4%
        (
            "Electricity and heat",
            "Non-ferrous metals",
            "Power/steam used by non-ferrous metals (allocated by industrial end-use).",
        ),
        # 1.4%
        (
            "Electricity and heat",
            "Machinery",
            "Power/steam used by machinery manufacturing (allocated by industrial end-use).",
        ),
        # 0.8%
        (
            "Electricity and heat",
            "Non-metallic minerals",
            "Power/steam used by non-metallic minerals (allocated by industrial end-use).",
        ),
        # 0.5%
        (
            "Electricity and heat",
            "Textile and leather",
            "Power/steam used by textiles & leather (allocated by industrial end-use).",
        ),
        # 0.5%
        (
            "Electricity and heat",
            "Mining and quarrying",
            "Power/steam used by mining & quarrying (allocated by industrial end-use).",
        ),
        # 0.5%
        (
            "Electricity and heat",
            "Paper, pulp and printing",
            "Power/steam used by paper, pulp & printing (allocated by industrial end-use).",
        ),
        # 0.4%
        (
            "Electricity and heat",
            "Transport equipment",
            "Power/steam used by transport equipment manufacturing (allocated by industrial end-use).",
        ),
        # 0.3%
        (
            "Electricity and heat",
            "Construction",
            "Electricity used by construction activities (allocated by industrial end-use).",
        ),
        # 0.1%
        (
            "Electricity and heat",
            "Wood and wood products",
            "Power/steam used by wood products (allocated by industrial end-use).",
        ),
        # --- Waste not clearly food-dominant (kept with industry/system management) ---
        # 0.1%
        (
            "Waste",
            "Other waste",
            "Other waste emissions (e.g., open burning, composting, incineration without energy recovery).",
        ),
        # Cross-category, related to Growing food.
        # 0.8%
        (
            "Electricity and heat",
            "Food and tobacco",
            "Emissions from energy use in food and tobacco processing industries. Note: although related to food, these emissions come from manufacturing and processing rather than primary food production.",
        ),
        # 0.4%
        (
            "Manufacturing and construction",
            "Food and tobacco",
            "Direct onsite fuel combustion in industry/construction (excludes purchased electricity/heat). Note: although related to food, these emissions come from manufacturing and processing rather than primary food production.",
        ),
    ],
}


# Sanity checks.
def sanity_check_inputs(subsectors):
    # Check that shares add up to ~100%.
    def deep_sum(d):
        return sum(deep_sum(v) if isinstance(v, dict) else v for v in d.values())

    error = "Share of GHG emissions from all subsectors don't add up to >99%"
    assert deep_sum(subsectors) > 99, error

    error = "Share of GHG emissions in custom mapping don't add up to >99%"
    assert (
        sum(sum(subsectors[toplevel][leaf] for toplevel, leaf, _ in items) for items in CUSTOM_MAPPING.values()) > 99
    ), error

    # Check that all subsectors are included in the custom mapping by comparing unique values.
    def leaves_values(d):
        for v in d.values():
            if isinstance(v, dict):
                yield from leaves_values(v)
            else:
                yield v

    all_vals = set(leaves_values(subsectors))
    mapped_vals = {subsectors[toplevel][leaf] for items in CUSTOM_MAPPING.values() for (toplevel, leaf, _desc) in items}
    assert all_vals == mapped_vals, f"Missing: {all_vals - mapped_vals}, Extra: {mapped_vals - all_vals}"


def fix_rounding_issues(tb_custom: Table) -> Table:
    # Round numbers to 1 decimal (this may actually not be needed, as all numbers are already rounded this way).
    tb_custom["share_of_global_ghg_emissions"] = tb_custom["share_of_global_ghg_emissions"].round(1)
    # Due to rounding, numbers won't add up to 100%. Find the residual, ensure it's small, and add it to the largest category (where it's contribution will be smallest).
    residual = 100 - tb_custom["share_of_global_ghg_emissions"].sum()
    error = "Expected residual error to be smaller."
    assert residual < 0.5, error
    tb_custom.loc[
        tb_custom.sort_values("share_of_global_ghg_emissions", ascending=False).head(1).index,
        "share_of_global_ghg_emissions",
    ] += residual
    error = "Unsuccessful correction for rounding error."
    assert tb_custom["share_of_global_ghg_emissions"].sum() == 100, error

    return tb_custom


# Just to be able to compare the Climate Watch data with the numbers reported on the WRI website.
DATA_TO_SECTOR_NAMING = {
    "agriculture_ghg_emissions": "Agriculture",
    "aviation_and_shipping_ghg_emissions": "International bunker",
    "buildings_ghg_emissions": "Buildings",
    "electricity_and_heat_ghg_emissions": "Electricity and heat",
    "fugitive_ghg_emissions": "Fugitive emissions",
    "industry_ghg_emissions": "Industrial processes",
    "land_use_change_and_forestry_ghg_emissions": "Land-use change and forestry",
    "manufacturing_and_construction_ghg_emissions": "Manufacturing and construction",
    "other_fuel_combustion_ghg_emissions": "Other fuel combustion",
    "transport_ghg_emissions": "Transportation",
    "waste_ghg_emissions": "Waste",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load emissions by detailed sector and read its main table.
    snap = paths.load_snapshot("emissions_by_detailed_sector.json")
    subsectors = load_json(snap.path)

    # Load emissions by sector and read its main table.
    # NOTE: This data is only used for some sanity checking (which is commented out below).
    # All data was extracted manually from the WRI site.
    # ds_emissions = paths.load_dataset("emissions_by_sector")
    # tb = ds_emissions.read("greenhouse_gas_emissions_by_sector")

    #
    # Process data.
    #
    # Sanity checks.
    sanity_check_inputs(subsectors=subsectors)

    # # Select relevant columns from the table.
    # tb = tb[(tb["country"] == "World") & (tb["year"] == YEAR)].drop(columns=["country", "year"], errors="raise")
    # total_emissions = tb["total_ghg_emissions_including_lucf"].item()
    # tb = tb[list(DATA_TO_SECTOR_NAMING)].rename(columns=DATA_TO_SECTOR_NAMING, errors="raise")
    # data_shares = {column: (100 * tb[column].item() / total_emissions).round(1) for column in tb.columns}
    # wri_shares = {sector: round(sum(SUBSECTORS[sector].values()), 1) for sector in SUBSECTORS}
    # # NOTE: I expected that the total shares of each category in the Climate Watch data would be similar to the ones extracted from the WRI data. However, they differ up to 15% for some sectors, and 73% for "Other fuel combustion":
    # for category in list(data_shares):
    #     print(category, 100 * abs(data_shares[category] - wri_shares[category]) / wri_shares[category])
    # Result:
    # Agriculture 3.418803418803422
    # International bunker 0.0
    # Buildings 0.0
    # Electricity and heat 10.847457627118654
    # Fugitive emissions 0.0
    # Industrial processes 1.5625000000000082
    # Land-use change and forestry 0.0
    # Manufacturing and construction 0.0
    # Other fuel combustion 72.72727272727272
    # Transportation 13.868613138686134
    # Waste 8.823529411764714

    # It would be good to have access to the disaggregated data from WRI, but they do not publish it.

    # Final shares in the custom mapping.
    tb_custom = Table(
        {
            "sector": CUSTOM_MAPPING.keys(),
            "share_of_global_ghg_emissions": [
                sum(subsectors[toplevel][leaf] for toplevel, leaf, _ in items) for items in CUSTOM_MAPPING.values()
            ],
        }
    )

    # Fix rounding issues.
    tb_custom = fix_rounding_issues(tb_custom=tb_custom)

    # Add Climate Watch origin to new column.
    tb_custom["share_of_global_ghg_emissions"].metadata.origins = [snap.metadata.origin]

    # Add country and year columns.
    tb_custom = tb_custom.assign(**{"country": "World", "year": YEAR})

    # Add an explanation to the metadata of which subsectors are included in each category.
    description_processing = "Each category is made up of the following subsectors, based on IPCC definitions (as reported by World Resources Institute):\n"
    for group, choices in CUSTOM_MAPPING.items():
        description_processing += f"\n{group}:\n"
        description_processing += "".join(
            f"* {sector} - {subsector}: {explanation}\n" for sector, subsector, explanation in choices
        )
    description_processing += "\nNote that direct fuel use in buildings is shown under 'Keeping warm and cool'. Electricity and district heat for buildings are included under 'Electricity', reflecting production-based reporting."
    tb_custom["share_of_global_ghg_emissions"].metadata.description_processing = description_processing

    # Improve table format.
    tb_custom = tb_custom.format(keys=["country", "sector", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_custom])
    ds_garden.save()
