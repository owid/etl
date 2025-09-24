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

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Year that Climate Watch data refers to.
YEAR = 2021

SUBSECTORS = {
    "Electricity and heat": {
        "Residential buildings": 7.5,
        "Commercial buildings": 4.8,
        "Unallocated fuel combustion": 2.8,
        "Chemical and petrochemical": 2.3,
        "Other industry": 2.2,
        "Iron and steel": 1.8,
        "Non-ferrous metals": 1.4,
        "Machinery": 1.4,
        "Agriculture and fishing energy use": 1,
        "Non-metallic minerals": 0.8,
        "Food and tobacco": 0.8,
        "Textile and leather": 0.5,
        "Mining and quarrying": 0.5,
        "Paper, pulp and printing": 0.5,
        "Transport equipment": 0.4,
        "Rail": 0.3,
        "Construction": 0.3,
        "Wood and wood products": 0.1,
        "Road": 0.1,
        "Pipeline": 0,
        "Other transportation": 0,
    },
    "Transportation": {
        "Road": 12.1,
        "Air": 0.7,
        "Ship": 0.3,
        "Pipeline": 0.3,
        "Rail": 0.2,
        "Other transportation": 0.1,
    },
    "Manufacturing and construction": {
        "Iron and steel": 4.3,
        "Other industry": 2.4,
        "Non-metallic minerals": 2.3,
        "Chemical and petrochemical": 1.5,
        "Food and tobacco": 0.4,
        "Non-ferrous metals": 0.4,
        "Construction": 0.3,
        "Mining and quarrying": 0.3,
        "Paper, pulp and printing": 0.3,
        "Machinery": 0.2,
        "Textile and leather": 0.1,
        "Transport equipment": 0.1,
        "Wood and wood products": 0,
    },
    "Buildings": {
        "Residential buildings": 5,
        "Commercial buildings": 1.6,
    },
    "Fugitive emissions": {
        "Vented": 4.4,
        "Flared": 1,
        "Production": 0.7,
        "Transmission and distribution": 0.4,
        "Unallocated fuel combustion": 0.1,
    },
    "Other fuel combustion": {
        "Unallocated fuel combustion": 3.5,
        "Agriculture and fishing energy use": 0.9,
    },
    "International bunker": {
        "Ship": 1.3,
        "Air": 0.7,
    },
    "Agriculture": {
        "Livestock and manure": 5.9,
        "Agriculture soils": 4.1,
        "Rice cultivation": 1.2,
        "Burning": 0.5,
    },
    "Industrial processes": {
        "Cement": 3.4,
        "Chemical and petrochemical (ip)": 2.6,
        "Other industry (ip)": 0.1,
        "Electronics (ip)": 0.1,
        "Electric power systems": 0.1,
        "Non-ferrous metals (ip)": 0.1,
    },
    "Waste": {
        "Landfills": 2,
        "Wastewater": 1.3,
        "Other waste": 0.1,
    },
    "Land-use change and forestry": {
        "Drained organic soils": 1.7,
        "Forest land": 0.6,
        "Forest fires": 0.4,
        "Fires in organic soils": 0,
    },
}


# Custom remapping of Climate Watch subsectors into our custom categories.
CUSTOM_MAPPING = {
    "Growing food": [
        ("Agriculture", "Livestock and manure", "Methane from animal digestion and manure handling."),
        ("Agriculture", "Agriculture soils", "Nitrous oxide from fertilizer use and other soil management."),
        ("Agriculture", "Rice cultivation", "Methane from flooded rice paddies."),
        ("Agriculture", "Burning", "Field burning of crop residues and savannas."),
        (
            "Other fuel combustion",
            "Agriculture and fishing energy use",
            "On-farm fuel use for tractors, pumps, and fishing vessels (non-electric).",
        ),
        (
            "Land-use change and forestry",
            "Drained organic soils",
            "Land emissions tied to agriculture/forestry expansion and drainage (LUCF).",
        ),
        (
            "Land-use change and forestry",
            "Forest land",
            "Net forest land sources (deforestation/degradation net of regrowth, per WRI slice).",
        ),
        ("Land-use change and forestry", "Forest fires", "Emissions from forest fires."),
        (
            "Land-use change and forestry",
            "Fires in organic soils",
            "Fires in organic soils/peat (listed as 0 here but keep for completeness).",
        ),
    ],
    "Keeping warm and cool": [
        (
            "Buildings",
            "Residential buildings",
            "Direct onsite combustion in homes (space/water heating, cooking when not electric).",
        ),
        (
            "Buildings",
            "Commercial buildings",
            "Direct onsite combustion in commercial buildings (space/water heating, cooking).",
        ),
        (
            "Other fuel combustion",
            "Unallocated fuel combustion",
            "Stationary fuel use not allocated elsewhere; largely generic heating/boilers.",
        ),
    ],
    "Getting around": [
        ("Transportation", "Road", "Road vehicles (gasoline/diesel, not counting electricity use)."),
        ("Transportation", "Air", "Aviation within national inventories (domestic)."),
        ("Transportation", "Ship", "Shipping within national inventories (domestic)."),
        ("Transportation", "Pipeline", "Fuels used for pipeline transport (e.g., compressors)."),
        ("Transportation", "Rail", "Rail transport fuels (diesel rail—electric rail is in electricity bucket)."),
        ("Transportation", "Other transportation", "Miscellaneous smaller transport categories."),
        ("International bunker", "Ship", "International shipping fuel (bunker)."),
        ("International bunker", "Air", "International aviation fuel (bunker)."),
    ],
    "Electricity": [
        ("Electricity and heat", "Residential buildings", "Electricity and heat generation for households."),
        ("Electricity and heat", "Commercial buildings", "Electricity and heat generation for commercial buildings."),
        (
            "Electricity and heat",
            "Chemical and petrochemical",
            "Electricity and heat generation used by industry (chemicals).",
        ),
        ("Electricity and heat", "Other industry", "Electricity and heat generation used by industry (other)."),
        ("Electricity and heat", "Iron and steel", "Electricity and heat generation used by iron and steel."),
        ("Electricity and heat", "Non-ferrous metals", "Electricity and heat generation used by non-ferrous metals."),
        ("Electricity and heat", "Machinery", "Electricity and heat generation used by machinery."),
        (
            "Electricity and heat",
            "Agriculture and fishing energy use",
            "Electricity and heat generation used by agriculture and fishing.",
        ),
        (
            "Electricity and heat",
            "Non-metallic minerals",
            "Electricity and heat generation used by non-metallic minerals.",
        ),
        (
            "Electricity and heat",
            "Food and tobacco",
            "Electricity and heat generation used by food and tobacco manufacturing.",
        ),
        (
            "Electricity and heat",
            "Textile and leather",
            "Electricity and heat generation used by textiles and leather.",
        ),
        (
            "Electricity and heat",
            "Mining and quarrying",
            "Electricity and heat generation used by mining and quarrying.",
        ),
        (
            "Electricity and heat",
            "Paper, pulp and printing",
            "Electricity and heat generation used by paper, pulp and printing.",
        ),
        (
            "Electricity and heat",
            "Transport equipment",
            "Electricity and heat generation used by transport equipment manufacturing.",
        ),
        ("Electricity and heat", "Rail", "Electricity used by rail systems."),
        ("Electricity and heat", "Construction", "Electricity used by construction activities."),
        ("Electricity and heat", "Wood and wood products", "Electricity and heat generation used by wood products."),
        ("Electricity and heat", "Road", "Electricity used in road transport (EVs, small share here)."),
        ("Electricity and heat", "Pipeline", "Electricity for pipelines (e.g., electric compressors/pumps)."),
        ("Electricity and heat", "Other transportation", "Miscellaneous electricity uses in transport."),
        ("Fugitive emissions", "Vented", "Upstream venting in fossil energy supply feeding power/heat."),
        ("Fugitive emissions", "Flared", "Upstream flaring in fossil energy supply feeding power/heat."),
        ("Fugitive emissions", "Production", "Upstream production leaks in fossil energy supply feeding power/heat."),
        (
            "Fugitive emissions",
            "Transmission and distribution",
            "Upstream T&D leaks in fossil energy supply feeding power/heat.",
        ),
        (
            "Fugitive emissions",
            "Unallocated fuel combustion",
            "Small unallocated stationary fuel slice tied to energy system operations.",
        ),
        ("Electricity and heat", "Unallocated fuel combustion", "Another small slice of unallocated fuel combustion."),
    ],
    "Making things": [
        (
            "Manufacturing and construction",
            "Iron and steel",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        (
            "Manufacturing and construction",
            "Other industry",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        (
            "Manufacturing and construction",
            "Non-metallic minerals",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        (
            "Manufacturing and construction",
            "Chemical and petrochemical",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        (
            "Manufacturing and construction",
            "Food and tobacco",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        (
            "Manufacturing and construction",
            "Non-ferrous metals",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        (
            "Manufacturing and construction",
            "Construction",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        (
            "Manufacturing and construction",
            "Mining and quarrying",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        (
            "Manufacturing and construction",
            "Paper, pulp and printing",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        (
            "Manufacturing and construction",
            "Machinery",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        (
            "Manufacturing and construction",
            "Textile and leather",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        (
            "Manufacturing and construction",
            "Transport equipment",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        (
            "Manufacturing and construction",
            "Wood and wood products",
            "Direct onsite combustion in industry and construction (not electricity).",
        ),
        ("Industrial processes", "Cement", "Process CO2 from cement clinker production."),
        (
            "Industrial processes",
            "Chemical and petrochemical (ip)",
            "Process emissions from chemical and petrochemical production (non-energy).",
        ),
        ("Industrial processes", "Other industry (ip)", "Other non-energy industrial process emissions."),
        ("Industrial processes", "Electronics (ip)", "Process emissions from electronics manufacturing."),
        (
            "Industrial processes",
            "Electric power systems",
            "SF6 and related gases from power systems equipment (industrial equipment use).",
        ),
        (
            "Industrial processes",
            "Non-ferrous metals (ip)",
            "Process emissions in non-ferrous metals (e.g., aluminum anodes).",
        ),
        ("Waste", "Landfills", "Post-production waste handling: landfills."),
        ("Waste", "Wastewater", "Post-production waste handling: wastewater."),
        ("Waste", "Other waste", "Post-production waste handling: other waste."),
    ],
}


# Sanity checks.
def sanity_check_inputs():
    # Check that shares add up to ~100%.
    def deep_sum(d):
        return sum(deep_sum(v) if isinstance(v, dict) else v for v in d.values())

    error = "Share of GHG emissions from all subsectors don't add up to >99%"
    assert deep_sum(SUBSECTORS) > 99, error

    error = "Share of GHG emissions in custom mapping don't add up to >99%"
    assert (
        sum(sum(SUBSECTORS[toplevel][leaf] for toplevel, leaf, _ in items) for items in CUSTOM_MAPPING.values()) > 99
    ), error

    # Check that all subsectors are included in the custom mapping by comparing unique values.
    def leaves_values(d):
        for v in d.values():
            if isinstance(v, dict):
                yield from leaves_values(v)
            else:
                yield v

    all_vals = set(leaves_values(SUBSECTORS))
    mapped_vals = {SUBSECTORS[toplevel][leaf] for items in CUSTOM_MAPPING.values() for (toplevel, leaf, _desc) in items}
    assert all_vals == mapped_vals, f"Missing: {all_vals - mapped_vals}, Extra: {mapped_vals - all_vals}"


def run() -> None:
    #
    # Load inputs.
    #
    # Load emissions by sector and read its main table.
    ds_emissions = paths.load_dataset("emissions_by_sector")
    tb = ds_emissions.read("greenhouse_gas_emissions_by_sector")

    #
    # Process data.
    #
    # Sanity checks.
    sanity_check_inputs()

    # Final shares in the custom mapping.
    tb_custom = Table(
        {
            "sector": CUSTOM_MAPPING.keys(),
            "share_of_global_ghg_emissions": [
                sum(SUBSECTORS[toplevel][leaf] for toplevel, leaf, _ in items) for items in CUSTOM_MAPPING.values()
            ],
        }
    )

    # Add Climate Watch origin to all new columns.
    origin = tb[tb.columns[-1]].metadata.origins[0]
    assert origin.producer == "Climate Watch"
    tb_custom["share_of_global_ghg_emissions"].metadata.origins = [origin]

    # Add country and year columns.
    tb_custom = tb_custom.assign(**{"country": "World", "year": YEAR})

    # Add an explanation to the metadata of which subsectors are included in each category.
    description_processing = "Each category contains each of the following subsectors, as defined by Climate Watch:\n"
    for group, choices in CUSTOM_MAPPING.items():
        description_processing += f"\n{group}:\n"
        description_processing += "".join(
            f"* {sector} - {subsector}: {explanation}\n" for sector, subsector, explanation in choices
        )
    tb_custom["share_of_global_ghg_emissions"].metadata.description_processing = description_processing

    # Improve table format.
    tb_custom = tb_custom.format(keys=["country", "sector", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb_custom])
    ds_garden.save()
