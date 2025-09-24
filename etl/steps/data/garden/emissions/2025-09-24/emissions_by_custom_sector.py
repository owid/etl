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
    "Energy": {
        "Electricity and Heat": {
            "Residential Buildings": 7.5,
            "Commercial Buildings": 4.8,
            "Unallocated Fuel Combustion": 2.8,
            "Chemical and petrochemical": 2.3,
            "Other Industry": 2.2,
            "Iron and steel": 1.8,
            "Non-ferrous metals": 1.4,
            "Machinery": 1.4,
            "Agriculture & Fishing Energy Use": 1,
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
            "Other Transportation": 0,
        },
        "Transportation": {
            "Road": 12.1,
            "Air": 0.7,
            "Ship": 0.3,
            "Pipeline": 0.3,
            "Rail": 0.2,
            "Other Transportation": 0.1,
        },
        "Manufacturing and Construction": {
            "Iron and steel": 4.3,
            "Other Industry": 2.4,
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
            "Residential Buildings": 5,
            "Commercial Buildings": 1.6,
        },
        "Fugitive Emissions": {
            "Vented": 4.4,
            "Flared": 1,
            "Production": 0.7,
            "Transmission and distribution": 0.4,
            "Unallocated Fuel Combustion": 0.1,
        },
        "Other Fuel Combustion": {
            "Unallocated Fuel Combustion": 3.5,
            "Agriculture & Fishing Energy Use": 0.9,
        },
        "International Bunker": {
            "Ship": 1.3,
            "Air": 0.7,
        },
    },
    "Agriculture": {
        "Livestock & Manure": 5.9,
        "Agriculture Soils": 4.1,
        "Rice Cultivation": 1.2,
        "Burning": 0.5,
    },
    "Industrial Processes": {
        "Cement": 3.4,
        "Chemical and petrochemical (IP)": 2.6,
        "Other Industry (IP)": 0.1,
        "Electronics (IP)": 0.1,
        "Electric Power Systems": 0.1,
        "Non-ferrous metals (IP)": 0.1,
    },
    "Waste": {
        "Landfills": 2,
        "Wastewater": 1.3,
        "Other Waste": 0.1,
    },
    "Land-use change and forestry": {
        "Drained organic soils": 1.7,
        "Forest Land": 0.6,
        "Forest fires": 0.4,
        "Fires in organic soils": 0,
    },
}

# Custom remapping of Climate Watch subsectors into our custom categories.
CUSTOM_MAPPING = {
    "Growing food": [
        # Methane from animal digestion and manure handling.
        SUBSECTORS["Agriculture"]["Livestock & Manure"],
        # Nitrous oxide from fertilizer use and other soil management.
        SUBSECTORS["Agriculture"]["Agriculture Soils"],
        # Methane from flooded rice paddies.
        SUBSECTORS["Agriculture"]["Rice Cultivation"],
        # Field burning of crop residues and savannas.
        SUBSECTORS["Agriculture"]["Burning"],
        # On-farm fuel use for tractors, pumps, and fishing vessels (non-electric).
        SUBSECTORS["Energy"]["Other Fuel Combustion"]["Agriculture & Fishing Energy Use"],
        # Land emissions tied to agriculture/forestry expansion and drainage (LUCF).
        SUBSECTORS["Land-use change and forestry"]["Drained organic soils"],
        # Net forest land sources (deforestation/degradation net of regrowth, per WRI slice).
        SUBSECTORS["Land-use change and forestry"]["Forest Land"],
        # Emissions from forest fires.
        SUBSECTORS["Land-use change and forestry"]["Forest fires"],
        # Fires in organic soils/peat (listed as 0 here but keep for completeness).
        SUBSECTORS["Land-use change and forestry"]["Fires in organic soils"],
    ],
    "Keeping warm and cool": [
        # Direct onsite combustion in homes (space/water heating, cooking when not electric).
        SUBSECTORS["Energy"]["Buildings"]["Residential Buildings"],
        # Direct onsite combustion in commercial buildings (space/water heating, cooking).
        SUBSECTORS["Energy"]["Buildings"]["Commercial Buildings"],
        # Stationary fuel use not allocated elsewhere; largely generic heating/boilers.
        SUBSECTORS["Energy"]["Other Fuel Combustion"]["Unallocated Fuel Combustion"],
    ],
    "Getting around": [
        # Road vehicles (gasoline/diesel, not counting electricity use).
        SUBSECTORS["Energy"]["Transportation"]["Road"],
        # Aviation within national inventories (domestic).
        SUBSECTORS["Energy"]["Transportation"]["Air"],
        # Shipping within national inventories (domestic).
        SUBSECTORS["Energy"]["Transportation"]["Ship"],
        # Fuels used for pipeline transport (e.g., compressors).
        SUBSECTORS["Energy"]["Transportation"]["Pipeline"],
        # Rail transport fuels (diesel rail—electric rail is in Electricity bucket).
        SUBSECTORS["Energy"]["Transportation"]["Rail"],
        # Miscellaneous smaller transport categories.
        SUBSECTORS["Energy"]["Transportation"]["Other Transportation"],
        # International shipping fuel (bunker).
        SUBSECTORS["Energy"]["International Bunker"]["Ship"],
        # International aviation fuel (bunker).
        SUBSECTORS["Energy"]["International Bunker"]["Air"],
    ],
    "Electricity": [
        # Electricity & heat generation for households.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Residential Buildings"],
        # Electricity & heat generation for commercial buildings.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Commercial Buildings"],
        # Electricity & heat generation used by industry (chemicals).
        SUBSECTORS["Energy"]["Electricity and Heat"]["Chemical and petrochemical"],
        # Electricity & heat generation used by industry (other).
        SUBSECTORS["Energy"]["Electricity and Heat"]["Other Industry"],
        # Electricity & heat generation used by iron & steel.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Iron and steel"],
        # Electricity & heat generation used by non-ferrous metals.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Non-ferrous metals"],
        # Electricity & heat generation used by machinery.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Machinery"],
        # Electricity & heat generation used by agriculture & fishing.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Agriculture & Fishing Energy Use"],
        # Electricity & heat generation used by non-metallic minerals.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Non-metallic minerals"],
        # Electricity & heat generation used by food & tobacco manufacturing.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Food and tobacco"],
        # Electricity & heat generation used by textiles & leather.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Textile and leather"],
        # Electricity & heat generation used by mining & quarrying.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Mining and quarrying"],
        # Electricity & heat generation used by paper/pulp/printing.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Paper, pulp and printing"],
        # Electricity & heat generation used by transport equipment manufacturing.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Transport equipment"],
        # Electricity used by rail systems.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Rail"],
        # Electricity used by construction activities.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Construction"],
        # Electricity & heat generation used by wood products.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Wood and wood products"],
        # Electricity used in road transport (EVs, small share here).
        SUBSECTORS["Energy"]["Electricity and Heat"]["Road"],
        # Electricity for pipelines (e.g., electric compressors/pumps).
        SUBSECTORS["Energy"]["Electricity and Heat"]["Pipeline"],
        # Misc. electricity uses in transport.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Other Transportation"],
        # Upstream leaks/venting/flaring in fossil energy supply feeding power/heat.
        SUBSECTORS["Energy"]["Fugitive Emissions"]["Vented"],
        SUBSECTORS["Energy"]["Fugitive Emissions"]["Flared"],
        SUBSECTORS["Energy"]["Fugitive Emissions"]["Production"],
        SUBSECTORS["Energy"]["Fugitive Emissions"]["Transmission and distribution"],
        # Small unallocated stationary fuel slice tied to energy system operations.
        SUBSECTORS["Energy"]["Fugitive Emissions"]["Unallocated Fuel Combustion"],
        # Another small slice of unallocated fuel combustion.
        SUBSECTORS["Energy"]["Electricity and Heat"]["Unallocated Fuel Combustion"],
    ],
    "Making things": [
        # Direct onsite combustion in industry & construction (not electricity).
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Iron and steel"],
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Other Industry"],
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Non-metallic minerals"],
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Chemical and petrochemical"],
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Food and tobacco"],
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Non-ferrous metals"],
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Construction"],
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Mining and quarrying"],
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Paper, pulp and printing"],
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Machinery"],
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Textile and leather"],
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Transport equipment"],
        SUBSECTORS["Energy"]["Manufacturing and Construction"]["Wood and wood products"],
        # Process CO2 from cement clinker production.
        SUBSECTORS["Industrial Processes"]["Cement"],
        # Process emissions from chemical & petrochemical production (non-energy).
        SUBSECTORS["Industrial Processes"]["Chemical and petrochemical (IP)"],
        # Other non-energy industrial process emissions.
        SUBSECTORS["Industrial Processes"]["Other Industry (IP)"],
        # Process emissions from electronics manufacturing.
        SUBSECTORS["Industrial Processes"]["Electronics (IP)"],
        # SF6 and related gases from power systems equipment (industrial equipment use).
        SUBSECTORS["Industrial Processes"]["Electric Power Systems"],
        # Process emissions in non-ferrous metals (e.g., aluminum anodes).
        SUBSECTORS["Industrial Processes"]["Non-ferrous metals (IP)"],
        # Post-production waste handling (landfills, wastewater, other waste).
        SUBSECTORS["Waste"]["Landfills"],
        SUBSECTORS["Waste"]["Wastewater"],
        SUBSECTORS["Waste"]["Other Waste"],
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
    assert sum([sum(subsectors) for subsectors in CUSTOM_MAPPING.values()]) > 99, error

    # Check that all subsectors are included in the custom mapping.
    # To do that, check that the lists of unique values coincide.
    def leaves_values(d):
        for v in d.values():
            if isinstance(v, dict):
                yield from leaves_values(v)
            else:
                yield v

    all_vals = set(leaves_values(SUBSECTORS))
    mapped_vals = {v for vals in CUSTOM_MAPPING.values() for v in vals}
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

    # TODO: Consider comparing with the original data.
    # TODO: Add metadata describing the subsectors that each category includes.
    # data = tb[(tb["country"]=="World") & (tb["year"]==YEAR)].drop(columns=["country", "year"]).iloc[0].to_dict()
    # data = {tb[field].metadata.title: 100 * value / data["total_ghg_emissions_including_lucf"] for field, value in data.items() if "capita" not in field if not "total" in field if "population" not in field}
    # data = {field.replace("Greenhouse gas emissions from ", "").replace(" of greenhouse gas from", "").capitalize(): value for field, value in data.items()}

    # Final shares in the custom mapping.
    # tb_custom = Table({sector: [sum(subsectors)] for sector, subsectors in CUSTOM_MAPPING.items()})
    tb_custom = Table(
        {
            "sector": CUSTOM_MAPPING.keys(),
            "share_of_global_ghg_emissions": [sum(values) for values in CUSTOM_MAPPING.values()],
        }
    )

    # Add Climate Watch origin to all new columns.
    origin = tb[tb.columns[-1]].metadata.origins[0]
    assert origin.producer == "Climate Watch"
    # for column in tb_custom.columns:
    #     tb_custom[column].origins = [origin]
    tb_custom["share_of_global_ghg_emissions"].metadata.origins = [origin]

    # Add country and year columns.
    tb_custom = tb_custom.assign(**{"country": "World", "year": YEAR})

    # Improve table format.
    tb_custom = tb_custom.format(keys=["country", "sector", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_custom])
    ds_garden.save()
