"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import map_series

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Sector mapping.
# This mapping can be found on page 12 of their version comparison document:
# https://github.com/JGCRI/CEDS/blob/master/documentation/Version_comparison_figures_v_2024_07_08_vs_v_2021_04_20.pdf
SECTOR_MAPPING = {
    # Sector originally called "Agriculture (AGR)".
    "Agriculture": [
        "3B_Manure-management",
        "3D_Rice-Cultivation",
        "3D_Soil-emissions",
        "3E_Enteric-fermentation",
        "3I_Agriculture-other",
    ],
    # In the document, they had a sector "Aviation", containing international aviation and domestic aviation. But I think it's better to separate them.
    # "Aviation": [
    #     "1A3ai_International-aviation",
    #     "1A3aii_Domestic-aviation",
    # ],
    "International aviation": ["1A3ai_International-aviation"],
    "Domestic aviation": ["1A3aii_Domestic-aviation"],
    # "Residential, Commercial, Other (DOM)".
    "Residential, commercial, and other": [
        "1A4a_Commercial-institutional",
        "1A4b_Residential",
        "1A4c_Agriculture-forestry-fishing",
        "1A5_Other-unspecified",
    ],
    # "Int. Shipping".
    "International shipping": [
        "1A3di_International-shipping",
        "1A3di_Oil_Tanker_Loading",
    ],
    # "Energy Transformation and Production (ENE)".
    "Energy transformation and production": [
        "1A1a_Electricity-autoproducer",
        "1A1a_Electricity-public",
        "1A1a_Heat-production",
        "1A1bc_Other-transformation",
        "1B1_Fugitive-solid-fuels",
        # In the document, there was "1B2_Fugitive-petr-and-gas", but in the data I only see:
        "1B2_Fugitive-petr",
        "1B2d_Fugitive-other-energy",
        "7A_Fossil-fuel-fires",
        # NOTE: The following were found in the data, but not in the mapping. I'm assuming they belong here (because of their similarity with other subsectors and because of the context).
        "1B2b_Fugitive-NG-distr",
        "1B2b_Fugitive-NG-prod",
        "7BC_Indirect-N2O-non-agricultural-N",
    ],
    # "Industry (IND)".
    "Industry": [
        "1A2a_Ind-Comb-Iron-steel",
        "1A2b_Ind-Comb-Non-ferrous-metals",
        "1A2c_Ind-Comb-Chemicals",
        "1A2d_Ind-Comb-Pulp-paper",
        "1A2e_Ind-Comb-Food-tobacco",
        "1A2f_Ind-Comb-Non-metalic-minerals",
        "1A2g_Ind-Comb-Construction",
        "1A2g_Ind-Comb-machinery",
        "1A2g_Ind-Comb-mining-quarying",
        "1A2g_Ind-Comb-other",
        "1A2g_Ind-Comb-textile-leather",
        "1A2g_Ind-Comb-transpequip",
        "1A2g_Ind-Comb-wood-products",
        "2A1_Cement-production",
        "2A2_Lime-production",
        # In the document, there was "2A3_Other-minerals", but in the data I see:
        "2Ax_Other-minerals",
        "2B_Chemical-industry",
        # In the document, there was "2C_Metal-production", but in the data I see the following three:
        "2C1_Iron-steel-alloy-prod",
        "2C3_Aluminum-production",
        "2C4_Non-Ferrous-other-metals",
        "2H_Pulp-and-paper-food-beverage-wood",
        # In the document, there was "2L_Other-process-emissions", but it does not appear in the data
        # "2L_Other-process-emissions",
        # NOTE: The following were found in the data, but not in the mapping. I'm assuming they belong here (because of their similarity with other subsectors and because of the context).
        "2B2_Chemicals-Nitric-acid",
        "2B3_Chemicals-Adipic-acid",
    ],
    # "Solvents (SLV)".
    "Solvents": [
        "2D_Degreasing-Cleaning",
        "2D_Paint-application",
        # In the document, there was "2D3_Chemical-products-manufacture-processing", but in the data I see:
        "2D_Chemical-products-manufacture-processing",
        # In the document, there was "2D3_Other-product-use", but in the data I see:
        "2D_Other-product-use",
    ],
    # "Transportation (TRA)".
    "Transportation": [
        "1A3b_Road",
        "1A3c_Rail",
        "1A3dii_Domestic-navigation",
        "1A3eii_Other-transp",
    ],
    # "Waste (WST)".
    "Waste": [
        "5A_Solid-waste-disposal",
        # In the document, there was "5C_Waste-incineration", but in the data I see:
        "5C_Waste-combustion",
        "5D_Wastewater-handling",
        "5E_Other-waste-handling",
        "6A_Other-in-total",
        # NOTE: The following were found in the data, but not in the mapping. I'm assuming they belong here (because of their similarity with other subsectors and because of the context).
        "6B_Other-not-in-total",
    ],
}

# Define the name of the domestic aviation subsector (which will need to be handled separately).
SUBSECTOR_DOMESTIC_AVIATION = "1A3aii_Domestic-aviation"

# Subsectors expected in the bunkers table.
BUNKERS_SECTORS = [
    "1A3ai_International-aviation",
    SUBSECTOR_DOMESTIC_AVIATION,
    "1A3di_International-shipping",
]

# Mapping of pollutants.
POLLUTANTS_MAPPING = {
    "CO2": "CO₂",
    "CH4": "CH₄",
    "NMVOC": "NMVOC",
    "N2O": "N₂O",
    "SO2": "SO₂",
    "CO": "CO",
    "BC": "BC",
    "NH3": "NH₃",
    "OC": "OC",
    "NOx": "NOₓ",
}

# Expected units for each pollutant.
# NOTE: Units will be assigned using the metadata yaml file.
EXPECTED_UNITS = {
    "BC": "ktC",
    "CH4": "ktCH4",
    "CO": "ktCO",
    "CO2": "ktCO2",
    "N2O": "ktN2O",
    "NH3": "ktNH3",
    "NMVOC": "ktNMVOC",
    "NOx": "ktNO2",
    "OC": "ktC",
    "SO2": "ktSO2",
}


def sanity_check_inputs(tb_detailed: Table, tb_bunkers: Table) -> None:
    error = "Columns in detailed table have changed."
    assert [column for column in tb_detailed.columns if not column.startswith("x")] == [
        "em",
        "country",
        "sector",
        "fuel",
        "units",
    ], error
    assert set([int(column.replace("x", "")) for column in tb_detailed.columns if column.startswith("x")]) <= set(
        range(1750, 2023)
    ), error
    error = "Columns in bunkers table have changed."
    assert [column for column in tb_bunkers.columns if not column.startswith("x")] == [
        "em",
        "iso",
        "sector",
        "units",
    ], error
    assert set([int(column.replace("x", "")) for column in tb_bunkers.columns if column.startswith("x")]) <= set(
        range(1750, 2023)
    ), error
    error = "List of subsectors in the detailed table has changed."
    all_subsectors = sum(list(SECTOR_MAPPING.values()), [])
    assert set(tb_detailed["sector"]) == set(all_subsectors), error
    error = "List of subsectors in the bunkers table has changed."
    assert set(tb_bunkers["sector"]) == set(BUNKERS_SECTORS), error
    assert set(BUNKERS_SECTORS) < set(all_subsectors), error
    error = "Each pollutant was expected to have just one unit."
    assert (
        tb_detailed.groupby("em", as_index=False, observed=True).agg({"units": "nunique"})["units"] == 1
    ).all(), error
    assert (
        tb_bunkers.groupby("em", as_index=False, observed=True).agg({"units": "nunique"})["units"] == 1
    ).all(), error
    error = "Detailed table was expected to have all countries in the bunkers table. This has changed (not important, simply check it and redefine this assertion)."
    assert set(tb_detailed["country"]) - set(tb_bunkers["iso"]) == set(), error
    error = "Bunkers table was expected to have all countries in the detailed table, except Palestine. This has changed (not important, simply check it redefine this assertion)."
    assert set(tb_bunkers["iso"]) - set(tb_detailed["country"]) == set(["pse"]), error
    # Check that domestic aviation emissions given in the bunkers table are the same as the ones given in the detailed table.
    detailed_domestic_aviation = (
        tb_detailed[(tb_detailed["sector"] == SUBSECTOR_DOMESTIC_AVIATION) & (~tb_detailed["country"].isin(["global"]))]
        .drop(columns=["fuel"])
        .groupby(["em", "country", "sector", "units"], as_index=False, observed=True)
        .sum()
        .sort_values(["em", "country", "sector", "units"])
        .reset_index(drop=True)
    )
    bunkers_domestic_aviation = (
        tb_bunkers[(tb_bunkers["sector"] == SUBSECTOR_DOMESTIC_AVIATION) & (~tb_bunkers["iso"].isin(["global", "pse"]))]
        .rename(columns={"iso": "country"})
        .sort_values(["em", "country", "sector", "units"])
        .reset_index(drop=True)
    )
    # A simple "df1.equals(df2)" doesn't work. For simplicity, first assert indexes are identical, and then assert that values agree within a small tolerance.
    detailed_domestic_aviation[["em", "country", "sector", "units"]].astype(str).equals(
        bunkers_domestic_aviation[["em", "country", "sector", "units"]].astype(str)
    )
    for column in detailed_domestic_aviation.columns:
        if column.startswith("x"):
            error = f"Data from detailed table differs from bunkers table more than expected (column {column})."
            assert (
                100
                * abs(detailed_domestic_aviation[column] - bunkers_domestic_aviation[column].fillna(0.0))
                / (detailed_domestic_aviation[column] + 1e-7)
            ).max() < 1e-4, error

    # Check that the details table contains international aviation and shipping only for "global".
    error = "International aviation and shipping was expected to be informed only for 'global' in the detailed table."
    for sector in ["1A3ai_International-aviation", "1A3di_International-shipping"]:
        assert set(tb_detailed[tb_detailed["sector"] == sector]["country"]) == set(["global"]), error

    error = "Expected 'global' in the bunkers table to be nonzero only for international shipping."
    assert set(
        tb_bunkers[
            (tb_bunkers["iso"] == "global")
            & (tb_bunkers[[c for c in tb_bunkers.columns if c.startswith("x")]].sum(axis=1) > 0)
        ]["sector"]
    ), error
    error = "Pollutant units have changed."
    assert tb_detailed[["em", "units"]].drop_duplicates().set_index(["em"])["units"].to_dict() == EXPECTED_UNITS, error


def combine_detailed_and_bunkers_tables(tb_detailed: Table, tb_bunkers: Table) -> Table:
    # The bunkers table contains a "global" country. But note that, according to the README inside the bunkers zip folder,
    # * The "global" emissions in the detailed table contain bunker emission (international shipping, domestic aviation, and international aviation).
    # * The "global" emissions in the bunkers table (already contained in the detailed "global" emissions) are the difference between total shipping fuel consumption (as estimated by the International Maritime Organization and other sources) and fuel consumption as reported by IEA. This additional fuel cannot be allocated to specific iso's. This correction to total fuel consumption is modest in recent years, but becomes much larger in earlier years.
    # So, we can draw the following conclusions:
    # 1. We don't need to add bunker emissions to the detailed "global" emissions.
    # 2. We can rename the bunkers "global" emissions as "Other", given that these emissions are not allocated to any country. If this causes too much confusion, we can consider deleting them.
    # 3. Domestic aviation for each country is exactly the same in the detailed table and in the bunkers table (except that the latter contains data for Palestine). Therefore, delete domestic aviation from the detailed table.
    # 4. International aviation in the detailed table is only informed for "global". So, when combining with the bunkers table, there will be no duplicates problem.
    # 5. International shipping (sector) in the detailed table includes international shipping (subsector, which contains only "global") and oil tanker loading (which contains data for other countries). Bunkers table contains International shipping (subsector). Therefore, when combining both tables, there will be duplicates for all countries (one row corresponding to the oil tankers data from the detailed table, and another from the international shipping from the bunkers table).

    # Rename bunkers table consistently with the detailed table.
    tb_bunkers = tb_bunkers.rename(columns={"iso": "country"}, errors="raise")

    # Rename "global" in the bunkers table to "Other" (since it corresponds to a difference between estimates, that cannot be allocated to any country).
    # NOTE: In sanity checks, we asserted that bunkers 'global' is only informed (and non-zero) for international shipping.
    tb_bunkers["country"] = tb_bunkers["country"].cat.rename_categories(lambda x: "Other" if x == "global" else x)
    # Now, remove domestic aviation from the detailed table (which is identical to the one in the bunkers table, except that the later contains Palestine).
    tb_detailed = tb_detailed[tb_detailed["sector"] != SUBSECTOR_DOMESTIC_AVIATION].reset_index(drop=True)

    tb = pr.concat([tb_detailed, tb_bunkers], short_name=paths.short_name)

    # Remove units column.
    # NOTE: In the sanity checks, we asserted that they were as expected.
    tb = tb.drop(columns=["units"], errors="raise")

    # Sanity check.
    error = "There are duplicated rows in the combined table."
    assert tb[tb.duplicated(subset=["em", "country", "sector"], keep=False)].empty, error

    return tb


def remap_table_categories(tb: Table) -> Table:
    # We don't need the detailed sectorial information, and we don't need to keep the fuel information either.
    # So, map detailed subsectors into broader sectors, e.g. "Transportation", "Agriculture".
    subsector_to_sector = {
        subsector: sector for sector, subsectors in SECTOR_MAPPING.items() for subsector in subsectors
    }
    tb["sector"] = map_series(
        tb["sector"], mapping=subsector_to_sector, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )
    tb = tb.groupby(["em", "country", "sector"], as_index=False, observed=True).sum()

    # Rename columns conveniently.
    tb = tb.rename(columns={"em": "pollutant"}, errors="raise")

    # Map pollutants.
    tb["pollutant"] = map_series(
        tb["pollutant"], mapping=POLLUTANTS_MAPPING, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ceds_air_pollutants")

    # Load auxiliary dataset of regions, income groups, and population.
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")
    ds_population = paths.load_dataset("population")

    # Read tables from meadow dataset.
    # The "detailed" table contains emissions for each pollutant, country, sector, fuel, and year (a column for each year). There is an additional column for units, but they are always the same for each pollutant.
    # The "bunkers" table contains only data on international aviation and shipping, and domestic aviation, at the country level.
    # NOTE: We keep the optimal types (which includes categoricals) for better performance, given the tables sizes.
    tb_detailed = ds_meadow.read("ceds_air_pollutants__detailed", safe_types=False)
    tb_bunkers = ds_meadow.read("ceds_air_pollutants__bunkers", safe_types=False)

    #
    # Process data.
    #
    # Sanity checks inputs.
    sanity_check_inputs(tb_detailed=tb_detailed, tb_bunkers=tb_bunkers)

    # We don't need to keep the fuel information from the detailed table.
    # Drop the fuel column and sum over all other dimensions.
    # NOTE: This needs to be done before combining with bunkers, given that the latter does not contain fuel information.
    tb_detailed = (
        tb_detailed.drop(columns=["fuel"])
        .groupby(["em", "country", "sector", "units"], as_index=False, observed=True)
        .sum()
    )

    # Combine detailed and bunkers tables.
    tb = combine_detailed_and_bunkers_tables(tb_detailed=tb_detailed, tb_bunkers=tb_bunkers)

    # Simplify subsectors into broader categories, and remove the "fuel" dimension, which for now we don't need.
    tb = remap_table_categories(tb=tb)

    # Restructure table to have year as a column.
    tb = tb.rename(columns={column: int(column[1:]) for column in tb.columns if column.startswith("x")})
    tb = tb.melt(id_vars=["pollutant", "country", "sector"], var_name="year", value_name="emissions")

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add region aggregates.
    tb = geo.add_regions_to_table(
        tb=tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        index_columns=["country", "year", "pollutant", "sector"],
    )

    # Add per capita variables.
    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, warn_on_missing_countries=False)
    tb["emissions_per_capita"] = tb["emissions"] / tb["population"]

    # TODO: Create an "all sectors" aggregate sector, and an "All pollutants" aggregate pollutant.

    # Improve table format.
    tb = tb.format(["country", "year", "pollutant", "sector"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
