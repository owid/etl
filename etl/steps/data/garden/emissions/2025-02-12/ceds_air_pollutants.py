"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

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
    # "Aviation".
    "Aviation": [
        "1A3ai_International-aviation",
        "1A3aii_Domestic-aviation",
    ],
    # "Residential, Commercial, Other (DOM)".
    "Residential, commercial, and other": [
        "1A4a_Commercial-institutional",
        "1A4b_Residential",
        "1A4c_Agriculture-forestry-fishing",
        "1A5_Other-unspecified",
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
    # "Int. Shipping".
    "International shipping": [
        "1A3di_International-shipping",
        "1A3di_Oil_Tanker_Loading",
    ],
    # "Waste (WST)".
    "Waste": [
        "5A_Solid-waste-disposal",
        # In the document, there was "5C_Waste-incineration", but in the data I see:
        "5C_Waste-combustion",
        "5D_Wastewater-handling",
        "5E_Other-waste-handling",
        "6A_Other-in-total",
    ],
}

# TODO: Add the following sectors to the mapping (they appeared in the data but not in the mapping):
# {'1B2b_Fugitive-NG-distr',
#  '1B2b_Fugitive-NG-prod',
#  '2B2_Chemicals-Nitric-acid',
#  '2B3_Chemicals-Adipic-acid',
#  '6B_Other-not-in-total',
#  '7BC_Indirect-N2O-non-agricultural-N'}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ceds_air_pollutants")

    # Read tables from meadow dataset.
    # NOTE: We keep the optimal types (which includes categoricals) for better performance, given the tables sizes.
    tb_detailed = ds_meadow.read("ceds_air_pollutants__detailed", safe_types=False)
    tb_bunkers = ds_meadow.read("ceds_air_pollutants__bunkers", safe_types=False)

    #
    # Process data.
    #
    # The "detailed" file contains emissions for each pollutant, country, sector, fuel, and year (a column for each year).
    # There is an additional column for units, but they are always the same for each pollutant.
    def sanity_check_inputs(tb_detailed: Table, tb_bunkers: Table) -> None:
        # TODO: Assert the exact name of expected columns in each table.
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

    # Sanity checks inputs.
    sanity_check_inputs(tb_detailed=tb_detailed, tb_bunkers=tb_bunkers)

    # For now, we do not need to keep fuel information.
    # Drop the fuel column and sum over all other dimensions.
    tb_detailed = (
        tb_detailed.drop(columns=["fuel"])
        .groupby(["em", "country", "sector", "units"], as_index=False, observed=True)
        .sum()
    )

    # We don't need the detailed sectorial information.
    # Instead, we want to map these detailed sectors into broader sector categories, e.g. "Transportation", "Agriculture".
    all_subsectors = sum(list(SECTOR_MAPPING.values()), [])
    set(tb_detailed["sector"]) - set(all_subsectors)
    set(all_subsectors) - set(tb_detailed["sector"])

    # TODO: Create a mapping for pollutants.
    # TODO: Maybe after the remapping we don't need to keep categoricals.

    # Restructure detailed table to have year as a column.
    tb_detailed = tb_detailed.rename(
        columns={column: int(column[1:]) for column in tb_detailed.columns if column.startswith("x")}
    )
    tb_detailed = tb_detailed.melt(id_vars=["em", "country", "sector", "units"], var_name="year", value_name="value")

    # Restructure bunkers table to have year as a column.
    tb_bunkers = tb_bunkers.rename(columns={"iso": "country"}).rename(
        columns={column: int(column[1:]) for column in tb_bunkers.columns if column.startswith("x")}
    )
    tb_bunkers = tb_bunkers.melt(id_vars=["em", "country", "sector", "units"], var_name="year", value_name="value")

    # The bunkers table contains a "global" country. But note that, according to the README inside the bunkers zip folder,
    # * The "global" emissions in the detailed table contain bunker emission (international shipping, domestic aviation, and international aviation).
    # * The "global" emissions in the bunkers table (already contained in the detailed "global" emissions) are the difference between total shipping fuel consumption (as estimated by the International Maritime Organization and other sources) and fuel consumption as reported by IEA. This additional fuel cannot be allocated to specific iso's. This correction to total fuel consumption is modest in recent years, but becomes much larger in earlier years.
    # So, we can draw the following conclusions:
    # 1. We don't need to add bunker emissions to the detailed "global" emissions.
    # 2. We can rename the bunkers "global" emissions as "Other", given that these emissions are not allocated to any country. If this causes too much confusion, we can consider deleting them.
    tb_bunkers["country"] = tb_bunkers["country"].cat.rename_categories(lambda x: "Other" if x == "global" else x)

    # NOTE: Both the bunkers and the detailed tables contain international shipping, domestic aviation, and international aviation. However, the detailed table contains international aviation and shipping only for "global" (whereas the bunkers table contains them at the country level).
    # TODO: Assert that the details table contains international aviation and shipping only for "global".
    # TODO: Assert that domestic emissions in the details and bunkers table coincide (except for "global" and "pse").
    # TODO: If they coincide, remove domestic aviations from the bunkers table.
    tb_bunkers = tb_bunkers[tb_bunkers["sector"] != "1A3aii_Domestic-aviation"].reset_index(drop=True)

    # Combine tables.
    tb = pr.concat([tb_detailed, tb_bunkers], short_name=paths.short_name)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # TODO: Define a dictionary of pollutant: units, and remove the units column.
    tb = tb.drop(columns=["units"], errors="raise")

    # Improve table format.
    tb = tb.format(["em", "country", "sector", "year"])

    tb["value"].m

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
