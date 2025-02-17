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
    # Define list of data columns
    data_columns = [column for column in tb_detailed.columns if column.startswith("x")]

    # Check that columns are as expected.
    error = "Columns in detailed table have changed."
    assert [column for column in tb_detailed.columns if column not in data_columns] == [
        "em",
        "country",
        "sector",
        "fuel",
        "units",
    ], error
    assert set([int(column.replace("x", "")) for column in data_columns]) <= set(range(1750, 2023)), error
    error = "Columns in bunkers table have changed."
    assert [column for column in tb_bunkers.columns if column not in data_columns] == [
        "em",
        "iso",
        "sector",
        "units",
    ], error
    assert set([int(column.replace("x", "")) for column in data_columns]) <= set(range(1750, 2023)), error

    # Check that sectors are as expected.
    error = "List of subsectors in the detailed table has changed."
    all_subsectors = sum(list(SECTOR_MAPPING.values()), [])
    assert set(tb_detailed["sector"]) == set(all_subsectors), error
    error = "List of subsectors in the bunkers table has changed."
    assert set(tb_bunkers["sector"]) == set(BUNKERS_SECTORS), error
    assert set(BUNKERS_SECTORS) < set(all_subsectors), error

    # Check that units are as expected.
    error = "Each pollutant was expected to have just one unit."
    assert (
        tb_detailed.groupby("em", as_index=False, observed=True).agg({"units": "nunique"})["units"] == 1
    ).all(), error
    assert (
        tb_bunkers.groupby("em", as_index=False, observed=True).agg({"units": "nunique"})["units"] == 1
    ).all(), error
    error = "Pollutant units have changed."
    assert tb_detailed[["em", "units"]].drop_duplicates().set_index(["em"])["units"].to_dict() == EXPECTED_UNITS, error
    # Specifically, check that all units are in kilotonnes.
    error = "Expected units to be in kilotonnes."
    assert all([unit.startswith("kt") for unit in EXPECTED_UNITS.values()]), error

    # Check that countries are as expected.
    error = "Detailed table was expected to have all countries in the bunkers table. This has changed (not important, simply check it and redefine this assertion)."
    assert set(tb_detailed["country"]) - set(tb_bunkers["iso"]) == set(), error
    error = "Bunkers table was expected to have all countries in the detailed table, except Palestine. This has changed (not important, simply check it redefine this assertion)."
    assert set(tb_bunkers["iso"]) - set(tb_detailed["country"]) == set(["pse"]), error

    # Check that in the detailed table, domestic aviation emissions for "global" are exactly zero.
    error = (
        "Expected 'global' emissions for domestic aviation to be zero in the detailed table and in the bunkers table."
    )
    assert (
        tb_detailed[(tb_detailed["sector"] == SUBSECTOR_DOMESTIC_AVIATION) & (tb_detailed["country"] == "global")][
            data_columns
        ]
        .sum()
        .sum()
        == 0
    ), error
    assert (
        tb_bunkers[(tb_bunkers["sector"] == SUBSECTOR_DOMESTIC_AVIATION) & (tb_bunkers["iso"] == "global")][
            data_columns
        ]
        .sum()
        .sum()
        == 0
    ), error
    # -> We can safely remove "global" domestic aviation from both tables.
    # TODO: Ensure at the end there is an aggregated global total for domestic emissions.

    # Check that national domestic aviation emissions given in the bunkers table are the same as the ones given in the detailed table.
    # NOTE: Here, ignore "pse" (Palestine) in bunkers, which is not informed in the detailed table.
    detailed_domestic_aviation = (
        tb_detailed[(tb_detailed["sector"] == SUBSECTOR_DOMESTIC_AVIATION)]
        .drop(columns=["fuel"])
        .groupby(["em", "country", "sector", "units"], as_index=False, observed=True)
        .sum()
        .sort_values(["em", "country", "sector", "units"])
        .reset_index(drop=True)
    )
    bunkers_domestic_aviation = (
        tb_bunkers[(tb_bunkers["sector"] == SUBSECTOR_DOMESTIC_AVIATION) & (~tb_bunkers["iso"].isin(["pse"]))]
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
            error = f"Domestic aviation data from detailed table differs from bunkers table more than expected (column {column})."
            assert (
                100
                * abs(detailed_domestic_aviation[column] - bunkers_domestic_aviation[column].fillna(0.0))
                / (detailed_domestic_aviation[column] + 1e-7)
            ).max() < 1e-4, error
    # -> We can safely remove all domestic aviations from the detailed table, since the bunkers table contains all national data (plus Palestine).

    # Check that the detailed table contains international aviation and shipping only for "global".
    error = "International aviation and shipping was expected to be informed only for 'global' in the detailed table."
    for sector in ["1A3ai_International-aviation", "1A3di_International-shipping"]:
        assert set(tb_detailed[tb_detailed["sector"] == sector]["country"]) == set(["global"]), error

    # Check that the only nonzero "global" information in the bunkers table is for international shipping (representing "Other" emissions that cannot be allocated to any country).
    error = "Expected 'global' in the bunkers table to be nonzero only for international shipping."
    assert set(
        tb_bunkers[(tb_bunkers["iso"] == "global") & (tb_bunkers[[c for c in data_columns]].sum(axis=1) > 0)]["sector"]
    ) == set(["1A3di_International-shipping"]), error
    # -> We can rename "global" emissions in the bunkers table as "Other" for international shipping, and remove all other "global" emissions (which are zero, namely for domestic and international aviation).

    # Check that the "global" emissions in the detailed table for international shipping and international aviation are the sum of all national emissions (given in the bunkers table).
    bunkers_international_transport = (
        tb_bunkers[tb_bunkers["sector"].isin(["1A3ai_International-aviation", "1A3di_International-shipping"])]
        .drop(columns=["iso"])
        .groupby(["em", "sector", "units"])
        .sum()
    )
    detailed_international_transport = (
        tb_detailed[(tb_detailed["sector"].isin(["1A3ai_International-aviation", "1A3di_International-shipping"]))]
        .drop(columns=["country", "fuel"])
        .groupby(["em", "sector", "units"])
        .sum()
    )
    bunkers_international_transport.equals(detailed_international_transport)
    assert bunkers_international_transport.shape == detailed_international_transport.shape
    for column in detailed_international_transport.columns:
        if column.startswith("x"):
            error = f"International transport data from detailed table differs from bunkers table more than expected (column {column})."
            assert (
                100
                * abs(detailed_international_transport[column] - bunkers_international_transport[column].fillna(0.0))
                / (detailed_international_transport[column] + 1e-7)
            ).max() < 1e-4, error
    # -> We can safely remove international aviation and shipping from the detailed table, since the bunkers table contains all national data. We will create a region aggregate afterwards to account for global emissions.

    # So, combining all the conclusions above:
    # -> We can safely remove all data for domestic aviation, international aviation, and international shipping from the detailed table.

    # TODO: In the detailed table, check that there is a "global" aggregate for all sectors, and that it adds up to exactly the same as the sum of all countries (except for international shipping and aviation, where it doesn't exist at the national level).

    # Check where there is nonzero global data in the detailed table.
    # In principle, I understood that only international aviation and shipping should be informed for "global" in the detailed table.
    # However, it seems that also 1A3di_Oil_Tanker_Loading is informed.
    # This will be included as part of international shipping.
    # TODO: Note that, for NMVOC, this is informed from 1960, and exactly zero before that. This create an abrupt jump in international shipping (but probably small in the true global total of NMVOC).
    # TODO: It seems that also other pollutants are nonzero from 1960 in the detailed table for "global". By eye, they seem to be a very small contribution, possibly spurious.
    # So, add the appropriate sanity checks and consider removing these points or adding them as "Other".
    # global_data = tb_detailed[(tb_detailed["country"]=="global") & (tb_detailed[data_columns] > 0).any(axis=1) & (~tb_detailed["sector"].isin(["1A3ai_International-aviation", "1A3di_International-shipping"]))]
    # error = "Expected global data to be "
    # assert global_data[[column for column in data_columns if int(column.replace("x", "")) < 1960]].sum().sum() == 0


def combine_detailed_and_bunkers_tables(tb_detailed: Table, tb_bunkers: Table) -> Table:
    # This function combines the detailed and bunkers tables.
    # The decisions are based on the results of "sanity_check_inputs" and the content of the README file in the bunkers zip folder.

    # Firstly, we don't need to keep the fuel information from the detailed table.
    # Drop the fuel column and sum over all other dimensions.
    # NOTE: This needs to be done before combining with bunkers, given that the latter does not contain fuel information.
    tb_detailed = (
        tb_detailed.drop(columns=["fuel"])
        .groupby(["em", "country", "sector", "units"], as_index=False, observed=True)
        .sum()
    )

    # Rename bunkers table consistently with the detailed table.
    tb_bunkers = tb_bunkers.rename(columns={"iso": "country"}, errors="raise")

    # The main source of confusion between both tables is related to (domestic and international) aviation and international shipping.
    # We concluded that this data can safely be removed from the detailed table, since
    # * For international aviation and shipping, the detailed table contains only "global" data, which is the sum of all national data in the bunkers table.
    # * For domestic aviation, the detailed table contains the same data as the bunkers table (except that the latter contains data for Palestine).
    # TODO: Create global variables for international aviation and shipping sector names and replace them everywhere.
    tb_detailed = tb_detailed[
        ~tb_detailed["sector"].isin(
            [SUBSECTOR_DOMESTIC_AVIATION, "1A3ai_International-aviation", "1A3di_International-shipping"]
        )
    ].reset_index(drop=True)

    # The "global" domestic aviation emissions are exactly zero on both tables, so we can safely remove them from the bunkers table.
    tb_bunkers = tb_bunkers[
        ~((tb_bunkers["sector"] == SUBSECTOR_DOMESTIC_AVIATION) & (tb_bunkers["country"] == "global"))
    ].reset_index(drop=True)

    # The "global" emissions in the bunkers table are the difference between total shipping fuel consumption (as estimated by the International Maritime Organization and other sources) and fuel consumption as reported by IEA. This additional fuel cannot be allocated to specific iso's. This correction to total fuel consumption is modest in recent years, but becomes much larger in earlier years.
    # In fact, the only nonzero "global" emissions in the bunkers table is for international shipping.
    # So, rename "global" in the bunkers table to "Other" (since it corresponds to a difference between estimates, that cannot be allocated to any country).
    tb_bunkers["country"] = tb_bunkers["country"].cat.rename_categories(lambda x: "Other" if x == "global" else x)
    # We already removed "global" domestic aviation emissions from the bunkers table (which were zero), so now remove also international aviation emissions (which are also zero).
    tb_bunkers = tb_bunkers[
        ~((tb_bunkers["country"] == "Other") & (tb_bunkers["sector"].isin(["1A3ai_International-aviation"])))
    ].reset_index(drop=True)
    # Sanity check.
    assert tb_bunkers[tb_bunkers["country"] == "global"].empty
    assert set(tb_bunkers[tb_bunkers["country"] == "Other"]["sector"]) == set(["1A3di_International-shipping"])

    # Manually create "global" aggregates for domestic aviation, international aviation, and international shipping in the bunkers table.
    tb_bunkers = pr.concat(
        [
            tb_bunkers,
            tb_bunkers.drop(columns=["country"])
            .groupby(["em", "sector", "units"], as_index=False, observed=True)
            .sum()
            .assign(**{"country": "global"}),
        ]
    )

    # Combine detailed table (where domestic aviation, international aviation, and international shipping were removed) and bunkers tables (which now contains all national and global data on domesetic aviation, international aviation, and international shipping).
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

    # Combine detailed and bunkers tables.
    tb = combine_detailed_and_bunkers_tables(tb_detailed=tb_detailed, tb_bunkers=tb_bunkers)

    # Simplify subsectors into broader categories, and remove the "fuel" dimension, which for now we don't need.
    tb = remap_table_categories(tb=tb)

    # Restructure table to have year as a column.
    tb = tb.rename(columns={column: int(column[1:]) for column in tb.columns if column.startswith("x")})
    tb = tb.melt(id_vars=["pollutant", "country", "sector"], var_name="year", value_name="emissions")

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # TODO: It seems that CH4 and N20 have only non-zero data from 1970 onwards, and all data is exactly zero right before this year.
    # These are probably spurious zeros. So, assert that prior to 1970 all data for these two pollutants is zero, and remove those points.
    # TODO: I noticed that World is smaller than individual countries, e.g. for NOx all sectors. What's going on here, is "global" only given for certain sectors? If so, recalculate aggregate.

    # Convert units from kilotonnes to tonnes.
    tb["emissions"] *= 1e3

    # Create an "All sectors" aggregate.
    tb = pr.concat(
        [
            tb,
            tb.groupby(["pollutant", "country", "year"], as_index=False, observed=True)
            .agg({"emissions": "sum"})
            .assign(**{"sector": "All sectors"})
            .assign(**{"sector": "All sectors"}),
        ]
    )

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
    tb = tb.drop(columns=["population"])

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
