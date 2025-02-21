"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import map_series

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Sector mapping.
# Define the name of the domestic aviation subsector (which will need to be handled separately).
SUBSECTOR_INTERNATIONAL_AVIATION = "1A3ai_International-aviation"
SUBSECTOR_INTERNATIONAL_SHIPPING = "1A3di_International-shipping"
SUBSECTOR_DOMESTIC_AVIATION = "1A3aii_Domestic-aviation"
SUBSECTOR_OIL_TANKER_LOADING = "1A3di_Oil_Tanker_Loading"

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
        # NOTE: The following subsectors 7BC and 1A4c were found in the data, but not in the mapping. I found this other document:
        # https://essd.copernicus.org/preprints/essd-2023-306/essd-2023-306-supplement.pdf
        # which places these subsectors under "Agriculture soils".
        # Regarding 7BC, I understand that, despite originating from non-agricultural Nitrogen sources, these emissions are closely linked to the broader nitrogen cycle, which is heavily influenced by agricultural activities.
        "7BC_Indirect-N2O-non-agricultural-N",
        # Regarding 1A4C, I understand that "Agriculture" is mostly about emissions from biological processes, whereas 1A4C is about energy processes in farming, forestry and fishing. However, 1A4C is still more closely related to agriculture than energy or any other sector.
        # Another option would be to create a separate sector for "Farming, forestry and fishing", but I think that would not be very useful.
        "1A4c_Agriculture-forestry-fishing",
    ],
    # In the document, they had a sector "Aviation", containing international aviation and domestic aviation. But I think it's better to separate them.
    # "Aviation": [
    #     SUBSECTOR_INTERNATIONAL_AVIATION,
    #     SUBSECTOR_DOMESTIC_AVIATION,
    # ],
    "International aviation": [SUBSECTOR_INTERNATIONAL_AVIATION],
    "Domestic aviation": [SUBSECTOR_DOMESTIC_AVIATION],
    # In the document, they had a sector "Residential, Commercial, Other (DOM)", containing:
    # "1A4a_Commercial-institutional",
    # "1A4b_Residential",
    # "1A4c_Agriculture-forestry-fishing",
    # "1A5_Other-unspecified",
    # However, this is not a particularly useful categorization.
    # Instead, I'll create a "Buildings" category, move "1A4c_Agriculture-forestry-fishing" into "Agriculture" and create a new "Other fuel use" sector.
    "Buildings": [
        "1A4a_Commercial-institutional",
        "1A4b_Residential",
    ],
    # "Int. Shipping".
    "International shipping": [
        SUBSECTOR_INTERNATIONAL_SHIPPING,
        SUBSECTOR_OIL_TANKER_LOADING,
    ],
    # In the document, they had a sector called "Energy Transformation and Production (ENE)". We will simply call it "Energy".
    "Energy": [
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
        # NOTE: The following was found in the data, but not in the mapping. However, I found it in:
        # https://essd.copernicus.org/preprints/essd-2023-306/essd-2023-306-supplement.pdf
        # It was categorized under Power generation. So I suppose it makes sense to include it under "Energy".
        "1A5_Other-unspecified",
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

# Subsectors expected in the bunkers table.
BUNKERS_SECTORS = [
    SUBSECTOR_INTERNATIONAL_AVIATION,
    SUBSECTOR_DOMESTIC_AVIATION,
    SUBSECTOR_INTERNATIONAL_SHIPPING,
]

# Mapping of pollutants.
# NOTE: Map to None to ignore any of the pollutants.
POLLUTANTS_MAPPING = {
    "CH4": "CH₄",
    "NMVOC": "NMVOC",
    "N2O": "N₂O",
    "SO2": "SO₂",
    "CO": "CO",
    "BC": "BC",
    "NH3": "NH₃",
    "OC": "OC",
    "NOx": "NOₓ",
    # We will not include CO2. Even though it can contribute to local air pollution in a very secondary way, its impact is very low.
    # CO2's main concern is climate change, for which we already have a different explorer.
    # NOTE: the same logic does not apply to methane, which is a greenhouse gas but also a more significant contributor to local air pollution. There's a direct pathway for methane to produce O3, which doesn't happen to CO2.
    "CO2": None,
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

# Name for the additional "country" representing unallocated emissions (e.g. those included as "global" in the bunkers file).
# TODO: In the metadata, we should clarify that "Other" doesn't mean "Other countries", but rather "Not possible to allocate to individual countries".
ENTITY_FOR_UNALLOCATED_EMISSIONS = "Other"

# Regions to create when aggregating data.
REGIONS = {
    # Default continents.
    "Africa": {},
    "Asia": {},
    "Europe": {},
    "North America": {},
    "Oceania": {},
    "South America": {},
    # Income groups.
    "Low-income countries": {},
    "Upper-middle-income countries": {},
    "Lower-middle-income countries": {},
    "High-income countries": {},
    # Other special regions.
    "European Union (27)": {},
    # Global aggregate.
    # Include unallocated emissions in the global total.
    "World": {"additional_members": [ENTITY_FOR_UNALLOCATED_EMISSIONS]},
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
    for sector in [SUBSECTOR_INTERNATIONAL_AVIATION, SUBSECTOR_INTERNATIONAL_SHIPPING]:
        assert set(tb_detailed[tb_detailed["sector"] == sector]["country"]) == set(["global"]), error

    # Check that the only nonzero "global" information in the bunkers table is for international shipping (representing "Other" emissions that cannot be allocated to any country).
    error = "Expected 'global' in the bunkers table to be nonzero only for international shipping."
    assert set(
        tb_bunkers[(tb_bunkers["iso"] == "global") & (tb_bunkers[[c for c in data_columns]].sum(axis=1) > 0)]["sector"]
    ) == set([SUBSECTOR_INTERNATIONAL_SHIPPING]), error
    # -> We can rename "global" emissions in the bunkers table as "Other" for international shipping, and remove all other "global" emissions (which are zero, namely for domestic and international aviation).

    # Check that the "global" emissions in the detailed table for international shipping and international aviation are the sum of all national emissions given in the bunkers table.
    # NOTE: Here, the sum in bunkers international shipping will include the "global" component (which will be renamed "Other", as it corresponds to unallocated emissions).
    bunkers_international_transport = (
        tb_bunkers[tb_bunkers["sector"].isin([SUBSECTOR_INTERNATIONAL_AVIATION, SUBSECTOR_INTERNATIONAL_SHIPPING])]
        .drop(columns=["iso"])
        .groupby(["em", "sector", "units"])
        .sum()
    )
    detailed_international_transport = (
        tb_detailed[(tb_detailed["sector"].isin([SUBSECTOR_INTERNATIONAL_AVIATION, SUBSECTOR_INTERNATIONAL_SHIPPING]))]
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

    # Once domestic aviation, international aviation, and international shipping are removed, we would expect that there is no "global" data in the detailed table. However, that's not the case. There seems to be other sectors with nonzero "global" contribution in the detailed table.
    # One of those sectors is oil tanker loading, which contains a significant amount of emissions (and will be mapped within international shipping later). I will sanity check this one later.
    # For now, ignore oil tanker loading, and compare the rest of those potentially spurious "global" emissions with the aggregate of all other countries.
    global_exceptions = tb_detailed[
        ~(
            tb_detailed["sector"].isin(
                [
                    SUBSECTOR_INTERNATIONAL_AVIATION,
                    SUBSECTOR_INTERNATIONAL_SHIPPING,
                    SUBSECTOR_DOMESTIC_AVIATION,
                    SUBSECTOR_OIL_TANKER_LOADING,
                ]
            )
        )
        & (tb_detailed["country"] == "global")
        & (tb_detailed[data_columns] > 0).any(axis=1)
    ].drop(columns=["country", "fuel"])
    aggregate_exceptions = (
        tb_detailed[(tb_detailed["country"] != "global")]
        .drop(columns=["country", "fuel"])
        .groupby(["em", "sector", "units"], as_index=False, observed=True)
        .sum()
    )
    compared = global_exceptions.merge(
        aggregate_exceptions, how="inner", on=["em", "sector", "units"], suffixes=("_global", "_aggregate")
    )
    global_exceptions_values = compared[[column for column in compared.columns if column.endswith("_global")]].values
    aggregate_exceptions_values = compared[
        [column for column in compared.columns if column.endswith("_aggregate")]
    ].values
    error = "Expected that, when aggregating all emissions and getting a total of zero, 'global' (potentially spurious) emissions should also be zero. That's no longer the case."
    assert (
        len(global_exceptions_values[(global_exceptions_values != 0) & (aggregate_exceptions_values == 0)]) == 0
    ), error
    error = "Expected that the given (potentially spurious) global emissions should be a small percentage of the aggregate of emissions (except for oil tanker loading, and international shipping and aviation). That's no longer the case."
    assert (
        max(
            100
            * global_exceptions_values[aggregate_exceptions_values > 0]
            / aggregate_exceptions_values[aggregate_exceptions_values > 0]
        )
        < 0.03
    ), error
    # -> All "global" emissions in the detailed table (except for oil tanker loading, domestic aviation, international aviation, and international shipping) are < 0.03% of the aggregate of all emissions. So we can rename them as "Other".

    # Find nonzero contributions to oil tanker loading in the detailed table.
    oil_tanker = tb_detailed[
        (tb_detailed["sector"] == SUBSECTOR_OIL_TANKER_LOADING) & (tb_detailed[data_columns] > 0).any(axis=1)
    ][["em", "country", "fuel"]].drop_duplicates()
    error = "Nonzero data for oil tanker loading has changed. Adapt this sanity check."
    assert oil_tanker.values.tolist() == [
        ["BC", "usa", "process"],
        ["NMVOC", "global", "process"],
        ["OC", "usa", "process"],
    ], error
    # Oil tanker loading is zero for most countries-pollutants. It's only nonzero for the US (BC and OC) and for "global" (NMVOC). It doesn't make sense to have zero NMVOC from all countries, and yet have a "global" nonzero contribution. So it seems safe to consider this "global" contribution also as "Other".
    # -> Rename all remaining "global" data in the detailed table as "Other".


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
    tb_detailed = tb_detailed[
        ~tb_detailed["sector"].isin(
            [SUBSECTOR_DOMESTIC_AVIATION, SUBSECTOR_INTERNATIONAL_AVIATION, SUBSECTOR_INTERNATIONAL_SHIPPING]
        )
    ].reset_index(drop=True)

    # The "global" domestic aviation emissions are exactly zero on both tables, so we can safely remove them from the bunkers table.
    tb_bunkers = tb_bunkers[
        ~((tb_bunkers["sector"] == SUBSECTOR_DOMESTIC_AVIATION) & (tb_bunkers["country"] == "global"))
    ].reset_index(drop=True)

    # The "global" emissions in the bunkers table are the difference between total shipping fuel consumption (as estimated by the International Maritime Organization and other sources) and fuel consumption as reported by IEA. This additional fuel cannot be allocated to specific iso's. This correction to total fuel consumption is modest in recent years, but becomes much larger in earlier years.
    # In fact, the only nonzero "global" emissions in the bunkers table is for international shipping.
    # So, rename "global" in the bunkers table to "Other" (since it corresponds to a difference between estimates, that cannot be allocated to any country).
    tb_bunkers["country"] = tb_bunkers["country"].cat.rename_categories(
        lambda x: ENTITY_FOR_UNALLOCATED_EMISSIONS if x == "global" else x
    )
    # We already removed "global" domestic aviation emissions from the bunkers table (which were zero), so now remove also international aviation emissions (which are also zero).
    tb_bunkers = tb_bunkers[
        ~(
            (tb_bunkers["country"] == ENTITY_FOR_UNALLOCATED_EMISSIONS)
            & (tb_bunkers["sector"].isin([SUBSECTOR_INTERNATIONAL_AVIATION]))
        )
    ].reset_index(drop=True)
    # Sanity check.
    assert tb_bunkers[tb_bunkers["country"] == "global"].empty
    assert set(tb_bunkers[tb_bunkers["country"] == ENTITY_FOR_UNALLOCATED_EMISSIONS]["sector"]) == set(
        [SUBSECTOR_INTERNATIONAL_SHIPPING]
    )

    # Similarly, in the detailed table, after removing aviation and shipping, there is still some (almost negligible) "global" contribution in certain pollutants and sectors.
    # They may be spurious (and add up to less than 0.03% of the aggregate of all countries for each year-sector-pollutant).
    # There is only one exception: "global" oil tanker loading NMVOC is not negligible (and in fact, "global" is the only nonzero country).
    # So, instead of removing all those "global", we can simply rename them as "Other" (meaning: "Not allocated to any individual country").
    tb_detailed["country"] = tb_detailed["country"].cat.rename_categories(
        lambda x: ENTITY_FOR_UNALLOCATED_EMISSIONS if x == "global" else x
    )
    assert tb_detailed[tb_detailed["country"] == "global"].empty

    # Combine detailed table (where domestic aviation, international aviation, and international shipping were removed) and bunkers tables (which now contains all national and global data on domesetic aviation, international aviation, and international shipping).
    tb = pr.concat([tb_detailed, tb_bunkers], short_name=paths.short_name)

    # Remove units column.
    # NOTE: In the sanity checks, we asserted that they were as expected.
    tb = tb.drop(columns=["units"], errors="raise")

    # Sanity checks.
    error = "There are duplicated rows in the combined table."
    assert tb[tb.duplicated(subset=["em", "country", "sector"], keep=False)].empty, error
    error = "Expected no 'global' data in the combined table. Something is wrong in the processing."
    assert tb[tb["country"] == "global"].empty, error

    return tb


def remap_table_categories(tb: Table) -> Table:
    # We don't need the detailed sectorial information.
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
    # Remove any rows with pollutant None.
    tb = tb[tb["pollutant"].notnull()].reset_index(drop=True)

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
    # Sanity check inputs.
    sanity_check_inputs(tb_detailed=tb_detailed, tb_bunkers=tb_bunkers)

    # Combine detailed and bunkers tables (and remove the "fuel" dimension, which for now we don't need).
    tb = combine_detailed_and_bunkers_tables(tb_detailed=tb_detailed, tb_bunkers=tb_bunkers)

    # Simplify subsectors into broader categories.
    tb = remap_table_categories(tb=tb)

    # Restructure table to have year as a column.
    tb = tb.rename(columns={column: int(column[1:]) for column in tb.columns if column.startswith("x")})
    tb = tb.melt(id_vars=["pollutant", "country", "sector"], var_name="year", value_name="emissions")

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # TODO: I have found a few data issues:
    # * BC waste becomes flat from 2014 onwards.
    # * Note that, NMVOC oil tanker loading is informed from 1960, and exactly zero before that. This create an abrupt jump in international shipping (but maybe small in the true global total of NMVOC).

    ####################################################################################################################
    # We have detected some potential data issues.
    # * CH4 and N20 emissions are exactly zero before 1970, and non-zero data from exactly 1970 onwards, creating an abrupt jump on that year.
    # I suppose the zeros prior to 1970 are spurious, so they will be removed.
    error = "Expected CH4 and N2O emissions to be exactly zero before 1970, and nonzero from 1970 on. Data has changed."
    assert tb[(tb["pollutant"].isin(["CH₄", "N₂O"])) & (tb["year"] < 1970)]["emissions"].sum() == 0, error
    assert tb[(tb["pollutant"].isin(["CH₄", "N₂O"])) & (tb["year"] >= 1970)]["emissions"].sum() > 0, error
    tb = tb.loc[~((tb["pollutant"].isin(["CH₄", "N₂O"])) & (tb["year"] < 1970)), :].reset_index(drop=True)
    ####################################################################################################################

    # Create an "All sectors" aggregate.
    tb = pr.concat(
        [
            tb,
            tb.groupby(["pollutant", "country", "year"], as_index=False, observed=True)
            .agg({"emissions": "sum"})
            .assign(**{"sector": "All sectors"}),
        ]
    )

    # Add region aggregates.
    tb = geo.add_regions_to_table(
        tb=tb,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        index_columns=["country", "year", "pollutant", "sector"],
    )

    # Add per capita variables.
    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, warn_on_missing_countries=False)
    tb["emissions_per_capita"] = tb["emissions"] / tb["population"]
    tb = tb.drop(columns=["population"])

    # Convert emissions units from kilotonnes to tonnes.
    tb["emissions"] *= 1e3

    # Convert per capita emissions units from kilotonnes to kilograms.
    tb["emissions_per_capita"] *= 1e6

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
