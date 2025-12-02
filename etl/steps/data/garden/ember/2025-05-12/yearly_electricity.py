"""Garden step for Ember's Yearly Electricity Data (combining global and European data)."""

from typing import Dict

import owid.catalog.processing as pr
from owid.catalog import Table, utils
from owid.datautils.dataframes import combine_two_overlapping_dataframes, map_series
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factors.
# Terawatt-hours to kilowatt-hours.
TWH_TO_KWH = 1e9
# Megatonnes to grams.
MT_TO_G = 1e12

# Columns to use from Ember's yearly electricity data, and how to rename them.
COLUMNS_YEARLY_ELECTRICITY = {
    "area": "country",
    "year": "year",
    "variable": "variable",
    "value": "value",
    "unit": "unit",
    "category": "category",
    "subcategory": "subcategory",
}
# Categories expected to exist in the data.
CATEGORIES_GLOBAL = [
    "Capacity",
    "Electricity demand",
    "Electricity generation",
    "Electricity imports",
    "Power sector emissions",
]
CATEGORIES_EUROPE = [
    "Electricity demand",
    "Electricity generation",
    "Electricity imports",
    "Power sector emissions",
]
# Subcategories expected to exist in the data.
SUBCATEGORIES = [
    "Aggregate fuel",
    "CO2 intensity",
    "Demand",
    "Demand per capita",
    "Electricity imports",
    "Fuel",
    "Total",
]
# Variables expected to exist in the data.
VARIABLES_GLOBAL = [
    "Bioenergy",
    "CO2 intensity",
    "Clean",
    "Coal",
    "Demand",
    "Demand per capita",
    "Fossil",
    "Gas",
    "Gas and Other Fossil",
    "Hydro",
    "Hydro, Bioenergy and Other Renewables",
    "Net Imports",
    "Nuclear",
    "Other Fossil",
    "Other Renewables",
    "Renewables",
    "Solar",
    "Total Generation",
    "Total emissions",
    "Wind",
    "Wind and Solar",
]
VARIABLES_EUROPE = [
    "Bioenergy",
    "CO2 intensity",
    "Clean",
    "Coal",
    "Demand",
    "Demand per capita",
    "Fossil",
    "Gas",
    "Hard coal",
    "Hydro",
    "Hydro, bioenergy and other renewables",
    "Lignite",
    "Net imports",
    "Nuclear",
    "Offshore wind",
    "Onshore wind",
    "Other fossil",
    "Other renewables",
    "Renewables",
    "Solar",
    "Total generation",
    "Wind",
    "Wind and solar",
]

# Regions for which aggregates will be created.
REGIONS = {
    "Africa": {},
    "Asia": {},
    "Europe": {},
    "North America": {},
    "Oceania": {},
    "South America": {},
    "European Union (27)": {},
    "Low-income countries": {},
    "Upper-middle-income countries": {},
    "Lower-middle-income countries": {},
    "High-income countries": {},
}


def sanity_check_inputs(tb_global: Table, tb_europe: Table) -> None:
    assert set(tb_global.columns) == set(tb_europe.columns), "Columns in global and European data have changed."
    assert set(tb_global["category"]) == set(CATEGORIES_GLOBAL), "Categories have changed in data."
    assert set(tb_europe["category"]) == set(CATEGORIES_EUROPE), "Categories have changed in data."
    assert set(tb_global["subcategory"]) == set(SUBCATEGORIES), "Subcategories have changed in data."
    assert set(tb_europe["subcategory"]) == set(SUBCATEGORIES), "Subcategories have changed in data."
    assert set(tb_global["variable"]) == set(VARIABLES_GLOBAL), "Variables have changed in global data."
    assert set(tb_europe["variable"]) == set(VARIABLES_EUROPE), "Variables have changed in data."


def prepare_input_data(tb: Table) -> Table:
    tb = tb.copy()

    # Select and rename columns conveniently.
    tb = tb[list(COLUMNS_YEARLY_ELECTRICITY)].rename(columns=COLUMNS_YEARLY_ELECTRICITY, errors="raise")

    # Harmonize names of variables, categories and subcategories.
    for field in ["variable", "category", "subcategory"]:
        tb[field] = [value.capitalize().replace("Co2", "CO2") for value in tb[field]]

    # Harmonize country names.
    # For debugging, set warn_on_unused_countries to True
    # (the only unused country in global data should be "Türkiye", which is only used in European data).
    tb = paths.regions.harmonize_names(tb=tb, warn_on_missing_countries=True, warn_on_unused_countries=False)

    return tb


def combine_global_and_europe_data(tb_global: Table, tb_europe: Table) -> Table:
    # These are the main differences between the two datasets:
    # - Variables in global data are title-cased and in European data are sentence-cased.
    # - Global data includes category 'Capacity', while European does not.
    # - Global data includes variable 'Total emissions', while European does not.
    # - European data includes the following subcategory-variables pairs:
    #   'Aggregate fuel - Coal',
    #   'Aggregate fuel - Wind',
    #   'Fuel - Hard coal',
    #   'Fuel - Lignite',
    #   'Fuel - Offshore wind',
    #   'Fuel - Onshore wind',
    # - Meanwhile global data only includes:
    #   'Aggregate fuel - Gas and other fossil',
    #   'Fuel - Coal',
    #   'Fuel - Wind',
    # - Units differ slightly: for global, emissions and intensity units are called "mtCO2" and "gCO2/kWh", while for European data, units are called "MtCO2e" and "gCO2e per kWh". After inspection of a few countries, the values seem to be in good agreement, so the units are most likely the same. As explained in their methodology, it should refer to CO2 equivalents over a 100 year timescale:
    # https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/ember_electricity_data_methodology.pdf
    # - Global data includes data for all European countries from 2000 onwards. European data includes the same data from 2000 onwards, but also data from 1990 to 1999.

    error = "Variables in global and European data have changed."
    assert set(tb_global["variable"]) - set(tb_europe["variable"]) == {
        "Gas and other fossil",
        "Total emissions",
    }, error
    assert set(tb_europe["variable"]) - set(tb_global["variable"]) == {
        "Hard coal",
        "Lignite",
        "Offshore wind",
        "Onshore wind",
    }, error

    # The simplest solution regarding coal and wind is to rename the subcategory of European data from "Aggregate fuel" to "Fuel".
    tb_europe.loc[
        (tb_europe["subcategory"] == "Aggregate fuel") & (tb_europe["variable"].isin(["Coal", "Wind"])),
        "subcategory",
    ] = "Fuel"

    # Harmonize units. Arbitrarily, adopt the global ones.
    # NOTE: All units will be redefined at the end of this step, using the definitions in the accompanying meta.yaml file.
    error = "Units have changed."
    assert set(tb_europe[(tb_europe["category"] == "Power sector emissions")]["unit"]) == {
        "MtCO2e",
        "gCO2e per kWh",
    }, error
    assert set(tb_global[(tb_global["category"] == "Power sector emissions")]["unit"]) == {"mtCO2", "gCO2/kWh"}, error
    tb_europe.loc[tb_europe["unit"] == "MtCO2e", "unit"] = "mtCO2"
    tb_europe.loc[tb_europe["unit"] == "gCO2e per kWh", "unit"] = "gCO2/kWh"

    # Create the gas and other fossil aggregate for European data.
    error = "Expected European data to not include 'Gas and other fossil' variable."
    assert not (tb_europe["variable"] == "Gas and other fossil").any(), error
    tb_europe_gas_and_other_fossil = (
        tb_europe[(tb_europe["variable"].isin(["Gas", "Other fossil"])) & (tb_europe["unit"].isin(["TWh", "mtCO2"]))]
        .groupby(["country", "year", "unit", "category"], as_index=False)
        .agg({"value": "sum"})
        .assign(**{"variable": "Gas and other fossil", "subcategory": "Aggregate fuel"})
    )
    tb_europe = pr.concat([tb_europe, tb_europe_gas_and_other_fossil], ignore_index=True)

    # Check that the category-subcategory-variable groups are now identical for global and European data.
    set_global = set(
        [
            t["category"] + " - " + t["subcategory"] + " - " + t["variable"]
            for _, t in tb_global[(tb_global["category"] != "Capacity") & (tb_global["variable"] != "Total emissions")][
                ["category", "subcategory", "variable"]
            ]
            .drop_duplicates()
            .iterrows()
        ]
    )
    set_europe = set(
        [
            t["category"] + " - " + t["subcategory"] + " - " + t["variable"]
            for _, t in tb_europe[
                ~tb_europe["variable"].isin(["Hard coal", "Lignite", "Onshore wind", "Offshore wind"])
            ][["category", "subcategory", "variable"]]
            .drop_duplicates()
            .iterrows()
        ]
    )
    assert (
        set_global == set_europe
    ), "After adapting European data, all category-subcategory-variables should be identical, except for:\n* Capacity and total emissions (only given in global), and\n* Hard coal, Lignite, Onshore wind and Offshore wind, only given in European data."

    # Combine the two overlapping datasets, prioritizing European on overlapping rows.
    tb = combine_two_overlapping_dataframes(
        df1=tb_europe,
        df2=tb_global,
        index_columns=["country", "year", "variable", "unit", "category", "subcategory"],
    )

    return tb


def make_wide_table(tb: Table, category: str) -> Table:
    """Convert data from long to wide format for a specific category.

    This is a common processing for all categories in the data.

    Parameters
    ----------
    tb : Table
        Data, after harmonizing country names.
    category : str
        Name of category (as defined above in CATEGORIES) to process.

    Returns
    -------
    table : Table
        Table in wide format.

    """
    # Select data for given category.
    _tb = tb[tb["category"] == category].copy()

    # Pivot table to have a column for each variable.
    table = _tb.pivot(
        index=["country", "year"],
        columns=["variable", "unit"],
        values="value",
        join_column_levels_with=" - ",
        fill_dimensions=False,
    )

    return table


def make_table_electricity_demand(tb: Table) -> Table:
    """Create table with processed data of category "Electricity demand".

    Parameters
    ----------
    tb : Table
        Data in long format for all categories, after harmonizing country names.

    Returns
    -------
    table : Table
        Table of processed data for the given category.

    """
    # Prepare wide table.
    table = make_wide_table(tb=tb, category="Electricity demand")

    # Add population to data
    table = paths.regions.add_population(tb=table, warn_on_missing_countries=False)

    # Recalculate demand per capita.
    # We could do this only for region aggregates (since they do not have per capita values),
    # but we do this for all countries, to ensure per-capita variables are consistent with our population data.
    table["Demand per capita - kWh"] = table["Demand - TWh"] * TWH_TO_KWH / table["population"]

    # Delete the original demand per capita column.
    table = table.drop(columns=["Demand per capita - MWh"], errors="raise")

    return table


def combine_yearly_electricity_data(tables: Dict[str, Table]) -> Table:
    """Combine all tables in Ember's Yearly Electricity Data into one table.

    Parameters
    ----------
    tables : List[Table]
        Yearly Electricity data (containing tables for capacity, electricity demand, generation, imports and
        emissions).

    Returns
    -------
    tb_combined : Table
        Combined table containing all data in the Yearly Electricity dataset.

    """
    category_renaming = {
        "capacity": "Capacity - ",
        "electricity_demand": "",
        "electricity_generation": "Generation - ",
        "electricity_imports": "",
        "lifecycle_emissions": "Emissions (lifecycle) - ",
        "direct_emissions": "Emissions (direct combustion) - ",
    }
    error = "Tables in yearly electricity dataset have changed"
    assert set(category_renaming) == set(tables), error
    index_columns = ["country", "year"]
    for table_name in list(tables):
        tables[table_name] = (
            tables[table_name]
            .reset_index()
            .rename(
                columns={
                    column: utils.underscore(category_renaming[table_name] + column)
                    for column in tables[table_name].columns
                    if column not in index_columns
                },
                errors="raise",
            )
        )

    # Merge all tables into one, with an appropriate short name.
    tb_combined = pr.multi_merge(list(tables.values()), on=index_columns, how="outer", short_name=paths.short_name)  # type: ignore

    # Rename certain columns for consistency.
    tb_combined = tb_combined.rename(
        columns={
            "net_imports__twh": "imports__total_net_imports__twh",
            "demand__twh": "demand__total_demand__twh",
            "demand_per_capita__kwh": "demand__total_demand_per_capita__kwh",
        },
        errors="raise",
    )

    # Remove unnecessary columns and any possible rows with no data.
    tb_combined = tb_combined.drop(columns=["population"], errors="raise").dropna(how="all")

    # Set a convenient index and sort rows and columns conveniently.
    tb_combined = tb_combined.format(sort_columns=True)

    return tb_combined


def add_region_aggregates(tb: Table) -> Table:
    # NOTE: For % variables and carbon intensities, aggregates will be recalculated later.
    tb = paths.regions.add_aggregates(
        tb=tb,
        index_columns=["country", "year", "variable", "unit", "category", "subcategory"],
        ignore_overlaps_of_zeros=True,
    )

    # Recalculate share variables for regions.
    # Add a temporary column for the total generation of each country-year.
    tb = tb.merge(
        tb[(tb["unit"] == "TWh") & (tb["variable"] == "Total generation")][["country", "year", "value"]].rename(
            columns={"value": "_total_generation"}
        ),
        on=["country", "year"],
        how="left",
    )
    # Add a temporary column for the generation of each country-year-variable.
    tb = tb.merge(
        tb[(tb["unit"] == "TWh")][["country", "year", "variable", "value"]].rename(columns={"value": "_generation"}),
        on=["country", "year", "variable"],
        how="left",
    )
    tb["_percentage"] = 100 * tb["_generation"] / tb["_total_generation"]
    select_regions_pct = (tb["unit"] == "%") & (tb["country"].isin(REGIONS))
    tb.loc[select_regions_pct, "value"] = tb.loc[select_regions_pct, "_percentage"]

    # Sanity check.
    assert (
        abs(
            tb[
                (tb["country"].isin(REGIONS))
                & (tb["unit"] == "%")
                & (
                    tb["variable"].isin(
                        [
                            "Bioenergy",
                            "Hydro",
                            "Nuclear",
                            "Solar",
                            "Wind",
                            "Gas",
                            "Coal",
                            "Other fossil",
                            "Other renewables",
                        ]
                    )
                )
            ]
            .groupby(["country", "year"])
            .agg({"value": "sum"})["value"]
            - 100
        )
        < 1
    ).all()

    # Add lifecycle carbon intensity for regions.
    # Add a temporary column for the total emissions of each country-year.
    tb = tb.merge(
        tb[(tb["unit"] == "mtCO2") & (tb["variable"] == "Total emissions")][["country", "year", "value"]].rename(
            columns={"value": "_total_emissions"}
        ),
        on=["country", "year"],
        how="left",
    )
    # Convert mtCO2 / TWh to gCO2/kWh.
    # X (1e12 gCO2 / 1 mtCO2) * (1 TWh / 1e9) = X * 1e3 gCO2/kWh
    tb["_intensity"] = 1e3 * tb["_total_emissions"] / tb["_total_generation"]
    select_regions_intensity = (tb["unit"] == "gCO2/kWh") & (tb["country"].isin(REGIONS))
    tb.loc[select_regions_intensity, "value"] = tb.loc[select_regions_intensity, "_intensity"]

    # Remove temporary columns.
    tb = tb.drop(
        columns=["_total_generation", "_generation", "_percentage", "_total_emissions", "_intensity"], errors="raise"
    )

    return tb


def replicate_ember_lifecycle_emissions(tb: Table) -> None:
    # To check we understand how lifecycle emissions are calculated by Ember, I'll calculate the emissions of a few sources, and compare the result with theirs.
    # The emission factors for coal, gas, nuclear and wind, as Ember's methodology explains, are a bit more complicated; they come from different sources and may change at the country level.
    # So we will not attempt to replicate those.

    # Let's take the lifecycle emission factors from Ember's methodology:
    # https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/ember_electricity_data_methodology.pdf
    lifecycle_factors = {"Bioenergy": 230, "Hydro": 24, "Solar": 48, "Other renewables": 38, "Other fossil": 700}
    # In principle, most of them come from the median valeus of Table A.III.2 of
    # https://www.ipcc.ch/site/assets/uploads/2018/02/ipcc_wg3_ar5_annex-iii.pdf
    # Indeed, the numbers for hydro and solar come from this table; hydro seems to be the sum of (infrastructure & supply chain emissions) + (biogenic CO2 emissions and albedo effect), rounded to two significant figures.
    # The value for "Other renewables" corresponds to the median value of geothermal.
    # The origin of the value of "Other fossil" is unclear.
    # We check that we can reproduce their results reasonably well.
    for source, emission_factor in lifecycle_factors.items():
        _tb = tb[(~tb["country"].isin(REGIONS)) & (tb["unit"] == "TWh") & (tb["variable"] == source)].reset_index(
            drop=True
        )
        # Generation is in TWh, and the emission factor is in gCO2e/kWh. To convert to MtCO2:
        # X TWH * (1e9 kWh / 1 TWh) * (1 MtCO2e / 1e12 gCO2e) * gCO2e / kWh = X * 1e-3 MtCO2e
        _tb["value"] *= emission_factor * 1e-3
        _tb = (
            tb[(tb["unit"] == "mtCO2") & (tb["variable"] == source) & (tb["value"] > 0)]
            .drop(columns=["unit", "category"])
            .merge(
                _tb.drop(columns=["unit", "category"]),
                on=["country", "year", "variable", "subcategory"],
                how="inner",
                suffixes=("_true", "_pred"),
            )
        )
        # The true values seem to be rounded to 2 decimals. I'll check that there are no instances where the true value of emissions differs from the predicted one by more than 2% and 0.02 in absolute value.
        error = f"Unable to reproduce Ember's lifecycle emissions for {source}"
        assert _tb[
            ((100 * abs(_tb["value_true"].round(2) - _tb["value_pred"].round(2)) / (_tb["value_true"].round(2))) > 15)
            & (abs(_tb["value_true"].round(2) - _tb["value_pred"].round(2)) > 0.02)
        ].empty, error


def estimate_emissions_and_carbon_intensity_of_direct_combustion(
    tb: Table, tb_electricity_factors: Table, tb_energy_factors: Table
) -> Table:
    # Create a harmonized table of electricity emission factors.
    # NOTE: Biomass has no factor; we simply assign zero
    tb_factors = (
        tb_electricity_factors[["source", "median_direct_combustion_emission_factor"]]
        .fillna(0)
        .rename(columns={"median_direct_combustion_emission_factor": "emission_factor"}, errors="raise")
    )
    # Add oil from the "Residual Fuel Oil" energy factor.
    tb_factors = pr.concat(
        [tb_factors, tb_energy_factors[tb_energy_factors["source"] == "Residual Fuel Oil"]], ignore_index=True
    )
    # Map ember sources to each of the emission factor sources.
    technology_to_emission_factor = {
        "Bioenergy": "Biomass-dedicated",
        "Coal": "Coal",
        "Gas": "Gas",
        "Hard coal": "Coal",
        "Hydro": "Hydropower",
        "Lignite": "Coal",
        "Nuclear": "Nuclear",
        "Offshore wind": "Wind offshore",
        "Onshore wind": "Wind onshore",
        "Other fossil": "Residual Fuel Oil",
        "Other renewables": "Geothermal",
        "Solar": "Solar PV—utility",
        "Wind": "Wind onshore",
    }
    error = "Incorrect mapping of emission factor source names."
    assert set(technology_to_emission_factor.values()) <= set(tb_factors["source"]), error

    # Aggregate sources and their individual components.
    # NOTE: Ensure that the components are not aggregates, to ensure they will always be found.
    aggregate_sources = {
        "Gas and other fossil": ["Gas", "Other fossil"],
        "Fossil": ["Coal", "Gas", "Other fossil"],
        "Wind and solar": ["Wind", "Solar"],
        "Hydro, bioenergy and other renewables": ["Hydro", "Bioenergy", "Other renewables"],
        "Renewables": ["Hydro", "Bioenergy", "Other renewables", "Wind", "Solar"],
        "Clean": ["Hydro", "Bioenergy", "Other renewables", "Wind", "Solar", "Nuclear"],
        "Total generation": [
            "Hydro",
            "Bioenergy",
            "Other renewables",
            "Wind",
            "Solar",
            "Nuclear",
            "Coal",
            "Gas",
            "Other fossil",
        ],
    }

    # Check that the list of individual and aggregate sources are as expected.
    error = "List of individual or aggregate sources may have changed."
    assert set(tb[(tb["category"] == "Electricity generation") & (tb["unit"] == "TWh")]["variable"]) == (
        set(technology_to_emission_factor) | set(aggregate_sources)
    ), error

    # Create a temporary table of electricity generation and emissions.
    tb_generation = tb[
        (tb["category"] == "Electricity generation")
        & (tb["unit"] == "TWh")
        & (tb["variable"].isin(list(technology_to_emission_factor)))
    ].reset_index(drop=True)
    assert set(tb_generation["subcategory"]) == {"Fuel"}

    # Add a column with emission factors.
    tb_generation["source"] = map_series(
        tb_generation["variable"],
        mapping=technology_to_emission_factor,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
    )
    tb_generation = tb_generation.merge(tb_factors, how="left", on=["source"]).drop(columns=["source"], errors="raise")
    assert tb_generation["emission_factor"].notnull().all()

    # Add a column with emissions.
    tb_generation["emissions"] = tb_generation["value"] * tb_generation["emission_factor"]
    # Calculate aggregate emissions (and also aggregate generation, for sanity checks).
    for aggregate, components in aggregate_sources.items():
        subcategory = "Total" if aggregate == "Total generation" else "Aggregate fuel"
        _tb_generation_aggregate = (
            tb_generation[(tb_generation["variable"].isin(components))]
            .groupby(["country", "year", "unit", "category"], as_index=False)
            .agg({"value": "sum", "emissions": "sum"})
            .assign(**{"variable": aggregate, "subcategory": subcategory})
        )
        tb_generation = pr.concat([tb_generation, _tb_generation_aggregate], ignore_index=True)

    # Add a column for carbon intensity of direct emissions.
    assert tb_generation[(tb_generation["emissions"] > 0) & (tb_generation["value"] == 0)].empty
    tb_generation["intensity"] = (tb_generation["emissions"] / tb_generation["value"]).fillna(0)

    # Check that the generation aggregates coincide with the originals.
    tb_generation = tb_generation.merge(
        tb[(tb["category"] == "Electricity generation") & (tb["unit"] == "TWh")].rename(
            columns={"value": "value_original"}
        ),
        on=["country", "year", "variable", "unit", "category", "subcategory"],
        how="left",
    )
    error = "Failed to reconstruct the original generation of aggregates."
    assert tb_generation[
        (100 * abs(tb_generation["value"] - tb_generation["value_original"]) / tb_generation["value_original"]) > 1
    ].empty, error
    # After this check, we can expect that the aggregates for direct emissions are also well calculated.

    # So now use the emission values to create a table of emissions.
    tb_emissions = (
        tb_generation.drop(columns=["unit", "category", "value", "value_original", "emission_factor", "intensity"])
        .rename(columns={"emissions": "value"}, errors="raise")
        .assign(**{"unit": "mtCO2", "category": "Direct emissions"})
    )
    tb_emissions.loc[(tb_emissions["variable"] == "Total generation"), "variable"] = "Total emissions"

    # The original data has one emissions entry for each energy source; however, it has only one intensity entry.
    # This must be based on the total emissions and generation.
    tb_intensity = (
        tb_generation[(tb_generation["variable"] == "Total generation")]
        .drop(columns=["unit", "category", "value", "value_original", "emission_factor", "emissions"])
        .rename(columns={"intensity": "value"}, errors="raise")
        .assign(**{"unit": "gCO2/kWh", "category": "Direct emissions"})
    )
    tb_intensity.loc[(tb_intensity["variable"] == "Total generation"), "variable"] = "CO2 intensity"

    # Check that clean variables have no emissions.
    _all_zero_variables = tb_emissions.groupby(["variable"], as_index=False).agg(
        {"value": lambda x: (sum(x) == 0).all()}
    )
    _all_zero_variables = set(_all_zero_variables[_all_zero_variables["value"] == 1]["variable"])
    assert _all_zero_variables == {
        "Bioenergy",
        "Clean",
        "Hydro",
        "Hydro, bioenergy and other renewables",
        "Nuclear",
        "Offshore wind",
        "Onshore wind",
        "Other renewables",
        "Renewables",
        "Solar",
        "Wind",
        "Wind and solar",
    }
    # To avoid having columns with only zeros, remove those variables.
    tb_emissions = tb_emissions[~tb_emissions["variable"].isin(_all_zero_variables)].reset_index(drop=True)

    # Append the new direct emissions and intensities to the original table.
    tb_emissions_and_intensities = pr.concat([tb_emissions, tb_intensity], ignore_index=True)

    return tb_emissions_and_intensities


def run() -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its main table.
    ds_meadow = paths.load_dataset("yearly_electricity")
    tb_global = ds_meadow.read("yearly_electricity__global")
    tb_europe = ds_meadow.read("yearly_electricity__europe")

    # Load emission factors dataset.
    ds_factors = paths.load_dataset("emission_factors")
    # Read the electricity emission factors table (to calculate emissions and intensity for gas and coal).
    tb_electricity_factors = ds_factors.read("electricity_emission_factors")
    # Read the energy emission factors table (to calculate emissions and intensity for other fossil).
    tb_energy_factors = ds_factors.read("energy_emission_factors")

    #
    # Process data.
    #
    # Sanity check inputs.
    sanity_check_inputs(tb_global=tb_global, tb_europe=tb_europe)

    # Prepare global and European input data.
    tb_global = prepare_input_data(tb=tb_global)
    tb_europe = prepare_input_data(tb=tb_europe)

    # Combine global and European data.
    tb = combine_global_and_europe_data(tb_global=tb_global, tb_europe=tb_europe)

    # Create region aggregates.
    tb = add_region_aggregates(tb=tb)

    # Sanity check: replicate Ember's lifecycle emissions for a few sources.
    replicate_ember_lifecycle_emissions(tb=tb)

    # Calculate emissions and carbon intensity of direct combustion.
    # NOTE: Ember provides only lifecycle emissions and intensities.
    tb_direct_emissions_and_intensities = estimate_emissions_and_carbon_intensity_of_direct_combustion(
        tb=tb, tb_electricity_factors=tb_electricity_factors, tb_energy_factors=tb_energy_factors
    )

    # Split data into different tables, one per category, and process each one individually.
    tables = {
        "capacity": make_wide_table(tb=tb, category="Capacity"),
        "electricity_demand": make_table_electricity_demand(tb=tb),
        "electricity_generation": make_wide_table(tb=tb, category="Electricity generation"),
        "electricity_imports": make_wide_table(tb=tb, category="Electricity imports"),
        "lifecycle_emissions": make_wide_table(tb=tb, category="Power sector emissions"),
        "direct_emissions": make_wide_table(tb=tb_direct_emissions_and_intensities, category="Direct emissions"),
    }

    for table_name in tables:
        # Prepare table's format.
        tables[table_name] = tables[table_name].format(short_name=table_name)

    ####################################################################################################################
    # The data for many regions presents a big drop in the last year, simply because many countries are not informed.
    # Assert that this drop exists, and remove the last data point for regions.
    error = (
        "Expected a big drop in the last data point for regions (because of limited data availability)."
        "If that is no longer the case, remove this part of the code and keep the last data points for regions."
    )
    assert tables["capacity"].loc["Africa"]["renewables__gw"].diff().iloc[-1] < -29, error
    for table_name in tables:
        latest_year = tables[table_name].reset_index()["year"].max()
        for column in tables[table_name].columns:
            for region in REGIONS:
                tables[table_name].loc[(region, latest_year), column] = None

    # Similarly, data prior to 2000 exists only for European countries.
    # This can cause spurious jump in aggregate data.
    # For example, there is a spurious jump from 1999 to 2000 for Upper-middle-income countries
    # (see e.g. "Renewables - TWh"), because prior to 2000 only a few UMI countries have data.
    # Assert that this jump exists, and remove aggregate data prior to 2000 (except European aggregates).
    error = (
        "Expected a big jump (>1000%) (in e.g. renewable generation) between 1999 and 2000 for Upper-middle-income "
        "countries (because prior to 2000 only Ukraine has data). If that is no longer the case (because not only "
        "European countries are informed prior to 2000), remove this part of the code."
    )
    renewables_umic_1999 = tables["electricity_generation"].loc["Upper-middle-income countries", 1999][
        "renewables__twh"
    ]
    renewables_umic_2000 = tables["electricity_generation"].loc["Upper-middle-income countries", 2000][
        "renewables__twh"
    ]
    assert 100 * (renewables_umic_2000 - renewables_umic_1999) / renewables_umic_1999 > 1000, error
    # We could still create European aggregates, but certain European countries are also missing data prior to 2000.
    # It seems safer to make nan all aggregate data in all yearly electricity tables prior to 2000.
    for table_name in tables:
        for column in tables[table_name].columns:
            tables[table_name].loc[
                (tables[table_name].index.get_level_values(0).isin(REGIONS))
                & (tables[table_name].index.get_level_values(1) < 2000),
                :,
            ] = None
    ####################################################################################################################

    # Combine all tables into one.
    tb_combined = combine_yearly_electricity_data(tables=tables)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_combined])
    ds_garden.save()
