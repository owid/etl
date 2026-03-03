"""Garden step for Ember's Yearly Electricity Data (combining global and European data)."""

from typing import Dict

import owid.catalog.processing as pr
from owid.catalog import Table, utils
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder

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
    # - NEW (2026-01-26): European data has duplication in Power sector emissions where aggregate variables (Clean, Fossil, Renewables, Wind and solar, etc.) appear in both 'Fuel' and 'Aggregate fuel' subcategories with identical values. We keep only 'Aggregate fuel'.
    # - NEW (2026-01-26): European data no longer includes 'Gas and other fossil' aggregate, so we create it by summing Gas + Other fossil.
    tb_global = tb_global.copy()
    tb_europe = tb_europe.copy()

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

    # Remove duplicates in European Power sector emissions data.
    # NEW in 2026-01-26: European data now only provides aggregate emissions (Clean, Fossil, Renewables, etc.) and no longer includes individual fuel emissions (Bioenergy, Gas, Nuclear, etc.). All aggregate emissions appear in both 'Fuel' and 'Aggregate fuel' subcategories (complete duplication).
    # We keep only 'Aggregate fuel' subcategory and remove all 'Fuel' rows.
    europe_emissions = tb_europe[tb_europe["category"] == "Power sector emissions"]
    fuel_vars = set(europe_emissions[europe_emissions["subcategory"] == "Fuel"]["variable"].unique())
    agg_vars = set(europe_emissions[europe_emissions["subcategory"] == "Aggregate fuel"]["variable"].unique())
    overlapping_vars = fuel_vars & agg_vars
    error = "Expected overlap between 'Fuel' and 'Aggregate fuel' subcategories in European Power sector emissions."
    assert overlapping_vars, error

    # Remove 'Fuel' subcategory rows for Power sector emissions (they're all duplicates of aggregate fuel')
    mask_duplicates = (tb_europe["category"] == "Power sector emissions") & (tb_europe["subcategory"] == "Fuel")
    tb_europe = tb_europe[~mask_duplicates].copy()

    # For both Electricity generation and Power sector emissions, rename the subcategory from "Aggregate fuel" to "Fuel" for Coal and Wind.
    # This harmonizes with global data structure where Coal and Wind are in "Fuel" subcategory.
    mask_coal_wind = (
        (tb_europe["category"].isin(["Electricity generation", "Power sector emissions"]))
        & (tb_europe["subcategory"] == "Aggregate fuel")
        & (tb_europe["variable"].isin(["Coal", "Wind"]))
    )
    tb_europe.loc[mask_coal_wind, "subcategory"] = "Fuel"

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
    # NOTE: This can only be created for Electricity generation, not Power sector emissions,
    # because European emissions data doesn't include individual fuel emissions (Gas, Other fossil, etc.).
    error = "Expected European data to not include 'Gas and other fossil' variable."
    assert not (tb_europe["variable"] == "Gas and other fossil").any(), error
    tb_europe_gas_and_other_fossil = (
        tb_europe[
            (tb_europe["variable"].isin(["Gas", "Other fossil"]))
            & (tb_europe["category"] == "Electricity generation")
            & (tb_europe["unit"] == "TWh")
        ]
        .groupby(["country", "year", "unit", "category"], as_index=False)
        .agg({"value": "sum"})
        .assign(**{"variable": "Gas and other fossil", "subcategory": "Aggregate fuel"})
    )
    tb_europe = pr.concat([tb_europe, tb_europe_gas_and_other_fossil], ignore_index=True)

    # Check that the category-subcategory-variable groups are compatible between global and European data.
    # NEW in 2026-01-26: European data no longer includes individual fuel emissions, only aggregates.
    # So we need to exclude individual fuel emissions from the global data when comparing.
    individual_fuel_emissions = ["Bioenergy", "Gas", "Hydro", "Nuclear", "Other fossil", "Other renewables", "Solar"]

    set_global = set(
        [
            t["category"] + " - " + t["subcategory"] + " - " + t["variable"]
            for _, t in tb_global[
                (tb_global["category"] != "Capacity")
                & (tb_global["variable"] != "Total emissions")
                &
                # Exclude individual fuel emissions from Power sector emissions (not available in European data)
                ~(
                    (tb_global["category"] == "Power sector emissions")
                    & (tb_global["subcategory"] == "Fuel")
                    & (tb_global["variable"].isin(individual_fuel_emissions))
                )
                &
                # Also exclude "Gas and other fossil" from Power sector emissions (can't be created for European data)
                ~(
                    (tb_global["category"] == "Power sector emissions")
                    & (tb_global["variable"] == "Gas and other fossil")
                )
            ][["category", "subcategory", "variable"]]
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

    # Log the differences for debugging if they don't match
    if set_global != set_europe:
        diff_global = set_global - set_europe
        diff_europe = set_europe - set_global
        error_msg = "After adapting European data, category-subcategory-variables should match, except for:\n"
        error_msg += "* Capacity and total emissions (only in global)\n"
        error_msg += "* Hard coal, Lignite, Onshore wind, Offshore wind (only in European)\n"
        error_msg += "* Individual fuel emissions (Bioenergy, Gas, Hydro, Nuclear, Other fossil, Other renewables, Solar) in Power sector emissions (only in global)\n"
        if diff_global:
            error_msg += f"\nIn GLOBAL but NOT in EUROPEAN ({len(diff_global)} items):\n"
            error_msg += "\n".join(f"  - {item}" for item in sorted(diff_global)[:10])
            if len(diff_global) > 10:
                error_msg += f"\n  ... and {len(diff_global) - 10} more"
        if diff_europe:
            error_msg += f"\n\nIn EUROPEAN but NOT in GLOBAL ({len(diff_europe)} items):\n"
            error_msg += "\n".join(f"  - {item}" for item in sorted(diff_europe)[:10])
            if len(diff_europe) > 10:
                error_msg += f"\n  ... and {len(diff_europe) - 10} more"
        raise AssertionError(error_msg)

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
        min_num_countries_informed=5,
        min_frac_countries_informed=0.8,
        min_num_values_per_year=1,
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
            .agg({"value": lambda x: x.sum(skipna=False)})["value"]
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
    # https://files.ember-energy.org/public-downloads/ember_electricity_data_methodology.pdf
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


def fix_incomplete_aggregates(tables):
    # Prior to 2000, only EU countries have data. All other aggregates are incomplete, and show a spurious jump between 1999 and 2000. So, we will remove the data of all aggregates prior to 2000, except EU.
    regions_to_remove = [region for region in REGIONS if region != "European Union (27)"]
    for table_name in tables:
        tables[table_name].loc[
            (tables[table_name].index.get_level_values(0).isin(regions_to_remove))
            & (tables[table_name].index.get_level_values(1) < 2000),
            :,
        ] = None


def run() -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its main table.
    ds_meadow = paths.load_dataset("yearly_electricity")
    tb_global = ds_meadow.read("yearly_electricity__global")
    tb_europe = ds_meadow.read("yearly_electricity__europe")

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

    # Split data into different tables, one per category, and process each one individually.
    tables = {
        "capacity": make_wide_table(tb=tb, category="Capacity"),
        "electricity_demand": make_table_electricity_demand(tb=tb),
        "electricity_generation": make_wide_table(tb=tb, category="Electricity generation"),
        "electricity_imports": make_wide_table(tb=tb, category="Electricity imports"),
        "lifecycle_emissions": make_wide_table(tb=tb, category="Power sector emissions"),
    }

    # Improve table formats.
    for table_name in tables:
        tables[table_name] = tables[table_name].format(short_name=table_name)

    # Remove data for aggregate regions with incomplete data.
    fix_incomplete_aggregates(tables)

    ####################################################################################################################
    # 2026-01-26 Since the EU has data for 2025, but not other European countries, the aggregates for Europe and High-income countries present abrupt jumps between 2024 and 2025.
    # Hence we remove the data for 2025 for these regions.
    error = "Expected jump in Europe wind generation (and other variables in the latest year). They have changed. Inspect and consider removing this code."
    assert tables["electricity_generation"].loc["Europe"]["wind__twh"].diff().iloc[-1] < -14, error
    for table_name in tables:
        latest_year = tables[table_name].reset_index()["year"].max()
        for column in tables[table_name].columns:
            for region in ["Europe", "High-income countries"]:
                tables[table_name].loc[(region, latest_year), column] = None

    # For some reason, Europe's "other_renewables" also has poor coverage in 2024.
    error = "Expected poor data coverage of other renewables for Europe in 2024. Data has changed; consider removing this code."
    _tb = tables["electricity_generation"].dropna(subset="other_renewables__twh").reset_index()
    assert len(set(_tb[_tb["year"] == 2023]["country"])) > 170, error
    assert len(set(_tb[_tb["year"] == 2024]["country"])) < 84, error
    tables["electricity_generation"].loc[("Europe", 2024), ["other_renewables__twh", "other_renewables__pct"]] = None
    ####################################################################################################################

    # Combine all tables into one.
    tb_combined = combine_yearly_electricity_data(tables=tables)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_combined])
    ds_garden.save()
