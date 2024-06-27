import json
from pathlib import Path
from typing import Any, List, Optional, Tuple, cast

import owid.catalog.processing as pr
import pandas as pd

# from deaths import process as process_deaths
# from demographics import process as process_demographics
# from dep_ratio import process as process_depratio
# from fertility import process as process_fertility
from owid.catalog import Table
from shared import harmonize_dimension

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEAR_SPLIT = 2024
COLUMNS_INDEX = ["country", "year", "sex", "age", "variant"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("un_wpp")

    # Load tables
    tb_population = ds_meadow["population"].reset_index()
    tb_population_density = ds_meadow["population_density"].reset_index()
    tb_growth_rate = ds_meadow["growth_rate"].reset_index()
    tb_nat_change = ds_meadow["natural_change_rate"].reset_index()
    tb_fertility = ds_meadow["fertility_rate"].reset_index()
    tb_migration = ds_meadow["net_migration"].reset_index()
    tb_migration_rate = ds_meadow["net_migration_rate"].reset_index()
    tb_deaths = ds_meadow["deaths"].reset_index()
    tb_death_rate = ds_meadow["death_rate"].reset_index()
    tb_births = ds_meadow["births"].reset_index()
    tb_birth_rate = ds_meadow["birth_rate"].reset_index()
    tb_median_age = ds_meadow["median_age"].reset_index()
    tb_le = ds_meadow["life_expectancy"].reset_index()

    #
    # Process data.
    #
    tb_population, tb_sex_ratio = process_population_sex_ratio(tb_population, tb_population_density)
    tb_growth_rate = process_standard(tb_growth_rate)
    tb_nat_change = process_standard(tb_nat_change)
    tb_migration = process_migration(tb_migration, tb_migration_rate)
    del tb_migration_rate
    tb_deaths = process_deaths(tb_deaths, tb_death_rate)
    del tb_death_rate
    tb_births = process_births(tb_births, tb_birth_rate)
    del tb_birth_rate
    tb_median_age = process_standard(tb_median_age)
    tb_fertility = process_standard(tb_fertility)
    tb_le = process_le(tb_le)

    # Drop 55-59 age group in fertility (is all zero!)
    assert (
        tb_fertility.loc[tb_fertility["age"] == "55-59", "fertility_rate"] == 0
    ).all(), "Unexpected non-zero fertility rate values for age group 55-59."
    tb_fertility = tb_fertility.loc[tb_fertility["age"] != "55-59"]
    # Drop 55-59 age group in births (is all zero!)
    assert (
        tb_births.loc[tb_births["age"] == "55-59", "births"] == 0
    ).all(), "Unexpected non-zero births values for age group 55-59."
    tb_births = tb_births.loc[tb_births["age"] != "55-59"]

    # Split estimates vs. projections
    tb_population = set_variant_to_estimates(tb_population)
    tb_growth_rate = set_variant_to_estimates(tb_growth_rate)
    tb_nat_change = set_variant_to_estimates(tb_nat_change)
    tb_fertility = set_variant_to_estimates(tb_fertility)
    tb_migration = set_variant_to_estimates(tb_migration)
    tb_deaths = set_variant_to_estimates(tb_deaths)
    tb_births = set_variant_to_estimates(tb_births)
    tb_median_age = set_variant_to_estimates(tb_median_age)
    tb_le = set_variant_to_estimates(tb_le)
    tb_sex_ratio = set_variant_to_estimates(tb_sex_ratio)

    # Particular processing
    tb_nat_change["natural_change_rate"] /= 10

    # Format
    tb_population = tb_population.format(COLUMNS_INDEX)
    tb_growth_rate = tb_growth_rate.format(COLUMNS_INDEX)
    tb_nat_change = tb_nat_change.format(COLUMNS_INDEX)
    tb_fertility = tb_fertility.format(COLUMNS_INDEX)
    tb_migration = tb_migration.format(COLUMNS_INDEX, short_name="migration")
    tb_deaths = tb_deaths.format(COLUMNS_INDEX, short_name="deaths")
    tb_births = tb_births.format(COLUMNS_INDEX, short_name="births")
    tb_median_age = tb_median_age.format(COLUMNS_INDEX)
    tb_le = tb_le.format(COLUMNS_INDEX)
    tb_sex_ratio = tb_sex_ratio.format(COLUMNS_INDEX, short_name="sex_ratio")

    # Build tables list for dataset
    tables = [
        tb_population,
        tb_growth_rate,
        tb_nat_change,
        tb_fertility,
        tb_migration,
        tb_deaths,
        tb_births,
        tb_median_age,
        tb_le,
        tb_sex_ratio,
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_population_sex_ratio(tb: Table, tb_density: Table) -> Tuple[Table, Table]:
    """Process the population table."""
    paths.log.info("Processing population variables...")

    # Sanity check
    assert tb.notna().all(axis=None), "Some NaNs detected"

    # Scale
    tb["population"] *= 1000

    # Remove location_type
    tb = tb.drop(columns="location_type")

    # Estimate sex ratio
    tb_sex = estimate_sex_ratio(tb)

    # Estimate age groups
    tb = estimate_age_groups(tb)

    # Add population change
    tb = add_population_change(tb)

    # Add sex ratio
    tb_sex = add_sex_ratio_all(tb_sex, tb)

    # Harmonize country names
    tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)
    tb_sex = geo.harmonize_countries(tb_sex, countries_file=paths.country_mapping_path)

    # Harmonize dimensions
    tb = harmonize_dimension(
        tb,
        "variant",
        mapping={"Medium variant": "medium"},
    )
    tb_sex = harmonize_dimension(
        tb_sex,
        "variant",
        mapping={"Medium variant": "medium"},
    )
    tb = harmonize_dimension(
        tb,
        "sex",
        {
            "Female": "female",
            "Male": "male",
            "Total": "all",
        },
    )

    # Multiply sex_ratio x 100
    tb_sex["sex_ratio"] *= 100

    # Add population density
    tb_density = process_standard(tb_density)
    tb = tb.merge(tb_density, on=COLUMNS_INDEX, how="left")
    del tb_density

    return tb, tb_sex


def process_migration(tb_mig: Table, tb_mig_rate: Table) -> Table:
    """Process the migration tables.

    Clean the individual tables and combine them into one with two indicators.
    """
    # Basic processing
    tb_mig = process_standard(tb_mig)
    tb_mig_rate = process_standard(tb_mig_rate)

    # Standardise sex dimension values
    tb_mig = harmonize_dimension(
        tb_mig,
        "sex",
        mapping={
            "Female": "female",
            "Male": "male",
            "Total": "all",
        },
    )

    # Merge
    tb = tb_mig.merge(tb_mig_rate, on=COLUMNS_INDEX, how="left")

    return tb


def process_deaths(tb: Table, tb_rate: Table) -> Table:
    """Process the migration tables.

    Clean the individual tables and combine them into one with two indicators.
    """
    # Basic processing
    tb = process_standard(tb)
    tb_rate = process_standard(tb_rate)

    # Standardise sex dimension values
    tb = harmonize_dimension(
        tb,
        "sex",
        mapping={
            "Female": "female",
            "Male": "male",
            "Total": "all",
        },
    )
    tb = harmonize_dimension(
        tb,
        "age",
        mapping={
            "Total": "all",
        },
        strict=False,
    )

    # Add 10-year age groups from 20 to 100
    age_group_mapping = {
        key: value
        for i in range(20, 100, 10)
        for key, value in {f"{i}-{i+4}": f"{i}-{i+9}", f"{i+5}-{i+9}": f"{i}-{i+9}"}.items()
    }
    tb_10 = tb.copy()
    tb_10["age"] = tb_10["age"].map(age_group_mapping)
    tb_10 = cast(Table, tb_10.dropna(subset=["age"]))
    tb_10 = tb_10.groupby(COLUMNS_INDEX, as_index=False, observed=True)["deaths"].sum()
    tb = pr.concat([tb, tb_10], ignore_index=True)

    # Scale
    tb["deaths"] *= 1000

    # Merge
    tb = tb.merge(tb_rate, on=COLUMNS_INDEX, how="left")

    return tb


def process_births(tb: Table, tb_rate: Table) -> Table:
    """Process the migration tables.

    Clean the individual tables and combine them into one with two indicators.
    """
    # Basic processing
    tb = process_standard(tb)
    tb_rate = process_standard(tb_rate)

    # Standardise sex dimension values
    tb = harmonize_dimension(
        tb,
        "sex",
        mapping={
            "Female": "female",
            "Male": "male",
            "Total": "all",
        },
    )
    tb = harmonize_dimension(
        tb,
        "age",
        mapping={
            "Total": "all",
        },
        strict=False,
    )

    # Scale
    tb["births"] *= 1000

    # Merge
    tb = tb.merge(tb_rate, on=COLUMNS_INDEX, how="left")

    return tb


def process_le(tb: Table) -> Table:
    """Process the life expectancy table."""
    # Basic processing
    tb = process_standard(tb)

    assert set(tb["age"].unique()) == {str(i) for i in range(0, 100, 5)} | {"1", "100+"}, "Unexpected age values!"

    # Replace 100+ with 100
    tb["age"] = tb["age"].replace("100+", "100").astype(int)

    # Estimate actual life expectancy (not years left)
    tb["life_expectancy"] += tb["age"]

    return tb


def process_standard(tb: Table) -> Table:
    """Process the population table."""
    paths.log.info("Processing population variables...")

    # Sanity check
    assert tb.notna().all(axis=None), "Some NaNs detected"

    # Remove location_type
    tb = tb.drop(columns="location_type")

    # Harmonize country names
    tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)

    # Harmonize dimensions
    tb = harmonize_dimension(
        tb,
        "variant",
        mapping={"Medium variant": "medium"},
    )

    # Add missing dimensions
    if "sex" not in tb.columns:
        tb["sex"] = "all"
    if "age" not in tb.columns:
        tb["age"] = "all"

    return tb


def estimate_sex_ratio(tb: Table, age_groups: Optional[List[str]] = None):
    # Select relevant age groups
    if age_groups is None:
        age_groups = ["0", "5", "10", "15"] + [str(i) for i in range(20, 100, 10)] + ["100+"]

    tb_sex = tb.loc[(tb["age"].isin(age_groups)) & (tb["sex"] != "Total")].copy()
    # Pivot
    tb_sex = tb_sex.pivot(columns="sex", index=[col for col in COLUMNS_INDEX if col != "sex"], values="population")
    # Estimate ratio
    tb_sex["sex_ratio"] = tb_sex["Male"] / tb_sex["Female"]
    # Reset index
    tb_sex = tb_sex.reset_index()
    # Add missing sex column
    tb_sex["sex"] = "all"
    # Keep relevant columns
    tb_sex = tb_sex[COLUMNS_INDEX + ["sex_ratio"]]

    return tb_sex


def set_variant_to_estimates(tb: Table) -> Table:
    """For data before YEAR_SPLIT, make sure to have variant = 'estimates'."""
    tb["variant"] = tb["variant"].astype("string")
    tb.loc[tb["year"] < YEAR_SPLIT, "variant"] = "estimates"
    return tb


# Population
def estimate_age_groups(tb: Table) -> Table:
    """Estimate population values for all age groups."""
    tb_ = tb.copy()

    # List with all agge groups
    tbs = []

    # 1/ Basic age groups
    age_map = {
        **{str(i): f"{i - i%5}-{i + 4 - i%5}" for i in range(0, 100)},
        **{"100+": "100+"},
    }
    tb_basic = tb_.assign(age=tb_.age.map(age_map))
    tb_basic = tb_basic.groupby(
        ["country", "year", "sex", "age", "variant"],
        as_index=False,
        observed=True,
    )["population"].sum()
    tbs.append(tb_basic)

    # 2/ Additional age groups (NOTE: they are not disjoint!)
    age_groups = [
        0,
        1,
        (1, 4),  # 1-4
        (0, 14),  # 0-14
        (0, 24),  # 0-24
        (5, 14),  # 5-14
        (15, 24),  # 15-24
        (15, 64),  # 15-64
        (20, 29),  # 20-29
        (25, 64),  # 25-64
        (30, 39),  # 30-39
        (40, 49),  # 40-49
        (50, 59),  # 50-59
        (60, 69),  # 60-69
        (70, 79),  # 70-79
        (80, 89),  # 80-89
        (90, 99),  # 90-99
        (15, 200, "15+"),  # 15+
        (18, 200, "18+"),  # 18+
        (65, 200, "65+"),  # 65+
    ]
    for age_group in age_groups:
        if isinstance(age_group, int):
            tb_age = tb_[tb_.age == str(age_group)].copy()
        else:
            tb_age = _add_age_group(tb_, *age_group)

        tbs.append(tb_age)

    # 3/ All-age group
    tb_all = (
        tb_.groupby(
            ["country", "year", "sex", "variant"],
            as_index=False,
            observed=True,
        )["population"]
        .sum()
        .assign(age="all")
    )
    tbs.append(tb_all)

    # 4/ Merge all age groups
    tb_population = pr.concat(
        tbs,
        ignore_index=True,
    )

    return tb_population


def _add_age_group(tb: Table, age_min: int, age_max: int, age_group: Optional[str] = None) -> Table:
    """Estimate a new age group."""
    # Get subset of entries, apply groupby-sum if needed
    if age_min == age_max:
        tb_age = tb.loc[tb["age"] == str(age_min)].copy()
    else:
        ages_accepted = [str(i) for i in range(age_min, age_max + 1)]
        tb_age = tb.loc[tb["age"].isin(ages_accepted)].drop(columns="age").copy()

        tb_age = tb_age.groupby(
            ["country", "year", "sex", "variant"],
            as_index=False,
            observed=True,
        )["population"].sum()

    # Assign age group name
    if age_group:
        tb_age["age"] = age_group
    else:
        tb_age["age"] = f"{age_min}-{age_max}"

    return tb_age


def add_population_change(tb: Table) -> Table:
    """Estimate anual population change."""
    column_pop_change = "population_change"

    # Sort by year
    tb.sort_values("year")

    # Estimate population change
    pop_change = tb.groupby(["country", "sex", "age", "variant"])["population"].diff()
    tb[column_pop_change] = pop_change

    # Sanity check
    assert (years := set(tb[tb[column_pop_change].isna()]["year"])) == {1950}, f"Other than year 1950 detected: {years}"

    return tb


def add_sex_ratio_all(tb_sex: Table, tb: Table) -> Table:
    """Estimate anual population change."""
    tb_sex_all = estimate_sex_ratio(tb, age_groups=["all"])
    tb_sex = pr.concat([tb_sex, tb_sex_all])
    del tb_sex_all

    return tb_sex


#################################################################################
#################################################################################
# Old code below. Left in case it's needed for reference.
#################################################################################
#################################################################################
METRIC_CATEGORIES = {
    "migration": [
        "net_migration",
        "net_migration_rate",
    ],
    "fertility": [
        "fertility_rate",
        "births",
        "birth_rate",
    ],
    "population": [
        "population",
        "population_density",
        "population_change",
        "population_broad",
    ],
    "mortality": [
        "deaths",
        "death_rate",
        "life_expectancy",
        "child_mortality_rate",
        "infant_mortality_rate",
    ],
    "demographic": [
        "median_age",
        "growth_natural_rate",
        "growth_rate",
        "sex_ratio",
    ],
}


def merge_dfs(dfs: List[Table]) -> Table:
    """Merge all datasets"""
    df = pr.concat(dfs, ignore_index=True)
    # Fix variant name
    df.loc[df.year < YEAR_SPLIT, "variant"] = "estimates"
    # Index
    df = df.set_index(["location", "year", "metric", "sex", "age", "variant"], verify_integrity=True)
    df = df.dropna(subset=["value"])
    # df = df.sort_index()
    return df


def load_country_mapping() -> Any:
    with open(Path(__file__).parent / "un_wpp.countries.json") as f:
        return json.load(f)


def get_wide_df(df: pd.DataFrame) -> pd.DataFrame:
    df_wide = df.reset_index()
    df_wide = df_wide.pivot(
        index=["location", "year", "sex", "age", "variant"],
        columns="metric",
        values="value",
    )
    return df_wide


# def run_old(dest_dir: str) -> None:
#     ds = paths.load_dataset("un_wpp")
#     # country rename
#     paths.log.info("Loading country standardised names...")
#     country_std = load_country_mapping()
#     # pocess
#     paths.log.info("Processing population variables...")
#     df_population_granular, df_population = process_population(ds["population"], country_std)
#     paths.log.info("Processing fertility variables...")
#     df_fertility = process_fertility(ds["fertility"], country_std)
#     paths.log.info("Processing demographics variables...")
#     df_demographics = process_demographics(ds["demographics"], country_std)
#     paths.log.info("Processing dependency_ratio variables...")
#     df_depratio = process_depratio(ds["dependency_ratio"], country_std)
#     paths.log.info("Processing deaths variables...")
#     df_deaths = process_deaths(ds["deaths"], country_std)
#     # merge main df
#     paths.log.info("Merging tables...")
#     df = merge_dfs([df_population, df_fertility, df_demographics, df_depratio, df_deaths])
#     # create tables
#     table_long = df.update_metadata(
#         short_name="un_wpp",
#         description=(
#             "Main UN WPP dataset by OWID. It comes in 'long' format, i.e. column"
#             " 'metric' gives the metric name and column 'value' its corresponding"
#             " value."
#         ),
#     )
#     # generate sub-datasets
#     tables = []
#     for category, metrics in METRIC_CATEGORIES.items():
#         paths.log.info(f"Generating table for category {category}...")
#         tables.append(
#             df.query(f"metric in {metrics}")
#             .copy()
#             .update_metadata(
#                 short_name=category,
#                 description=f"UN WPP dataset by OWID. Contains only metrics corresponding to sub-group {category}.",
#             )
#         )
#     # add dataset with single-year age group population
#     cols_index = ["location", "year", "metric", "sex", "age", "variant"]
#     df_population_granular = df_population_granular.set_index(cols_index, verify_integrity=True)
#     tables.append(
#         df_population_granular.update_metadata(
#             short_name="population_granular",
#             description=(
#                 "UN WPP dataset by OWID. Contains only metrics corresponding to population for all dimensions (age and"
#                 " sex groups)."
#             ),
#         )
#     )
#     tables.append(table_long)

#     # create dataset
#     ds_garden = create_dataset(dest_dir, tables, default_metadata=ds.metadata)
#     ds_garden.save()
