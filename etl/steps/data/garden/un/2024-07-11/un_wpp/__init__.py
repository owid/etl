import json
from pathlib import Path
from typing import Any, List

import owid.catalog.processing as pr
import pandas as pd

# from deaths import process as process_deaths
# from demographics import process as process_demographics
# from dep_ratio import process as process_depratio
# from fertility import process as process_fertility
from owid.catalog import Table
from population import process as process_population
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
    tb_growth_rate = ds_meadow["growth_rate"].reset_index()
    tb_nat_change = ds_meadow["natural_change_rate"].reset_index()
    tb_fertility = ds_meadow["fertility_rate"].reset_index()
    tb_migration = ds_meadow["net_migration"].reset_index()
    tb_migration_rate = ds_meadow["net_migration_rate"].reset_index()

    #
    # Process data.
    #
    tb_population = process_population(tb_population)
    tb_growth_rate = process_standard(tb_growth_rate)
    tb_nat_change = process_standard(tb_nat_change)
    tb_fertility = process_standard(tb_fertility)
    tb_migration = process_migration(tb_migration, tb_migration_rate)

    # Split estimates vs. pro`ections
    tb_population = set_variant_to_estimates(tb_population)
    tb_growth_rate = set_variant_to_estimates(tb_growth_rate)
    tb_nat_change = set_variant_to_estimates(tb_nat_change)
    tb_fertility = set_variant_to_estimates(tb_fertility)
    tb_migration = set_variant_to_estimates(tb_migration)

    # Particular processing
    tb_nat_change["natural_change_rate"] /= 10

    # Format
    tb_population = tb_population.format(COLUMNS_INDEX)
    tb_growth_rate = tb_growth_rate.format(COLUMNS_INDEX)
    tb_nat_change = tb_nat_change.format(COLUMNS_INDEX)
    tb_fertility = tb_fertility.format(COLUMNS_INDEX)
    tb_migration = tb_migration.format(COLUMNS_INDEX, short_name="migration")

    # Build tables list for dataset
    tables = [
        tb_population,
        tb_growth_rate,
        tb_nat_change,
        tb_fertility,
        tb_migration,
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


def set_variant_to_estimates(tb: Table) -> Table:
    """For data before YEAR_SPLIT, make sure to have variant = 'estimates'."""
    tb["variant"] = tb["variant"].astype("string")
    tb.loc[tb["year"] < YEAR_SPLIT, "variant"] = "estimates"
    return tb


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
