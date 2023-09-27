import json
from pathlib import Path
from typing import Any, List

import owid.catalog.processing as pr
import pandas as pd
import structlog
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

from .deaths import process as process_deaths
from .demographics import process as process_demographics
from .dep_ratio import process as process_depratio
from .fertility import process as process_fertility
from .population import process as process_population

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEAR_SPLIT = 2022

log = structlog.get_logger()

metric_categories = {
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
    df = df.set_index(["location", "year", "metric", "sex", "age", "variant"])
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


def run(dest_dir: str) -> None:
    ds = paths.load_dataset("un_wpp")
    # country rename
    log.info("Loading country standardised names...")
    country_std = load_country_mapping()
    # pocess
    log.info("Processing population variables...")
    df_population_granular, df_population = process_population(ds["population"], country_std)
    log.info("Processing fertility variables...")
    df_fertility = process_fertility(ds["fertility"], country_std)
    log.info("Processing demographics variables...")
    df_demographics = process_demographics(ds["demographics"], country_std)
    log.info("Processing dependency_ratio variables...")
    df_depratio = process_depratio(ds["dependency_ratio"], country_std)
    log.info("Processing deaths variables...")
    df_deaths = process_deaths(ds["deaths"], country_std)
    # merge main df
    log.info("Merging tables...")
    df = merge_dfs([df_population, df_fertility, df_demographics, df_depratio, df_deaths])
    # create tables
    table_long = df.update_metadata(
        short_name="un_wpp",
        description=(
            "Main UN WPP dataset by OWID. It comes in 'long' format, i.e. column"
            " 'metric' gives the metric name and column 'value' its corresponding"
            " value."
        ),
    )
    # generate sub-datasets
    tables = []
    for category, metrics in metric_categories.items():
        log.info(f"Generating table for category {category}...")
        tables.append(
            df.query(f"metric in {metrics}")
            .copy()
            .update_metadata(
                short_name=category,
                description=f"UN WPP dataset by OWID. Contains only metrics corresponding to sub-group {category}.",
            )
        )
    # add dataset with single-year age group population
    tables.append(
        df_population_granular.update_metadata(
            short_name="population_granular",
            description=(
                "UN WPP dataset by OWID. Contains only metrics corresponding to population for all dimensions (age and"
                " sex groups)."
            ),
        )
    )
    tables.append(table_long)

    # create dataset
    ds_garden = create_dataset(dest_dir, tables, default_metadata=ds.metadata)
    ds_garden.save()
