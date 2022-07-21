from typing import List, Any


import pandas as pd
from pathlib import Path

from etl.paths import BASE_DIR as base_path
from owid import catalog
from owid.catalog import Table
from owid.catalog.meta import TableMeta

from .population import process as process_population
from .fertility import process as process_fertility
from .demographics import process as process_demographics
from .dep_ratio import process as process_depratio
from .deaths import process as process_deaths

YEAR_SPLIT = 2022


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


def merge_dfs(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Merge all datasets"""
    df = pd.concat(dfs, ignore_index=True)
    # Fix variant name
    df.loc[df.year < YEAR_SPLIT, "variant"] = "estimates"
    # Index
    df = df.set_index(["location", "year", "metric", "sex", "age", "variant"])
    df = df.dropna(subset=["value"])
    # df = df.sort_index()
    return df


def df_to_table(df: pd.DataFrame, **kwargs: Any) -> Table:
    """DataFrame to Table"""
    t = Table(df, metadata=TableMeta(**kwargs))
    return t


def load_country_mapping() -> Any:
    return (
        pd.read_csv(
            Path(__file__).parent / f"{Path(__file__).stem}.country_std.csv",
            index_col="Country",
        )
        .squeeze()
        .to_dict()
    )


def get_wide_df(df: pd.DataFrame) -> pd.DataFrame:
    df_wide = df.reset_index()
    df_wide = df_wide.pivot(
        index=["location", "year", "sex", "age", "variant"],
        columns="metric",
        values="value",
    )
    return df_wide


def dataset_to_garden(tables: List[Table], metadata: TableMeta, dest_dir: str) -> None:
    """Push dataset to garden"""
    ds_garden = catalog.Dataset.create_empty(dest_dir)
    ds_garden.metadata = metadata
    ds_garden.save()
    # Add tables
    for table in tables:
        ds_garden.add(table)
        ds_garden.save()


def run(dest_dir: str) -> None:
    meadow_path = base_path / "data/meadow/un/2022/un_wpp"
    ds = catalog.Dataset(meadow_path)
    # country rename
    country_std = load_country_mapping()
    # pocess
    df_population = process_population(ds["population"], country_std)
    df_fertility = process_fertility(ds["fertility"], country_std)
    df_demographics = process_demographics(ds["demographics"], country_std)
    df_depratio = process_depratio(ds["dependency_ratio"], country_std)
    df_deaths = process_deaths(ds["deaths"], country_std)
    # merge main df
    df = merge_dfs(
        [df_population, df_fertility, df_demographics, df_depratio, df_deaths]
    )
    # create tables
    table_long = df_to_table(
        df,
        short_name="long",
        description=(
            "Main UN WPP dataset by OWID. It comes in 'long' format, i.e. column"
            " 'metric' gives the metric name and column 'value' its corresponding"
            " value."
        ),
    )
    # generate sub-datasets
    tables = []
    for category, metrics in metric_categories.items():
        df_c = df.query(f"metric in {metrics}")
        tables.append(
            df_to_table(
                df_c,
                short_name=category,
                description=(
                    "UN WPP dataset by OWID. Contains only metrics corresponding to"
                    f" sub-group {category}."
                ),
            )
        )
    tables += [table_long]
    # create dataset
    dataset_to_garden([table_long], ds.metadata, dest_dir)
