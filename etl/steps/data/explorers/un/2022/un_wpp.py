import re
import sys
from copy import deepcopy
from typing import Any, List

import pandas as pd
import structlog
from owid import catalog
from tqdm.auto import tqdm

from etl.helpers import PathFinder
from etl.paths import DATA_DIR

N = PathFinder(__file__)
YEAR_SPLIT = 2022
no_dim_keyword = "full"
log = structlog.get_logger()


def _load_dataset() -> catalog.Dataset:
    dataset_garden_latest_dir = DATA_DIR / "garden" / "un" / "2022-07-11" / "un_wpp"
    dataset_garden = catalog.Dataset(dataset_garden_latest_dir)
    return dataset_garden


def _keep_relevant_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Only keep relevant rows."""
    log.info("Filtering relevant rows...")
    df = df.reset_index()
    df = df.loc[
        df.variant.isin(["estimates", "low", "medium", "high"])
        & -(df.metric.isin(["net_migration", "net_migration_rate"]) & (df.location == "World"))
    ].reset_index(drop=True)
    return df


def _organize_variants(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Filling all gaps (variants)...")
    df = pd.concat(
        [
            df.loc[df.year < YEAR_SPLIT].assign(variant="records").astype({"variant": "category"}),
            df.loc[df.year < YEAR_SPLIT].assign(variant="low").astype({"variant": "category"}),
            df.loc[df.year < YEAR_SPLIT].assign(variant="medium").astype({"variant": "category"}),
            df.loc[df.year < YEAR_SPLIT].assign(variant="high").astype({"variant": "category"}),
            df.loc[df.year >= YEAR_SPLIT],
        ]
    ).astype(
        {"variant": "category"}
    )  # type: ignore
    return df


def _pivot_df(df: pd.DataFrame) -> pd.DataFrame:
    # Pivot
    log.info("Pivoting table...")

    def _build_column_name(mcol: List[str]) -> Any:
        col = f"{mcol[0]}__{mcol[1]}__{mcol[2]}__{mcol[3]}"
        return catalog.utils.underscore(name=col, validate=True)

    df = df.dropna(how="all")
    df = df.pivot(
        index=["location", "year"],
        columns=["metric", "sex", "age", "variant"],
        values="value",
    )
    df = df.dropna(how="all")

    df.columns = df.columns.map(_build_column_name)
    return df


def _extract_dimension_values(
    df: pd.DataFrame, by_metric: bool, by_sex: bool, by_age: bool, by_variant: bool
) -> List[List[Any]]:
    groups_all = []
    regex = r"(.*)__(.*)__(.*)__(.*)"
    for col in df.columns:
        groups = list(re.search(regex, col).groups())  # type: ignore
        if not by_metric:
            groups[0] = no_dim_keyword
        if not by_sex:
            groups[1] = no_dim_keyword
        if not by_age:
            groups[2] = no_dim_keyword
        if not by_variant:
            groups[3] = no_dim_keyword
        groups_all.append(groups)
    groups_all = [list(x) for x in set(tuple(x) for x in groups_all)]
    return groups_all


def _init_dataset_explorer(dataset_garden: catalog.Dataset, dest_dir: str) -> catalog.Dataset:
    # Initialize new garden dataset.
    dataset = catalog.Dataset.create_empty(dest_dir)
    # Add dataset metadata.
    dataset.metadata = deepcopy(dataset_garden.metadata)
    dataset.metadata.version = N.version
    dataset.save()
    return dataset


def _build_table_variable(df: pd.DataFrame, metric: str, sex: str, age: str, variant: str) -> pd.DataFrame:
    short_name = f"{metric}__{sex}__{age}__{variant}"
    # Filter
    metric = metric if metric != no_dim_keyword else ".*"
    sex = sex if sex != no_dim_keyword else ".*"
    age = age if age != no_dim_keyword else ".*"
    variant = variant if variant != no_dim_keyword else ".*"
    regex = rf"{metric}__{sex}__{age}__{variant}"
    columns = list(df.filter(regex=regex).columns)
    df_ = df[columns].dropna(how="all")
    df_ = df_.sort_index()
    # Metadata
    df_.metadata.short_name = short_name
    return df_


def run(dest_dir: str) -> None:
    # Load table
    dataset_garden = _load_dataset()
    df = dataset_garden["un_wpp"]
    # Keep relevant rows for explorer
    df = _keep_relevant_rows(df)
    # Add estimates to projections timeseries
    df = _organize_variants(df)
    # Pivot
    df = _pivot_df(df)
    # Create dataset
    dataset_explorer = _init_dataset_explorer(dataset_garden, dest_dir)

    # Export
    dimension_values = _extract_dimension_values(df, by_metric=True, by_sex=True, by_age=False, by_variant=True)
    # for metric, sex, age, variant in dimension_values:
    for metric, sex, age, variant in tqdm(dimension_values, file=sys.stdout):
        # Table per variable (and dimensions)
        table = _build_table_variable(df, metric, sex, age, variant)
        # Add table to dataset.
        dataset_explorer.add(table, formats=["csv"])
