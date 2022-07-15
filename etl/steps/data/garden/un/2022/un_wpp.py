from owid import catalog
from typing import List


import pandas as pd
from etl.paths import BASE_DIR as base_path

from .population import process as process_population
from .fertility import process as process_fertility
from .demographics import process as process_demographics
from .dep_ratio import process as process_depratio
from .deaths import process as process_deaths

YEAR_SPLIT = 2022


def merge_dfs(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(dfs, ignore_index=True)
    # Fix variant name
    df.loc[df.year < YEAR_SPLIT, "variant"] = "estimates"
    # Sort rows
    df = df.sort_values(["location", "year", "metric", "sex", "age", "variant"])
    # Reset index
    df = df.reset_index(drop=True)
    # Types
    df = df.astype(
        {
            "location": "str",
            "year": int,
            "metric": "str",
            "age": "str",
            "variant": "str",
        }
    ).astype({"age": "str"})
    return df


def run(dest_dir: str) -> None:
    meadow_path = base_path / "data/meadow/un/2022/un_wpp"
    ds = catalog.Dataset(meadow_path)
    # country rename
    country_std = (
        pd.read_csv(
            base_path / "etl/steps/data/garden/un/2022/un_wpp.country_std.csv",
            index_col="Country",
        )
        .squeeze()
        .to_dict()
    )

    df_population = process_population(ds["population"], country_std)
    df_fertility = process_fertility(ds["fertility"], country_std)
    df_demographics = process_demographics(ds["demographics"], country_std)
    df_depratio = process_depratio(ds["dependency_ratio"], country_std)
    df_deaths = process_deaths(ds["deaths"], country_std)

    df = merge_dfs(
        [df_population, df_fertility, df_demographics, df_depratio, df_deaths]
    )
    df = df.dropna(subset=["value"])
