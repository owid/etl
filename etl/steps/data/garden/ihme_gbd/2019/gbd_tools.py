import json
from pathlib import Path
from typing import List, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore, underscore_table
from owid.datautils import geo
from structlog import get_logger

from etl.paths import DATA_DIR

log = get_logger()


def create_units(df: pd.DataFrame) -> pd.DataFrame:

    conds = [
        ((df["measure"] == "DALYs (Disability-Adjusted Life Years)") & (df["metric"] == "Rate")),
        ((df["measure"] == "DALYs (Disability-Adjusted Life Years)") & (df["metric"] == "Number")),
        ((df["measure"] == "DALYs (Disability-Adjusted Life Years)") & (df["metric"] == "Percent")),
        ((df["measure"] == "Deaths") & (df["metric"] == "Number")),
        ((df["measure"] == "Deaths") & (df["metric"] == "Rate")),
        ((df["measure"] == "Deaths") & (df["metric"] == "Percent")),
        ((df["measure"] == "Prevalence") & (df["metric"] == "Number")),
        ((df["measure"] == "Prevalence") & (df["metric"] == "Rate")),
        ((df["measure"] == "Prevalence") & (df["metric"] == "Percent")),
        ((df["measure"] == "Incidence") & (df["metric"] == "Number")),
        ((df["measure"] == "Incidence") & (df["metric"] == "Rate")),
        ((df["measure"] == "Incidence") & (df["metric"] == "Percent")),
    ]

    choices = [
        "DALYs per 100,000 people",
        "DALYs",
        "%",
        "deaths",
        "deaths per 100,000 people",
        "%",
        "",
        "",
        "%",
        "",
        "",
        "%",
    ]
    df["unit"] = ""
    df["unit"] = np.select(conds, choices)
    return df


def load_excluded_countries(excluded_countries_path: Path) -> List[str]:
    with open(excluded_countries_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(excluded_countries_path: Path, df: pd.DataFrame) -> pd.DataFrame:
    excluded_countries = load_excluded_countries(excluded_countries_path)
    return cast(pd.DataFrame, df.loc[~df.country.isin(excluded_countries)])


def harmonize_countries(country_mapping_path: Path, df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(country_mapping_path))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {country_mapping_path} to include these country "
            f"names; or (b) add them to excluded_countries_path."
            f"Raw country names: {missing_countries}"
        )

    return df


def tidy_countries(country_mapping_path: Path, excluded_countries_path: Path, df: pd.DataFrame) -> pd.DataFrame:
    log.info("exclude_countries")
    df = exclude_countries(excluded_countries_path, df)
    log.info("harmonize_countries")
    df = harmonize_countries(country_mapping_path, df)
    return df


def clean_values(df: pd.DataFrame) -> pd.DataFrame:
    # round 'number' rows to integer and 'percent' and 'rate' to 2dp - Feel like there is maybe a nicer way to do this?
    df = df.drop(columns=["upper", "lower"])
    df = df.copy(deep=True)
    df.loc[df.metric == "Number", "value"] = df.loc[df.metric == "Number", "value"].round(0).astype(int)
    df.loc[df.metric.isin(["Rate", "Percent"]), "value"] = (
        df.loc[df.metric.isin(["Rate", "Percent"]), "value"].round(2).astype(str)
    )
    return df


def prepare_garden(df: pd.DataFrame) -> Table:

    tb_garden = underscore_table(Table(df))
    tb_garden = clean_values(tb_garden)
    tb_garden = create_units(tb_garden)
    return tb_garden


def run_wrapper(
    dataset: str, country_mapping_path: Path, excluded_countries_path: Path, dest_dir: str, metadata_path: Path
) -> None:
    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / f"meadow/ihme_gbd/2019/{dataset}")

    tb_meadow = ds_meadow[f"{dataset}"]

    df = pd.DataFrame(tb_meadow)
    df = tidy_countries(country_mapping_path, excluded_countries_path, df)
    df_garden = prepare_garden(df)

    all_tables = create_tables(df_garden)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    for table in all_tables:
        if "rei" in table.columns:
            short_name = (
                dataset + "_" + table["measure"].iloc[0] + "_" + table["cause"].iloc[0] + "_" + table["rei"].iloc[0]
            )
            log.info(
                f"{dataset}.create_garden_table",
                measure=table["measure"].iloc[0],
                cause=table["cause"].iloc[0],
                risk_factor=table["rei"].iloc[0],
            )
        else:
            short_name = dataset + "_" + table["measure"].iloc[0] + "_" + table["cause"].iloc[0]
            log.info(
                f"{dataset}.create_garden_table",
                measure=table["measure"].iloc[0],
                cause=table["cause"].iloc[0],
            )

        tb_garden = Table(table)
        tb_garden.metadata = tb_meadow.metadata
        ds_garden.metadata.update_from_yaml(metadata_path)
        tb_garden.update_metadata_from_yaml(metadata_path, f"{dataset}")
        tb_garden.metadata.short_name = underscore(short_name)
        tb_garden = tb_garden.reset_index(drop=True)
        ds_garden.add(tb_garden)
    ds_garden.save()


def create_tables(df: pd.DataFrame) -> List[pd.DataFrame]:
    if "rei" in df.columns:
        df_group = df.groupby(["measure", "cause", "rei"])
    else:
        df_group = df.groupby(["measure", "cause"])

    output_tables = []
    for group_name, df_g in df_group:
        output_tables.append(df_g)
    return output_tables
