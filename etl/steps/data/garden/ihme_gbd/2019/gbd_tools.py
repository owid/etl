import json
from pathlib import Path
from typing import Any, List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.data_helpers import geo
from etl.paths import DATA_DIR

log = get_logger()


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
    # TODO: this should happen in harmonize_countries
    df["country"] = df["country"].astype("category")

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


def prepare_garden(df: pd.DataFrame) -> Table:
    log.info("prepare_garden")
    tb_garden = underscore_table(Table(df))
    return tb_garden


def _pivot_number(df: pd.DataFrame, dims: List[str]) -> Any:
    df_number = df[df.metric == "Number"].pivot(index=["country", "year"] + dims, columns="measure", values="value")
    df_number = df_number.rename(columns=lambda c: c + " - Number")
    return df_number


def _pivot_percent(df: pd.DataFrame, dims: List[str]) -> Any:
    df_percent = df[df.metric == "Percent"].pivot(index=["country", "year"] + dims, columns="measure", values="value")
    df_percent = df_percent.rename(columns=lambda c: c + " - Percent")
    return df_percent


def _pivot_rate(df: pd.DataFrame, dims: List[str]) -> Any:
    df_rate = df[df.metric == "Rate"].pivot(index=["country", "year"] + dims, columns="measure", values="value")
    df_rate = df_rate.rename(columns=lambda c: c + " - Rate")
    return df_rate


def _pivot_share(df: pd.DataFrame, dims: List[str]) -> Any:
    df_share = df[df.metric == "Share of the population"].pivot(
        index=["country", "year"] + dims, columns="measure", values="value"
    )
    df_share = df_share.rename(columns=lambda c: c + " - Share of the population")
    return df_share


def pivot(df: pd.DataFrame, dims: List[str]) -> Table:
    # NOTE: processing them separately simplifies the code and is faster (and less memory heavy) than
    # doing it all in one pivot operation
    df_number = _pivot_number(df, dims)
    df_percent = _pivot_percent(df, dims)
    df_rate = _pivot_rate(df, dims)
    df_share = _pivot_share(df, dims)

    tb_garden = Table(pd.concat([df_number, df_percent, df_rate, df_share], axis=1))
    tb_garden = underscore_table(tb_garden)

    return tb_garden


def omm_metrics(df: pd.DataFrame) -> Any:
    """Generate dataframe with OMM metrics with the same columns as input."""
    # {
    #     "All forms of violence": [
    #         "Deaths - Interpersonal violence - Sex: Both - Age: Age-standardized (Rate)",
    #         "Deaths - Conflict and terrorism - Sex: Both - Age: Age-standardized (Rate)",
    #         "Deaths - Executions and police conflict - Sex: Both - Age: Age-standardized (Rate)",
    #     ]
    # }

    # list of all OMMs
    omms = []

    # All forms of violence
    om = df[
        df.cause.isin({"Interpersonal violence", "Conflict and terrorism", "Executions and police conflict"})
        & (df.measure == "Deaths")
        & (df.sex == "Both")
        & (df.age == "Age-standardized")
        & (df.metric == "Rate")
    ]

    cols = [c for c in om.columns if c not in ("value", "cause")]
    gr = om.groupby(cols, observed=True, as_index=False).sum(numeric_only=True)
    gr["cause"] = "All forms of violence"
    omms.append(gr)

    # 0-27 days - child mortality age group

    om = df[
        (df.measure == "Deaths")
        & (df.sex == "Both")
        & (df.age.isin({"0-6 days", "7-27 days"}))
        & (df.metric == "Number")
    ]
    cols = [c for c in om.columns if c not in ("value", "cause")]
    gr = om.groupby(cols, observed=True, as_index=False).sum(numeric_only=True)
    gr["cause"] = "All forms of violence"
    omms.append(gr)

    # Smoking and air pollution still to figure out. See https://github.com/owid/importers/blob/master/ihme_gbd/ihme_gbd_risk/config/variables_to_sum.json

    return pd.concat(omms, axis=0)


def add_share_of_population(df: pd.DataFrame) -> pd.DataFrame:
    df_rate = df.loc[df["metric"] == "Rate"]
    df_percent = df_rate.copy()
    df_percent["metric"] = "Share of the population"
    df_percent.loc[:, "value"] = df_percent.loc[:, "value"] / 1000

    df = cast(pd.DataFrame, pd.concat([df, df_percent], axis=0))
    return df


def run_wrapper(
    dataset: str,
    country_mapping_path: Path,
    excluded_countries_path: Path,
    dest_dir: str,
    metadata_path: Path,
    dims: List[str],
) -> None:
    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / f"meadow/ihme_gbd/2019/{dataset}")

    tb_meadow = ds_meadow[dataset]
    tb_meadow = tb_meadow.drop(["index"], axis=1, errors="ignore")
    df_garden = pd.DataFrame(tb_meadow)
    df_garden = tidy_countries(country_mapping_path, excluded_countries_path, df_garden)
    df_garden = prepare_garden(df_garden)

    omm = omm_metrics(df_garden)
    df_garden = cast(pd.DataFrame, pd.concat([df_garden, omm], axis=0))
    df_garden = add_share_of_population(df_garden)
    tb_garden = pivot(df_garden, dims)

    # free up memory
    del df_garden

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden.metadata = tb_meadow.metadata
    ds_garden.metadata.update_from_yaml(metadata_path)
    tb_garden.update_metadata_from_yaml(metadata_path, f"{dataset}")
    ds_garden.add(tb_garden)
    ds_garden.save()
