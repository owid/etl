import json
from pathlib import Path
from typing import Any, List, cast

import pandas as pd
from owid.catalog import Dataset, Table, Variable, VariableMeta
from owid.catalog.utils import underscore, underscore_table
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


def pivot(df: pd.DataFrame, dims: List[str]) -> pd.DataFrame:
    # NOTE: processing them separately simplifies the code and is faster (and less memory heavy) than
    # doing it all in one pivot operation
    df_number = _pivot_number(df, dims)
    df_percent = _pivot_percent(df, dims)
    df_rate = _pivot_rate(df, dims)
    df_share = _pivot_share(df, dims)
    df_garden = pd.concat([df_number, df_percent, df_rate, df_share], axis=1)
    # add metadata step
    # tb_garden = Table(df_garden)
    # tb_garden = underscore_table(tb_garden)

    return df_garden


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


def create_variable_metadata(variable: Variable, cause: str, age: str, sex: str, rei: str = "None"):
    var_name_dict = {
        "Deaths - Rate": {
            "title": f"Deaths that are from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f" per 100,000 people, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "deaths per 100,000 people",
            "short_unit": "",
            "num_decimal_places": 1,
        },
        "DALYs (Disability-Adjusted Life Years) - Rate": {
            "title": f"DALYs from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f" per 100,000 people in, {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "DALYs per 100,000 people",
            "short_unit": "",
            "num_decimal_places": 1,
        },
        "Deaths - Percent": {
            "title": f"Share of total deaths that are from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f", in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "%",
            "short_unit": "%",
            "num_decimal_places": 1,
        },
        "DALYs (Disability-Adjusted Life Years) - Percent": {
            "title": f"Share of total DALYs that are from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f", in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "%",
            "short_unit": "%",
            "num_decimal_places": 1,
        },
        "Deaths - Number": {
            "title": f"Deaths that are from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f", in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "deaths",
            "short_unit": "",
            "num_decimal_places": 0,
        },
        "DALYs (Disability-Adjusted Life Years) - Number": {
            "title": f"DALYs that are from {cause.lower()}"
            + (f" attributed to {rei.lower()}" if rei != "None" else "")
            + f", in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "DALYs",
            "short_unit": "",
            "num_decimal_places": 1,
        },
        "Incidence - Number": {
            "title": f"Number of new cases of {cause.lower()}, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "cases",
            "short_unit": "",
            "num_decimal_places": 0,
        },
        "Prevalence - Number": {
            "title": f"Current number of cases of {cause.lower()}, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "cases",
            "short_unit": "",
            "num_decimal_places": 0,
        },
        "Incidence - Rate": {
            "title": f"Number of new cases of {cause.lower()} per 100,000 people, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "cases",
            "short_unit": "",
            "num_decimal_places": 1,
        },
        "Prevalence - Rate": {
            "title": f"Current number of cases of {cause.lower()} per 100,000 people, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "cases",
            "short_unit": "",
            "num_decimal_places": 1,
        },
        "Incidence - Share of the population": {
            "title": f"Number of new cases of {cause.lower()} per 100 people, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "%",
            "short_unit": "%",
            "num_decimal_places": 1,
        },
        "Prevalence - Share of the population": {
            "title": f"Current number of cases of {cause.lower()} per 100 people, in {sex.lower()} aged {age.lower()}",
            "description": "",
            "unit": "%",
            "short_unit": "%",
            "num_decimal_places": 1,
        },
    }
    new_variable = variable.copy()
    new_variable.name = underscore(var_name_dict[variable.name]["title"])
    new_variable.metadata = VariableMeta(
        title=var_name_dict[variable.name]["title"],
        description=var_name_dict[variable.name]["description"],
        unit=var_name_dict[variable.name]["unit"],
        short_unit=var_name_dict[variable.name]["short_unit"],
    )
    new_variable.metadata.display = {
        "name": var_name_dict[variable.name]["title"],
        "numDecimalPlaces": var_name_dict[variable.name]["num_decimal_places"],
    }

    return new_variable


def add_metadata(dest_dir: str, ds_meadow: Dataset, df: pd.DataFrame, dims: List[str]) -> Dataset:
    """
    Adding metadata at the variable level
    First step is to group by the dims, which are normally: age, sex and cause.
    Then for each variable (the different metrics) we add in the metadata.
    """
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    df = df.reset_index()
    df_group = df.groupby(dims)
    for group_id, group in df_group:
        # Grab out the IDs of each of the grouping factors, e.g. the age-group, sex and cause
        dims_id = dict(zip(dims, group_id))
        tb_group = Table(group)
        # Create the unique table short name
        dims_values = list(dims_id.values())
        tb_group.metadata.short_name = underscore(" - ".join(dims_values))[0:240]
        variables = tb_group.columns.drop(dims + ["country", "year"])
        for variable_name in variables:
            tb_group[variable_name] = Variable(tb_group[variable_name])
            # Create all the necessary metadata
            cleaned_variable = create_variable_metadata(variable=tb_group[variable_name], **dims_id)
            tb_group[cleaned_variable.name] = cleaned_variable
            tb_group = tb_group.drop(columns=variable_name)
            # dropping columns that are totally empty - not all combinations of variables exist or have been downloaded
        tb_group = tb_group.dropna(axis=1, how="all")
        # Dropping dims as table name contains them
        tb_group = tb_group.drop(columns=dims)
        tb_group = tb_group.set_index(["country", "year"], verify_integrity=True)
        ds_garden.add(tb_group)
    return ds_garden


def tidy_sex_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Improve the labelling of the sex column
    """
    sex_dict = {"Both": "Both sexes", "Female": "Females", "Male": "Males"}
    df["sex"] = df["sex"].replace(sex_dict, regex=False)
    return df


def tidy_age_dimension(df: pd.DataFrame) -> pd.DataFrame:
    age_dict = {
        "Early Neonatal": "0-6 days",
        "Late Neonatal": "7-27 days",
        "Post Neonatal": "28-364 days",
        "1 to 4": "1-4 years",
    }

    df["age"] = df["age"].replace(age_dict, regex=False)

    return df


def run_wrapper(
    dataset: str,
    country_mapping_path: Path,
    excluded_countries_path: Path,
    dest_dir: str,
    dims: List[str],
) -> None:
    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / f"meadow/ihme_gbd/2019/{dataset}")

    tb_meadow = ds_meadow[dataset]
    tb_meadow = tb_meadow.drop(["index"], axis=1, errors="ignore")
    df_garden = pd.DataFrame(tb_meadow)
    if dataset == "gbd_risk":
        assert max(df_garden["value"][df_garden["metric"] == "Percent"]) > 1
    df_garden = tidy_countries(country_mapping_path, excluded_countries_path, df_garden)
    df_garden = tidy_sex_dimension(df_garden)
    df_garden = tidy_age_dimension(df_garden)
    df_garden = prepare_garden(df_garden)

    omm = omm_metrics(df_garden)
    df_garden = cast(pd.DataFrame, pd.concat([df_garden, omm], axis=0))
    # Adding the share of the population affected, when the denominator _isn't_ the total number of deaths/DALYs.
    if dataset in ["gbd_mental_health", "gbd_prevalence"]:
        df_garden = add_share_of_population(df_garden)
    df_garden = pivot(df_garden, dims)

    ds_garden = add_metadata(dest_dir=dest_dir, ds_meadow=ds_meadow, df=df_garden, dims=dims)

    ds_garden.save()
