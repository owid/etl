import json
from functools import reduce
from typing import Any, List, cast

import pandas as pd
from owid import catalog
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

UNWPP = DATA_DIR / "garden/un/2022-07-11/un_wpp"

log = get_logger()

N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("who_vaccination.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/who/2022-07-17/who_vaccination")
    tb_meadow = ds_meadow["who_vaccination"]

    df = pd.DataFrame(tb_meadow)

    log.info("who_vaccination.exclude_countries")
    df = exclude_countries(df)

    log.info("who_vaccination.harmonize_countries")
    df = harmonize_countries(df)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = clean_and_format_data(df)

    pop_one_yr = get_population_one_year_olds()
    pop_one_yr = pop_one_yr.rename(columns={"location": "country", "value": "population"}).drop(
        columns=["metric", "variant", "sex", "age"]
    )
    # table = N.garden_dataset["who_vaccination"]
    tb_garden = calculate_vaccinated_unvaccinated_population(tb_garden, pop_one_yr)

    tb_garden.metadata = tb_meadow.metadata
    ds_garden.metadata.update_from_yaml(N.metadata_path)
    tb_garden.update_metadata_from_yaml(N.metadata_path, "who_vaccination")

    ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("who_vaccination.end")


def load_excluded_countries() -> List[str]:
    with open(N.excluded_countries_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(df: pd.DataFrame) -> pd.DataFrame:
    excluded_countries = load_excluded_countries()
    return cast(pd.DataFrame, df.loc[~df.country.isin(excluded_countries)])


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(N.country_mapping_path))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {N.country_mapping_path} to include these country "
            f"names; or (b) add them to {N.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df


def get_population_one_year_olds() -> pd.DataFrame:
    un_wpp_data = catalog.Dataset(UNWPP)
    pop = un_wpp_data["population"].reset_index()
    pop_one_yr = pop[
        (pop["age"] == "1") & (pop["variant"] == "estimates") & (pop["metric"] == "population") & (pop["sex"] == "all")
    ]
    return cast(pd.DataFrame, pop_one_yr)


def calculate_vaccinated_unvaccinated_population(table: Table, pop_one_yr: pd.DataFrame) -> Any:
    # vaccines where the coverage is measured as % of one-year olds
    vax_one_year_olds = [
        "bcg",
        "dtp_containing_vaccine__1st_dose",
        "dtp_containing_vaccine__3rd_dose",
        "hepb3",
        "hepb__birth_dose__given_within_24_hours_of_birth",
        "hib3",
        "inactivated_polio_containing_vaccine__1st_dose",
        "measles_containing_vaccine__1st_dose",
        "measles_containing_vaccine__2nd_dose",
        "pneumococcal_conjugate_vaccine__final_dose",
        "polio__3rd_dose",
        "rubella_containing_vaccine__1st_dose",
        "rotavirus__last_dose",
        "yellow_fever_vaccine",
    ]
    vax_pop = table[["country", "year"] + vax_one_year_olds].merge(pop_one_yr, on=["country", "year"])
    vax_pop[vax_one_year_olds] = (
        vax_pop[vax_one_year_olds].multiply(0.01).multiply(vax_pop["population"], axis="index").round(0).astype("Int64")
    )

    vax_pop = vax_pop.rename(columns={c: c + "_vaccinated" for c in vax_pop.columns if c in vax_one_year_olds})
    vax_pop = vax_pop.drop(columns=["population"])

    unvax_pop = table[["country", "year"] + vax_one_year_olds].merge(pop_one_yr, on=["country", "year"])
    unvax_pop[vax_one_year_olds] = 100 - unvax_pop[vax_one_year_olds]
    unvax_pop[vax_one_year_olds] = (
        unvax_pop[vax_one_year_olds]
        .multiply(0.01)
        .multiply(unvax_pop["population"], axis="index")
        .round(0)
        .astype("Int64")
    )
    unvax_pop = unvax_pop.rename(columns={c: c + "_unvaccinated" for c in unvax_pop.columns if c in vax_one_year_olds})
    unvax_pop = unvax_pop.drop(columns=["population"])
    data_frames = [table, vax_pop, unvax_pop]

    df_merged = reduce(lambda left, right: pd.merge(left, right, on=["country", "year"], how="outer"), data_frames)
    return cast(pd.DataFrame, df_merged)


def clean_and_format_data(df: pd.DataFrame) -> pd.DataFrame:
    # may need to combine japanese encephalitis and japanese encephalitis first dose
    # We use only the WUENIC figures - those estimated by WHO and UNICEF - other estimates available are OFFICIAL and ADMIN
    df = df[df["coverage_category"] == "WUENIC"]
    df = df[df["antigen"] != "MCV2X2"]
    df = df.dropna(subset="coverage")
    df = df.drop(columns=["index", "group", "antigen", "coverage_category", "coverage_category_description"])
    df = df.pivot_table(
        values=["coverage"],
        columns=["antigen_description"],
        index=[
            "country",
            "year",
        ],
    )
    df.columns = df.columns.to_series().apply("_".join).str.replace("coverage_", "")
    df = df.reset_index()

    tb_garden = underscore_table(Table(df))
    cols = tb_garden.drop(["country", "year"], axis=1).columns

    tb_garden[cols] = tb_garden[cols].astype(float).round(2)
    # replacing values where x <= 100 is False with None
    tb_garden[cols] = tb_garden[cols].where(lambda x: x.le(100), None)
    # dropping all columns that are only NA
    tb_garden = tb_garden.dropna(axis=1, how="all")
    return cast(pd.DataFrame, tb_garden)
