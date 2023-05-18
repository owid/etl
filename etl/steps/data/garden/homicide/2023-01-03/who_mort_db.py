import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table, VariableMeta
from owid.catalog.utils import underscore
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("who_mort_db.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/homicide/2023-01-03/who_mort_db")
    tb_meadow = ds_meadow["who_mort_db"]

    df = pd.DataFrame(tb_meadow)

    log.info("who_mort_db.exclude_countries")
    df = exclude_countries(df)

    log.info("who_mort_db.harmonize_countries")
    df = harmonize_countries(df)
    df["sex"] = df["sex"].str.lower()
    log.info("who_mort_df.calculate_youth_homicides")
    df = calculate_youth_homicide_rate(df)
    df = clean_up_dimensions(df)

    df_piv = df.pivot_table(
        index=["country", "year"],
        columns=["sex", "age_group_code"],
        values=[
            "number_of_deaths",
            "percentage_of_cause_specific_deaths_out_of_total_deaths",
            "age_standardized_death_rate_per_100_000_standard_population",
            "death_rate_per_100_000_population",
        ],
    )

    # df = df.set_index(["country", "year", "age_group_code", "sex"], verify_integrity=True)
    # create new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # create new table with the same metadata as meadow and add it to dataset
    tb_garden = Table(df_piv)

    for col in tb_garden.columns:
        tb_garden[col].metadata = build_metadata(col)

    ds_garden.add(tb_garden)

    # update metadata from yaml file
    ds_garden.update_metadata(N.metadata_path)

    ds_garden.save()

    log.info("who_mort_db.end")


def clean_up_dimensions(df: pd.DataFrame) -> pd.DataFrame:
    sex_dict = {"all": "Both Sexes", "male": "Males", "female": "Females", "unknown": "Unknown sex"}
    age_dict = {
        "Age_all": "All ages",
        "Age00": "Under 1",
        "Age01_04": "Age 1-4",
        "Age05_09": "Age 5-9",
        "Age10_14": "Age 10-14",
        "Age15_19": "Age 15-19",
        "Age20_24": "Age 20-24",
        "Age25_29": "Age 25-29",
        "Age30_34": "Age 30-34",
        "Age35_39": "Age 35-39",
        "Age40_44": "Age 40-44",
        "Age45_49": "Age 45-49",
        "Age50_54": "Age 50-54",
        "Age55_59": "Age 55-59",
        "Age60_64": "Age 60-64",
        "Age65_69": "Age 65-69",
        "Age70_74": "Age 70-74",
        "Age75_79": "Age 75-79",
        "Age80_84": "Age 80-84",
        "Age_unknown": "Unknown age",
        "Age00_14": "Under 15",
    }

    df = df.replace({"age_group_code": age_dict, "sex": sex_dict})

    return df


def build_metadata(col: tuple) -> pd.DataFrame:
    """
    Building the variable level metadata for each of the age-sex-metric combinations
    """
    metric_dict = {
        "age_standardized_death_rate_per_100_000_standard_population": {
            "title": "Age standardized homicide rate per 100,000 population",
            "unit": "homicides per 100,000 people",
            "short_unit": "",
            "numDecimalPlaces": 2,
        },
        "death_rate_per_100_000_population": {
            "title": "Homicide rate per 100,000 population",
            "unit": "homicides per 100,000 people",
            "short_unit": "",
            "numDecimalPlaces": 2,
        },
        "percentage_of_cause_specific_deaths_out_of_total_deaths": {
            "title": "Share of total deaths",
            "unit": "%",
            "short_unit": "%",
            "numDecimalPlaces": 2,
        },
        "number_of_deaths": {"title": "Number of deaths", "unit": "homicides", "short_unit": "", "numDecimalPlaces": 2},
    }
    meta = VariableMeta(
        title=f"{metric_dict[col[0]]['title']} - {col[1]} - {col[2]}",
        description=f"The {metric_dict[col[0]]['title'].lower()} for {col[1].lower()}, {col[2].lower()} years",
        unit=f"{metric_dict[col[0]]['unit']}",
        short_unit=f"{metric_dict[col[0]]['short_unit']}",
    )
    meta.display = {
        "numDecimalPlaces": metric_dict[col[0]]["numDecimalPlaces"],
    }
    return meta


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


def load_youth_population(min_year: int, max_year: int) -> Table:
    """Load population table from population UNWPP dataset."""
    ds_indicators: Dataset = N.load_dependency(channel="garden", namespace="un", short_name="un_wpp")
    tb_population = ds_indicators["population"].reset_index(drop=False)
    tb_population = tb_population[
        (tb_population["age"] == "0-14")
        & (tb_population["year"] >= min_year)
        & (tb_population["year"] <= max_year)
        & (tb_population["metric"] == "population")
    ]

    return tb_population


def calculate_youth_homicides(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sum the number of homicides for the following age-groups:
    * 0 years
    * 1-4 years
    * 5-9 years
    * 10-14 years

    To get the total homicides for those under age 15.
    """

    age_group_codes = ["Age00", "Age01_04", "Age05_09", "Age10_14"]

    df_youth = df[df["age_group_code"].isin(age_group_codes)]

    cols = ["country", "year", "sex", "age_group_code", "number_of_deaths"]
    df_youth = df_youth[cols].groupby(by=["country", "year", "sex"])["number_of_deaths"].sum().reset_index()
    df_youth.loc[:, "age_group_code"] = "Age00_14"

    # df_merge = pd.merge(df, df_youth, how="outer", on=["country", "year", "sex", "number_of_deaths", "age_group_code"])

    return df_youth


def calculate_youth_homicide_rate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the youth homicide rate:

    Total homicides where the victim was under 15/ total under 15 population
    """
    youth_homicides = calculate_youth_homicides(df)

    min_year = df["year"].min()
    max_year = df["year"].max()

    youth_pop = load_youth_population(min_year=min_year, max_year=max_year)

    youth_pop = youth_pop.rename(columns={"value": "population", "location": "country"}).drop(
        columns=["metric", "variant", "age"]
    )

    youth_homicides_df = pd.merge(
        youth_homicides,
        youth_pop,
        how="left",
        on=["country", "year", "sex"],
    ).dropna(subset="population")

    youth_homicides_df["death_rate_per_100_000_population"] = (
        youth_homicides_df["number_of_deaths"] / youth_homicides_df["population"]
    ) * 100000

    df_merge = pd.merge(
        df,
        youth_homicides_df,
        how="outer",
        on=["country", "year", "sex", "number_of_deaths", "death_rate_per_100_000_population", "age_group_code"],
    )

    df_merge = df_merge.drop(columns="population")

    return df_merge
