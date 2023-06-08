"""Generate GHE garden dataset"""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.datautils import dataframes
from structlog import get_logger

from etl.data_helpers import geo
from etl.data_helpers.population import add_population
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
N = PathFinder(__file__)
AGE_GROUPS_RANGES = {
    "ALLAges": [0, None],
    "YEARS0-1": [0, 1],
    "YEARS1-4": [1, 4],
    "YEARS5-9": [5, 9],
    "YEARS10-14": [10, 14],
    "YEARS15-19": [15, 19],
    "YEARS20-24": [20, 24],
    "YEARS25-29": [25, 29],
    "YEARS30-34": [30, 34],
    "YEARS35-39": [35, 39],
    "YEARS40-44": [40, 44],
    "YEARS45-49": [45, 49],
    "YEARS50-54": [50, 54],
    "YEARS55-59": [55, 59],
    "YEARS60-64": [60, 64],
    "YEARS65-69": [65, 69],
    "YEARS70-74": [70, 74],
    "YEARS75-79": [75, 79],
    "YEARS80-84": [80, 84],
    "YEARS85PLUS": [85, None],
}


def run(dest_dir: str) -> None:
    log.info("ghe: start")

    # read dataset from meadow
    ds_meadow = N.meadow_dataset
    df = pd.DataFrame(ds_meadow["ghe"])

    # Load countries regions
    regions_dataset: Dataset = N.load_dependency("regions")
    regions = regions_dataset["regions"]

    # convert codes to country names
    code_to_country = cast(Dataset, N.load_dependency("regions"))["regions"]["name"].to_dict()
    df["country"] = dataframes.map_series(df["country"], code_to_country, warn_on_missing_mappings=True)

    df = clean_data(df)

    ds_garden = create_dataset(dest_dir, tables=[Table(df, short_name="ghe")], default_metadata=ds_meadow.metadata)
    ds_garden.save()

    log.info("ghe.end")


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    log.info("ghe: basic cleaning")
    df["sex"] = df["sex"].map({"BTSX": "Both sexes", "MLE": "Male", "FMLE": "Female"})
    # Combine substance and alcohol abuse
    df = combine_drug_and_alcohol(df)
    # Add broader age groups
    df = add_age_groups(df)
    # Add global and regional values
    df = add_global_total(df)
    df = add_regions(df)
    # Set indices
    df = df.astype(
        {
            "country": "category",
            "year": "uint16",
            "cause": "category",
            "age_group": "category",
            "sex": "category",
            "daly_count": "float32",
            "daly_rate100k": "float32",
            "death_count": "int32",
            "death_rate100k": "float32",
            "flag_level": "uint8",
        }
    )
    return df.set_index(["country", "year", "age_group", "sex", "cause"])


def combine_drug_and_alcohol(df: pd.DataFrame) -> pd.DataFrame:
    substance_use_disorders = ["Drug use disorders", "Alcohol use disorders"]
    substance_df = df[df["cause"].isin(substance_use_disorders)]
    substance_agg = (
        substance_df.groupby(["country", "year", "age_group", "sex"], as_index=False, observed=True)[
            ["daly_count", "death_count"]
        ]
        .sum()
        .reset_index()
    )
    substance_agg = calculate_rates(substance_agg)
    return df


def calculate_rates(df: pd.DataFrame) -> pd.DataFrame:
    if all(df.columns != "population"):
        df = add_population(
            df=df,
            country_col="country",
            year_col="year",
            sex_col="sex",
            sex_group_all="Both sexes",
            sex_group_female="Female",
            sex_group_male="Male",
            age_col="age_group",
            age_group_mapping=AGE_GROUPS_RANGES,
        )
    df["daly_rate100k"] = 100000 * df["daly_count"] / df["population"]
    df["death_rate100k"] = 100000 * df["death_count"] / df["population"]
    df = df.drop(columns=["population"])

    return df


def build_custom_age_groups(df: pd.DataFrame, age_groups: dict, select_causes: list[str] = None) -> pd.DataFrame:
    """
    Estimate metrics for broader age groups. In line with the age-groups we define in the age_groups dict:
    """
    df_age = df.copy()
    # Add population values for each dimension
    log.info("ghe.add_population_values")

    df_age = df_age[df_age["age_group"].isin(age_groups.keys())]
    if select_causes is not None:
        df_age = df_age[df_age["cause"].isin(select_causes)]

    total_deaths = df_age["death_count"].sum()

    df_age = add_population(
        df=df_age,
        country_col="country",
        year_col="year",
        sex_col="sex",
        sex_group_all="Both sexes",
        sex_group_female="Female",
        sex_group_male="Male",
        age_col="age_group",
        age_group_mapping=AGE_GROUPS_RANGES,
    )
    # Map age groups to broader age groups. Missing age groups in the list are passed as they are (no need to assign to broad group)
    log.info("ghe.create_broader_age_groups")

    df_age["age_group"] = df_age["age_group"].map(age_groups)

    df_age = calculate_rates(df_age)

    assert total_deaths == df_age["death_count"].sum()

    return df_age


def remove_granular_age_groups(df: pd.DataFrame, age_groups_to_keep: list[str]) -> pd.DataFrame:
    """
    Remove the small five-year age-groups that are in the original dataset
    """
    df = df[df["age_group"].isin(age_groups_to_keep)]
    assert len(df["age_group"].drop_duplicates()) == len(age_groups_to_keep)
    return df


def add_age_groups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create custom age-group aggregations, there are a range of different ones that might be useful for this dataset.
    For example, we may want to compare to other causes of death imported via the IHME dataset, so we should include age-groups that match our chosen IHME groups.

    """
    age_groups_ihme = {
        "YEARS0-1": "YEARS0-4",
        "YEARS1-4": "YEARS0-4",
        "YEARS5-9": "YEARS5-14",
        "YEARS10-14": "YEARS5-14",
        "YEARS15-19": "YEARS15-49",
        "YEARS20-24": "YEARS15-49",
        "YEARS25_29": "YEARS15-49",
        "YEARS30_34": "YEARS15-49",
        "YEARS35_39": "YEARS15-49",
        "YEARS40_44": "YEARS15-49",
        "YEARS45_49": "YEARS15-49",
        "YEARS50_54": "YEARS50-69",
        "YEARS55_59": "YEARS50-69",
        "YEARS60_64": "YEARS50-69",
        "YEARS65_69": "YEARS50-69",
        "YEARS70_74": "YEARS70+",
        "YEARS75_79": "YEARS70+",
        "YEARS80_84": "YEARS70+",
        "YEARS85PLUS": "YEARS70+",
    }

    age_groups_self_harm = {
        "YEARS0-1": "YEARS0-14",
        "YEARS1-4": "YEARS0-14",
        "YEARS5-9": "YEARS0-14",
        "YEARS10-14": "YEARS0-14",
        "YEARS25-29": "YEARS25-34",
        "YEARS30-34": "YEARS25-34",
        "YEARS35-39": "YEARS35-44",
        "YEARS40-44": "YEARS35-44",
        "YEARS45-49": "YEARS45-54",
        "YEARS50-54": "YEARS45-54",
        "YEARS55-59": "YEARS55-64",
        "YEARS60-64": "YEARS55-64",
        "YEARS65-69": "YEARS65-74",
        "YEARS70-74": "YEARS65-74",
        "YEARS75-79": "YEARS75-84",
        "YEARS80-84": "YEARS75-84",
    }

    df_age_group_ihme = build_custom_age_groups(df, age_groups=age_groups_ihme)
    df_age_group_self_harm = build_custom_age_groups(df, age_groups=age_groups_self_harm, select_causes=["Self-harm"])
    df = remove_granular_age_groups(df, age_groups_to_keep=["ALLAges"])
    df_combined = pd.concat([df, df_age_group_ihme, df_age_group_self_harm], axis=0)
    df_combined = df_combined.loc[:, ~df_combined.columns.duplicated()]

    return df_combined


def add_global_total(df: pd.DataFrame, regions: Table) -> pd.DataFrame:
    """
    Calculate global total of cholera cases and add it to the existing dataset
    """

    countries = regions[regions["region_type"] == "country"]["name"].to_list()
    assert all(
        df["country"].isin(countries)
    ), f"{df['country'][~df['country'].isin(countries)].drop_duplicates()}, is not a country"
    df_glob = (
        df.groupby(["year", "age_group", "sex", "cause", "flag_level"])
        .agg({"daly_count": "sum", "death_count": "sum"})
        .reset_index()
    )
    df_glob["country"] = "World"
    df_glob = calculate_rates(df_glob)
    df = pd.concat([df, df_glob])

    return df


def add_regions(df: pd.DataFrame, regions: Table) -> pd.DataFrame:
    continents = regions[regions["region_type"] == "continent"]["name"].to_list()

    countries_in_regions = {
        region: sorted(set(geo.list_countries_in_region(region)) & set(df["country"])) for region in continents
    }
    df_cont = df
    df_out = pd.DataFrame()
    for continent in continents:
        df_cont = geo.add_region_aggregates(
            df=df[["year", "age_group", "sex", "cause", "flag_level"]],
            region=continent,
            countries_in_region=countries_in_regions[continent],
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            country_col="country",
            year_col="year",
            frac_allowed_nans_per_year=1,
        )
        df_cont = df_cont[df_cont["country"].isin(continents)]
        df_out = pd.concat([df_out, df_cont])
    df_out = calculate_rates(df_out)

    df = pd.concat([df, df_out])

    return df
