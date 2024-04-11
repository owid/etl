import json
from typing import List, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table, VariableMeta
from owid.catalog.utils import underscore
from structlog import get_logger

from etl.data_helpers import geo
from etl.data_helpers.population import add_population
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = PathFinder(__file__)

AGE_GROUPS_RANGES = {
    "Age00": [0, 0],
    "Age01_04": [1, 4],
    "Age05_09": [5, 9],
    "Age10_14": [10, 14],
    "Age15_19": [15, 19],
    "Age20_24": [20, 24],
    "Age25_29": [25, 29],
    "Age30_34": [30, 34],
    "Age35_39": [35, 39],
    "Age40_44": [40, 44],
    "Age45_49": [45, 49],
    "Age50_54": [50, 54],
    "Age55_59": [55, 59],
    "Age60_64": [60, 64],
    "Age65_69": [65, 69],
    "Age70_74": [70, 74],
    "Age75_79": [75, 79],
    "Age80_84": [80, 84],
    "Age85_over": [85, None],
    "All ages": [0, None],
}


def run(dest_dir: str) -> None:
    log.info("who_mort_db.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/homicide/2023-01-03/who_mort_db")
    tb_meadow = ds_meadow["who_mort_db"]

    df = pd.DataFrame(tb_meadow).astype({"number_of_deaths": float})

    log.info("who_mort_db.exclude_countries")
    df = exclude_countries(df)

    log.info("who_mort_db.harmonize_countries")
    df = harmonize_countries(df)
    df = clean_up_dimensions(df)
    df = add_age_groups(df)
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
    # create new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # create new table with the same metadata as meadow and add it to dataset
    tb_garden = Table(df_piv, short_name="who_mort_db")
    new_tb_garden = Table(short_name="who_mort_db")
    for col in tb_garden.columns:
        col_metadata = build_metadata(col)
        new_col = underscore(" ".join(col).strip())
        new_tb_garden[new_col] = tb_garden[col]
        new_tb_garden[new_col].metadata = col_metadata

    ds_garden.add(new_tb_garden)

    # update metadata from yaml file
    ds_garden.update_metadata(N.metadata_path)

    ds_garden.save()

    log.info("who_mort_db.end")


def clean_up_dimensions(df: pd.DataFrame) -> pd.DataFrame:
    sex_dict = {"All": "Both Sexes", "Male": "Males", "Female": "Females", "Unknown": "Unknown sex"}
    age_dict = {"Age_all": "All ages", "Age_unknown": "Unknown age"}
    df = df.astype({"sex": str, "age_group_code": str}).replace({"sex": sex_dict, "age_group_code": age_dict})

    return df


def add_age_groups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create custom age-group aggregations, there are a range of different ones that might be useful for this dataset.
    For example, we may want to compare to other causes of death imported via the IHME dataset, so we should include age-groups that match our chosen IHME groups.

    """
    age_groups_ihme = {
        "Age00": "Age 0-4",
        "Age01_04": "Age 0-4",
        "Age05_09": "Age 5-14",
        "Age10_14": "Age 5-14",
        "Age15_19": "Age 15-49",
        "Age20_24": "Age 15-49",
        "Age25_29": "Age 15-49",
        "Age30_34": "Age 15-49",
        "Age35_39": "Age 15-49",
        "Age40_44": "Age 15-49",
        "Age45_49": "Age 15-49",
        "Age50_54": "Age 50-69",
        "Age55_59": "Age 50-69",
        "Age60_64": "Age 50-69",
        "Age65_69": "Age 50-69",
        "Age70_74": "Age 70+",
        "Age75_79": "Age 70+",
        "Age80_84": "Age 70+",
        "Age85_over": "Age 70+",
        "All ages": "All ages",
        "Unknown age": "Unknown age",
    }

    age_groups_decadal = {
        "Age00": "Age 0-4",
        "Age01_04": "Age 0-4",
        "Age05_09": "Age 5-14",
        "Age10_14": "Age 5-14",
        "Age15_19": "Age 15-24",
        "Age20_24": "Age 15-24",
        "Age25_29": "Age 25-34",
        "Age30_34": "Age 25-34",
        "Age35_39": "Age 35-44",
        "Age40_44": "Age 35-44",
        "Age45_49": "Age 45-54",
        "Age50_54": "Age 45-54",
        "Age55_59": "Age 55-64",
        "Age60_64": "Age 55-64",
        "Age65_69": "Age 65-74",
        "Age70_74": "Age 65-74",
        "Age75_79": "Age 75-84",
        "Age80_84": "Age 75-84",
        "Age85_over": "Age 85+",
        "All ages": "All ages",
        "Unknown age": "Unknown age",
    }

    age_groups_child = {
        "Age00": "Age 0-19",
        "Age01_04": "Age 0-19",
        "Age05_09": "Age 0-19",
        "Age10_14": "Age 0-19",
        "Age15_19": "Age 0-19",
    }

    df_age_group_ihme = build_custom_age_groups(df, age_groups=age_groups_ihme)
    df_age_group_decadal = build_custom_age_groups(df, age_groups=age_groups_decadal)
    df_age_group_child = build_custom_age_groups(df, age_groups=age_groups_child)
    df_orig = remove_granular_age_groups(df)
    df_combined = pd.concat([df_orig, df_age_group_ihme, df_age_group_decadal, df_age_group_child], axis=0)
    df_combined = df_combined.loc[:, ~df_combined.columns.duplicated()]
    return df_combined


def build_custom_age_groups(df: pd.DataFrame, age_groups: dict) -> pd.DataFrame:
    """
    Estimate metrics for broader age groups. In line with the age-groups we define in the age_groups dict:
    """
    df_age = df.copy()
    # Add population values for each dimension
    log.info("who_mort_db.add_population_values")

    df_age = df_age[df_age["age_group_code"].isin(age_groups.keys())]

    total_deaths = df_age["number_of_deaths"].sum()

    df_age = add_population(
        df=df_age,
        country_col="country",
        year_col="year",
        sex_col="sex",
        sex_group_all="Both Sexes",
        sex_group_female="Females",
        sex_group_male="Males",
        age_col="age_group_code",
        age_group_mapping=AGE_GROUPS_RANGES,
    )
    # Map age groups to broader age groups. Missing age groups in the list are passed as they are (no need to assign to broad group)
    log.info("who_mort_db.create_broader_age_groups")

    df_age["age_group_code"] = df_age["age_group_code"].map(age_groups)

    # Sum
    df_age = df_age.drop(
        [
            "percentage_of_cause_specific_deaths_out_of_total_deaths",
            "age_standardized_death_rate_per_100_000_standard_population",
            "death_rate_per_100_000_population",
        ],
        axis=1,
    )
    df_age = df_age.groupby(["country", "year", "sex", "age_group_code"]).sum()
    df_age["death_rate_per_100_000_population"] = (
        df_age["number_of_deaths"].div(df_age["population"]).replace(np.inf, np.nan)
    ) * 100000
    df_age = df_age.drop(columns=["population"]).reset_index()

    assert total_deaths == df_age["number_of_deaths"].sum()

    return df_age


def remove_granular_age_groups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove the small five-year age-groups that are in the original dataset
    """
    age_groups_to_keep = ["All ages", "Unknown age"]
    df = df[df["age_group_code"].isin(age_groups_to_keep)]
    assert len(df["age_group_code"].drop_duplicates()) == len(age_groups_to_keep)
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
        "number_of_deaths": {
            "title": "Number of homicides",
            "unit": "homicides",
            "short_unit": "",
            "numDecimalPlaces": 0,
        },
    }
    meta = VariableMeta(
        title=f"{metric_dict[col[0]]['title']} - {col[1]} - {col[2]}",
        description=f"The {metric_dict[col[0]]['title'].lower()} for {col[1].lower()}, {col[2].lower()}.",
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
