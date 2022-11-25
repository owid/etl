import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from owid.datautils import geo
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = Names(__file__)
N = Names("etl/steps/data/garden/un/2021-12-20/un_igme.py")


def run(dest_dir: str) -> None:
    log.info("un_igme.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/un/2021-12-20/un_igme")
    tb_meadow = ds_meadow["un_igme"]

    df = pd.DataFrame(tb_meadow)

    log.info("un_igme.exclude_countries")
    df = exclude_countries(df)

    log.info("un_igme.harmonize_countries")
    df = harmonize_countries(df)

    # Calculate missing age-group mortality rates
    df = calculate_mortality_rate(df)
    # Making the values in the table a bit more appropriate for our use and pivoting to a wide table.
    log.info("un_igme.clean_data")
    df = clean_and_format_data(df)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    # I want to create one table per column and auto-populate the metadata
    for df_select in df:
        print(df_select)
        # Pulling out the relevant information from the column names for the metadata
        # Metric is the central value or upper/lower bound
        metric = df_select[0]
        sex = df_select[1]
        age_group = df_select[2]
        unit = df_select[3]

        df_t = Table(pd.DataFrame(df[df_select]).reset_index())
        df_t.columns = ["_".join(col).strip() for col in df_t.columns.values]
        tb_garden = underscore_table(df_t)
        # Creating table and variable level metadata
        tb_garden.metadata = tb_meadow.metadata
        tab_name = tb_garden.columns[2]
        tb_garden[tab_name].metadata.title = f"{age_group} - {sex} - {metric}"
        tb_garden[tab_name].metadata.short_name = tab_name
        tb_garden[tab_name].metadata.unit = unit
        if tb_garden[tab_name].metadata.unit in ["deaths", "Number of stillbirths"]:
            tb_garden[tab_name] = tb_garden[tab_name].astype("Int64").round(0)
        else:
            tb_garden[tab_name] = tb_garden[tab_name].astype("float").round(2)
        ds_garden.add(tb_garden)
    ds_garden.save()

    log.info("un_igme.end")


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


def clean_and_format_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleaning up the values and dropping unused columns"""
    df = df.drop(columns=["index", "regional_group"])
    df["unit"] = df["unit"].replace(
        {"Number of deaths": "deaths", "Deaths per 1000 live births": "deaths per 1,000 live births"}
    )
    df["sex"] = df["sex"].replace({"Total": "Both"})

    df = df.pivot(
        index=["country", "year"], columns=["sex", "indicator", "unit"], values=["value", "lower_bound", "upper_bound"]
    )
    df = df.dropna(how="all", axis=1)
    return df


def calculate_mortality_rate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculating  mortality rates for missing age-groups. For each key in the dictionary below we calculate the mortality rate using the items for each key
    """
    output_df = []
    rate_calculations = {
        "Under-ten mortality rate": ["Under-five mortality rate", "Mortality rate age 5-9"],
        "Under-fifteen mortality rate": ["Under-five mortality rate", "Mortality rate age 5-14"],
        "Under-twenty-five mortality rate": ["Under-five mortality rate", "Mortality rate age 5-24"],
    }

    for new_rate, components in rate_calculations.items():
        log.info(f"Calculating.{new_rate}")

        df_a = df[["country", "year", "value"]][(df["indicator"] == components[0]) & (df["sex"] == "Total")]
        df_a = df_a.rename(columns={"value": components[0]})
        df_b = df[["country", "year", "value"]][(df["indicator"] == components[1]) & (df["sex"] == "Total")]
        df_b = df_b.rename(columns={"value": components[1]})

        df_m = df_a.merge(df_b, on=["country", "year"], how="inner")
        df_m["int_value"] = ((1000 - df_m[components[0]])) / 1000 * df_m[components[1]]
        df_m["indicator"] = new_rate
        df_m["value"] = df_m["int_value"] + df_m[components[1]]
        df_m["unit"] = "Deaths per 1000 live births"
        df_m["sex"] = "Total"
        df_m = df_m[["country", "year", "sex", "indicator", "value", "unit"]]

        output_df.append(df_m)

    out_df = pd.DataFrame(pd.concat(output_df))

    df = pd.concat([df, out_df])
    return df
