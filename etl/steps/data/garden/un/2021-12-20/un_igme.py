import json
from typing import List, cast

import pandas as pd
from owid import catalog
from owid.catalog import Dataset, Table, Variable
from owid.catalog.utils import underscore, underscore_table
from owid.datautils import geo
from structlog import get_logger

from etl.helpers import Names
from etl.paths import DATA_DIR

log = get_logger()

GAPMINDER_CHILD_MORTALITY_DATASET_PATH = DATA_DIR / "open_numbers/open_numbers/latest/gapminder__child_mortality"
GAPMINDER_INFANT_MORTALITY_DATASET_PATH = DATA_DIR / "open_numbers/open_numbers/latest/gapminder__hist_imr"


# naming conventions
N = Names(__file__)
N = Names("etl/steps/data/garden/un/2021-12-20/un_igme.py")


def run(dest_dir: str) -> None:
    log.info("un_igme.start")
    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/un/2021-12-20/un_igme")
    tb_meadow = ds_meadow["un_igme"]
    df = pd.DataFrame(tb_meadow).drop(columns=["index"])
    # adding source tag to all UN IGME rows prior to combination with Gapminder data
    df["source"] = "UN IGME"
    df_gap = get_gapminder_data()
    df_combine = pd.concat([df, df_gap])

    log.info("un_igme.exclude_countries")
    df_combine = exclude_countries(df_combine)

    log.info("un_igme.harmonize_countries")
    dfc = harmonize_countries(df_combine)
    # Preferentially use UN IGME data where there is duplicate values for country-year combinations
    dfc = combine_datasets(dfc)
    # Calculate missing age-group mortality rates
    dfc = calculate_mortality_rate(dfc)
    # Making the values in the table a bit more appropriate for our use and pivoting to a wide table.
    log.info("un_igme.clean_data")
    dfc = clean_and_format_data(dfc)

    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata

    tb_garden = Table(dfc)
    tb_garden.metadata = ds_meadow.metadata
    tb_garden.columns = ["__".join(col).strip() for col in tb_garden.columns.values]
    # Create one table per column and auto-populate the metadata
    for col in tb_garden.columns:
        print(col)
        col_name = col.split(sep="__")
        # Pulling out the relevant information from the column names for the metadata
        # Metric is the central value or upper/lower bound
        metric = col_name[0]
        sex = col_name[1]
        age_group = col_name[2]
        unit = col_name[3]
        # Creating table and variable level metadata
        tb_garden[col].metadata.title = f"{age_group} - {sex} - {metric}"
        # tb_garden[col].metadata.short_name = underscore(tb_garden[col].metadata.title)
        tb_garden[col].metadata.unit = unit
        if tb_garden[col].metadata.unit in ["deaths", "Number of stillbirths"]:
            tb_garden[col] = tb_garden[col].astype("Int64").round(0)
        else:
            tb_garden[col] = tb_garden[col].astype("float").round(2)
        # tb_garden[col].name = underscore(tb_garden[col].metadata.title)
    # tb_garden = tb_garden.reset_index()
    tb_garden = underscore_table(tb_garden)
    ds_garden.add(tb_garden)
    ds_garden.save()
    log.info("un_igme.end")


def combine_datasets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine the UN IGME and the Gapminder datasets with a preference for the IGME data
     - Split dataframe into duplicate and non-duplicate rows (duplicate on country, indicator, sex, year)
     - Remove the Gapminder rows in the duplicated data
     - Recombine the two datasets
     - Check there are no longer any duplicates
    """

    no_dups_df = pd.DataFrame(df[~df.duplicated(subset=["country", "indicator", "sex", "year"], keep=False)])
    keep_igme = pd.DataFrame(
        df[(df.duplicated(subset=["country", "indicator", "sex", "year"], keep=False)) & (df.source == "UN IGME")]
    )
    df_clean = pd.concat([no_dups_df, keep_igme], ignore_index=True)
    assert df_clean[df_clean.groupby(["country", "indicator", "sex", "year"]).transform("size") > 1].shape[0] == 0
    return df_clean


def get_gapminder_data() -> pd.DataFrame:
    """
    Get child and infant mortality data from open numbers
    """
    gapminder_cm_df = catalog.Dataset(GAPMINDER_CHILD_MORTALITY_DATASET_PATH)
    gapminder_child_mort = pd.DataFrame(
        gapminder_cm_df["child_mortality_0_5_year_olds_dying_per_1000_born"]
    ).reset_index()
    gapminder_child_mort["indicator"] = "Under-five mortality rate"
    gapminder_child_mort["sex"] = "Total"
    gapminder_child_mort["unit"] = "Deaths per 1000 live births"
    gapminder_child_mort = gapminder_child_mort.rename(
        columns={"geo": "country", "time": "year", "child_mortality_0_5_year_olds_dying_per_1000_born": "value"}
    )
    # get infant mortality from open numbers
    gapminder_inf_m_df = catalog.Dataset(GAPMINDER_INFANT_MORTALITY_DATASET_PATH)
    gapminder_inf_mort = pd.DataFrame(gapminder_inf_m_df["infant_mortality_rate"]).reset_index()
    gapminder_inf_mort["indicator"] = "Infant mortality rate"
    gapminder_inf_mort["sex"] = "Total"
    gapminder_inf_mort["unit"] = "Deaths per 1000 live births"
    gapminder_inf_mort = gapminder_inf_mort.rename(columns={"area": "country", "infant_mortality_rate": "value"})

    df_gapminder = pd.concat([gapminder_child_mort, gapminder_inf_mort])
    df_gapminder["source"] = "Gapminder"
    # Removing rows with NA values
    df_gapminder = df_gapminder.dropna(subset="value")
    return df_gapminder


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
    df = df.drop(columns=["regional_group"])
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
        # Only calculating for both sexes
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
