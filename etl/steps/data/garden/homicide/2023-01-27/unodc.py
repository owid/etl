import json
from typing import List, cast

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("unodc.start")

    # read dataset from meadow
    ds_meadow = paths.meadow_dataset
    tb_meadow = ds_meadow["unodc"]

    df = pd.DataFrame(tb_meadow)

    log.info("unodc.exclude_countries")
    df = exclude_countries(df)

    log.info("unodc.harmonize_countries")
    df = harmonize_countries(df)

    df = clean_data(df)

    # create new dataset with the same metadata as meadow
    ds_garden = create_dataset(
        dest_dir, tables=[Table(df, short_name=tb_meadow.metadata.short_name)], default_metadata=ds_meadow.metadata
    )
    ds_garden.save()

    log.info("unodc.end")


def load_excluded_countries() -> List[str]:
    with open(paths.excluded_countries_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(df: pd.DataFrame) -> pd.DataFrame:
    excluded_countries = load_excluded_countries()
    return cast(pd.DataFrame, df.loc[~df.country.isin(excluded_countries)])


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(paths.country_mapping_path))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {paths.country_mapping_path} to include these country "
            f"names; or (b) add them to {paths.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(deep=True)

    # Splitting the data into that which has the totals and that which is disaggregated by mechanism
    df_mech = df[df["dimension"] == "by mechanisms"]

    df_mech = create_mechanism_df(df_mech)

    df_tot = df[df["dimension"] == "Total"]

    df_tot = create_total_df(df_tot)

    df = pd.merge(df_mech, df_tot, how="outer", on=["country", "year"])

    # Reconciling the variable names with previous aggregated version

    df = df.rename(
        columns={
            "Both sexes_All ages_Rate per 100,000 population": "Rate per 100,000 population",
            "Both sexes_All ages_Counts": "Counts",
        }
    )

    return df


def create_mechanism_df(df_mech: pd.DataFrame) -> pd.DataFrame:
    """
    Create the homicides by mechanism dataframe where we will have  homicides/homicide rate
    disaggregated by mechanism (e.g. weapon)
    """
    # df_mech = df_mech.drop(columns=["region", "subregion", "indicator", "dimension", "source", "sex", "age"])
    df_mech = df_mech.copy(deep=True)
    df_mech["category"] = (
        df_mech["category"]
        .map({"Firearms or explosives - firearms": "Firearms", "Another weapon - sharp object": "Sharp object"})
        .fillna(df_mech["category"])
    )

    # Make the table wider so we have a column for each mechanism
    df_mech = pivot_and_format_df(
        df_mech,
        drop_columns=["region", "subregion", "indicator", "dimension", "source", "sex", "age"],
        pivot_index=["country", "year"],
        pivot_values=["value"],
        pivot_columns=["category", "unit_of_measurement"],
    )

    return df_mech


def create_total_df(df_tot: pd.DataFrame) -> pd.DataFrame:
    """
    Create the total homicides dataframe where we will have total homicides/homicide rate
    disaggregated by age and sex
    """
    # To escape the dataframe slice warnings
    df_tot = df_tot.copy(deep=True)
    # There are some duplicates when sex is unknown so let's remove those rows
    df_tot = df_tot[df_tot["sex"] != "Unknown"]

    # Make it more obvious what total age and total sex means

    df_tot["age"] = df_tot["age"].map({"Total": "All ages"}, na_action="ignore").fillna(df_tot["age"])
    df_tot["sex"] = df_tot["sex"].map({"Total": "Both sexes"}, na_action="ignore").fillna(df_tot["sex"])

    df_tot = pivot_and_format_df(
        df_tot,
        drop_columns=["region", "subregion", "indicator", "dimension", "category", "source"],
        pivot_index=["country", "year"],
        pivot_values=["value"],
        pivot_columns=["sex", "age", "unit_of_measurement"],
    )
    return df_tot


def pivot_and_format_df(df, drop_columns, pivot_index, pivot_values, pivot_columns):
    """
    - Dropping a selection of columns
    - Pivoting by the desired disaggregations e.g. category, unit of measurement
    - Tidying the column names
    """
    df = df.drop(columns=drop_columns)
    df = df.pivot(index=pivot_index, values=pivot_values, columns=pivot_columns)
    # Make the columns nice
    df.columns = df.columns.droplevel(0)
    df.columns = df.columns.map("_".join)
    df = df.reset_index()
    return df
