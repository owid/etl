import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("unodc.start")

    # read dataset from meadow
    ds_meadow = Dataset(DATA_DIR / "meadow/homicide/2023-01-04/unodc")
    tb_meadow = ds_meadow["unodc"]

    df = pd.DataFrame(tb_meadow)

    log.info("unodc.exclude_countries")
    df = exclude_countries(df)

    log.info("unodc.harmonize_countries")
    df = harmonize_countries(df)

    df = clean_data(df)
    # create new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)

    # create new table with the same metadata as meadow and add it to dataset
    tb_garden = underscore_table(Table(df, short_name=tb_meadow.metadata.short_name))
    ds_garden.add(tb_garden)

    # update metadata from yaml file
    ds_garden.update_metadata(paths.metadata_path)

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
    # Making the units more obvious
    df["unit_of_measurement"] = df["unit_of_measurement"].replace(
        ["Counts", "Rate per 100,000 population"], ["Homicide count", "Homicides per 100,000 population"]
    )
    # Splitting the data into that which has the totals and that which is disaggregated by mechanism
    df_mech = df[df["dimension"] == "by mechanisms"]

    df_mech = create_mechanism_df(df_mech)

    df_tot = df[df["dimension"] == "Total"]

    df_tot = create_total_df(df_tot)

    df = pd.merge(df_mech, df_tot, how="outer", on=["country", "year"])

    return df


def create_total_df(df_tot: pd.DataFrame) -> pd.DataFrame:
    """Create the total homicides dataframe where we will have total homicides/homicide rate
    disaggregated by age and sex
    """
    df_tot = df_tot.drop(columns=["region", "subregion", "indicator", "dimension", "category", "source"])
    # There are some duplicates when sex is unknown so let's remove those rows
    df_tot = df_tot[df_tot["sex"] != "Unknown"]
    # Make it more obvious what total age and total sex means
    df_tot["age"] = df_tot["age"].replace("Total", "All ages")
    df_tot["sex"] = df_tot["sex"].replace("Total", "Both sexes")

    df_tot = df_tot.pivot(index=["country", "year"], values=["value"], columns=["sex", "age", "unit_of_measurement"])
    # Make the columns nice
    df_tot.columns = df_tot.columns.droplevel(0)
    df_tot.columns = df_tot.columns.map("_".join)
    df_tot = df_tot.reset_index()
    return df_tot


def create_mechanism_df(df_mech: pd.DataFrame) -> pd.DataFrame:
    """Create the homicides by mechanism dataframe where we will have  homicides/homicide rate
    disaggregated by mechanism (e.g. weapon)
    """
    df_mech = df_mech.drop(columns=["region", "subregion", "indicator", "dimension", "source", "sex", "age"])
    df_mech["category"] = df_mech["category"].replace(
        ["Firearms or explosives - firearms", "Another weapon - sharp object"], ["Firearms", "Sharp object"]
    )
    # Make the table wider so we have a column for each mechanism
    df_mech = df_mech.pivot(index=["country", "year"], values=["value"], columns=["category", "unit_of_measurement"])
    # Make the columns nice
    df_mech.columns = df_mech.columns.droplevel(0)
    df_mech.columns = df_mech.columns.map("_".join)
    df_mech = df_mech.reset_index()
    return df_mech
