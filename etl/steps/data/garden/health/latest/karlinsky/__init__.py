"""Get data on completeness of death registration based on Karlinsky (2021) paper.

Data is hosted on GitHub's repo: https://github.com/akarlinsky/death_registration and is updated regularly.
Due to the update frequency, we have decided to (i) directly source the data from the repository instead of
going through snapshot and meadow steps and (ii) use 'latest' version in garden instead of specific snapshots.
"""

import datetime as dt
import os

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import Names as N
from etl.helpers import downloaded

CURRENT_DIR = os.path.dirname(__file__)
PATH_DATASET = "https://raw.githubusercontent.com/akarlinsky/death_registration/main/death_reg_final.csv"
PATH_METADATA = os.path.join(CURRENT_DIR, "meta.yaml")
PATH_COUNTRIES = os.path.join(CURRENT_DIR, "countries.json")
log = get_logger()


def run(dest_dir: str) -> None:
    # load dataframe
    log.info("karlinsky: loading dataframe...")
    df = load_dataframe()

    # sanity checks
    log.info("karlinsky: sanity checking...")
    sanity_check(df)

    # build table
    log.info("karlinsky: converting dataframe to table...")
    tb = Table(df, short_name="death_registration")

    # build dataset
    log.info("karlinsky: creating dataset...")
    ds = Dataset.create_empty(dest_dir)

    # add table
    log.info("karlinsky: adding table to dataset...")
    ds.add(tb)

    # update metadata
    log.info("karlinsky: adding metadata...")
    ds.update_metadata(PATH_METADATA)
    ds.metadata.date_accessed = dt.date.today().strftime("%Y-%m-%d")

    # save
    log.info("karlinsky: saving dataset...")
    ds.save()


def load_dataframe() -> pd.DataFrame:
    # read data from source
    with downloaded(PATH_DATASET) as filename:
        df = pd.read_csv(filename, sep=",")
    # drop and rename columns
    df = df.drop(columns=["continent", "source"])
    df = df.rename(columns={"country_name": "country"})
    # harmonize country names
    df = harmonize_countries(df)
    # set indexes
    df = df.set_index(["country", "year"]).sort_index()
    return df


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(
        df=df,
        countries_file=str(PATH_COUNTRIES),
        warn_on_missing_countries=True,
        make_missing_countries_nan=True,
    )

    missing_countries = set(unharmonized_countries[df["country"].isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {N.country_mapping_path} to include these country "
            f"names; or (b) add them to {N.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df


def sanity_check(df: pd.DataFrame) -> None:
    # check columns
    columns_expected = {
        "death_comp",
        "expected_deaths",
        "expected_gbd",
        "expected_ghe",
        "expected_wpp",
        "reg_deaths",
    }
    columns_new = set(df.columns).difference(columns_expected)
    if columns_new:
        raise ValueError(f"Unexpected columns {columns_new}")

    # ensure percentages make sense (within range [0, 100])
    columns_perc = ["death_comp"]
    for col in columns_perc:
        assert all(df[col] <= 100), f"{col} has values larger than 100%"
        assert all(df[col] >= 0), f"{col} has values lower than 0%"

    # ensure absolute values make sense (positive, lower than population)
    columns_absolute = [col for col in df.columns if col not in columns_perc]
    df_ = df.reset_index()
    df_ = geo.add_population_to_dataframe(df_)
    for col in columns_absolute:
        x = df_.dropna(subset=[col])
        assert all(
            x[col] < 0.2 * x["population"]
        ), f"{col} contains values that might be too large (comapred to population values)!"
