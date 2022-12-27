"""Get data on completeness of death registration based on Karlinsky (2021) paper.

Data is hosted on GitHub's repo: https://github.com/akarlinsky/death_registration and is updated regularly.
Due to the update frequency, we have decided to (i) directly source the data from the repository instead of
going through snapshot and meadow steps and (ii) use 'latest' version in garden instead of specific snapshots.
"""

import datetime as dt
import os

import pandas as pd
from structlog import get_logger
from owid.catalog import Dataset, Table
from owid.datautils import geo

from etl.helpers import downloaded
from etl.helpers import Names as N


CURRENT_DIR = os.path.dirname(__file__)
PATH_DATASET = "https://raw.githubusercontent.com/akarlinsky/death_registration/main/death_reg_final.csv"
PATH_METADATA = os.path.join(CURRENT_DIR, "meta.yaml")
PATH_COUNTRIES = os.path.join(CURRENT_DIR, "countries.json")
log = get_logger()


def run(dest_dir: str) -> None:
    # load dataframe
    log.info("karlinsky: loading dataframe")
    df = load_dataframe()

    # build table
    log.info("karlinsky: converting dataframe to table")
    tb = Table(df, short_name="death_registration")

    # build dataset
    log.info("karlinsky: creating dataset")
    ds = Dataset.create_empty(dest_dir)

    # add table
    log.info("karlinsky: adding table to dataset")
    ds.add(tb)

    # update metadata
    log.info("karlinsky: adding metadata")
    ds.update_metadata(PATH_METADATA)
    ds.metadata.date_accessed = dt.date.today().strftime("%Y-%m-%d")

    # save
    log.info("karlinsky: saving dataset")
    ds.save()


def load_dataframe() -> pd.DataFrame:
    # read data from source
    with downloaded(PATH_DATASET) as filename:
        df = pd.read_csv(filename)
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
    df = geo.harmonize_countries(df=df, countries_file=str(PATH_COUNTRIES))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {N.country_mapping_path} to include these country "
            f"names; or (b) add them to {N.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df
