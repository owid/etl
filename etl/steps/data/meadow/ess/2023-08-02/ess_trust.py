"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger.
log = get_logger()


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename trust questions columns and separate country and year."""

    # Define dictionary of columns to rename ppltrst trstep trstlgl trstplc trstplt trstprl trstprt trstun gvimpc19 trstsci
    rename_dict = {
        "ppltrst": "trust",
        "trstep": "trust_european_parliament",
        "trstlgl": "trust_legal_system",
        "trstplc": "trust_police",
        "trstplt": "trust_politicians",
        "trstprl": "trust_local_parliament",
        "trstprt": "trust_political_parties",
        "trstun": "trust_united_nations",
        "gvimpc19": "trust_gov_covid19",
        "trstsci": "trust_scientists",
    }

    # Rename columns.
    df = df.rename(columns=rename_dict)

    # Extract first two characters from survey column and name it country. Extract last four characters from survey column and name it year.
    df["country"] = df["survey"].str[:2]
    df["year"] = df["survey"].str[-4:].astype(int)

    # Remove survey column and move country and year to the front.
    df = df.drop(columns=["survey"])
    df = df[["country", "year"] + list(df.columns[:-2])]

    # Harmonize ISO2 codes to OWID standard
    df = harmonize_countries(df)

    # Set index and verify that it is unique. And sort.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    return df


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    # Load reference file with country names in OWID standard
    df_countries_regions = cast(Dataset, paths.load_dependency("regions"))["regions"]

    # Merge dataset and country dictionary to get the name of the country
    df = pd.merge(
        df, df_countries_regions[["name", "iso_alpha2"]], left_on="country", right_on="iso_alpha2", how="left"
    )

    missing_list = list(df[df["name"].isnull()]["country"].unique())
    missing_count = len(missing_list)

    # Warns if there are still entities missing
    if missing_count > 0:
        log.warning(
            f"There are still {missing_count} countries/regions without a name and will be deleted! Take a look at this list:\n {missing_list}"
        )

    # Drop rows without match
    df = df[~df["name"].isnull()].reset_index(drop=True)
    # Drop old country and ISO alpha 2 variable. Rename the newly built variable as `country`
    df = df.drop(columns=["country", "iso_alpha2"])
    df = df.rename(columns={"name": "country"})

    return df


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("ess_trust.csv"))

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    #
    # Process data.
    df = rename_columns(df)

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
