"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename trust questions columns and separate country and year."""

    # Define dictionary of columns to rename ppltrst trstep trstlgl trstplc trstplt trstprl trstprt trstun gvimpc19 trstsci
    rename_dict = {
        "ppltrst": "trust_people",
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

    # Set index and verify that it is unique. And sort.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

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
