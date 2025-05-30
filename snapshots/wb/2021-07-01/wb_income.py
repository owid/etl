"""Imports World Bank income groups."""

import unicodedata

import click
import pandas as pd

from etl.snapshot import Snapshot

SOURCE_DATA_URL = "http://databank.worldbank.org/data/download/site-content/CLASS.xlsx"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot("wb/2021-07-01/wb_income.xlsx")

    assert snap.m.source

    # Update description
    df_new = pd.read_excel(SOURCE_DATA_URL, sheet_name="Notes")
    s = "\n\n".join(df_new.dropna().Notes.tolist())
    snap.m.source.description = unicodedata.normalize("NFKD", s)

    # Save snapshot.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()
