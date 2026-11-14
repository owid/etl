"""Load the snapshots of Defra's poultry and poultry meat statistics and of its June survey of agriculture, and create a
meadow dataset."""

import re

import numpy as np
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Annual sheets to read from each snapshot, and the short name of the resulting table.
SHEETS = {
    "uk_poultry_placings.ods": {"UK_Placings_Annual": "chicks_placed"},
    "uk_poultry_slaughterings.ods": {"Slaughterings_Annual": "poultry_slaughtered"},
}


def clean_column_name(column: str) -> str:
    """Convert a column name of the source sheets to snake case, e.g. "Boiling Fowl E&W[1]" to
    "boiling_fowl_england_and_wales"."""
    column = re.sub(r"\[\d+\]", "", column).replace("E&W", "England and Wales")
    return re.sub(r"\W+", "_", column).strip("_").lower()


def read_annual_sheet(snap: Snapshot, sheet_name: str, short_name: str) -> Table:
    # The third row holds the column names, and footnotes follow the data.
    tb = snap.read_excel(sheet_name=sheet_name, header=2)
    tb = tb.rename(columns={column: clean_column_name(column) for column in tb.columns}, errors="raise")

    # Years of 53 statistical weeks are marked with "**", and rows without a year are footnotes.
    tb["year"] = pd.to_numeric(tb["year"].astype(str).str.replace("**", "", regex=False), errors="coerce")
    tb = tb.dropna(subset=["year"]).reset_index(drop=True)
    tb["year"] = tb["year"].astype(int)

    # Confidential values are marked "[c]".
    tb = tb.replace("[c]", np.nan)
    for column in tb.columns.drop("year"):
        tb[column] = tb[column].astype(float)

    # Improve table format.
    tb = tb.format(["year"], short_name=short_name)

    return tb


# Rows of the "Poultry" sheet of the June survey (numbers of birds at 1 June) to read, and their new names.
JUNE_SURVEY_ROWS = {
    "Total breeding and laying fowl": "total_breeding_and_laying_fowl",
    "Hens and pullets laying eggs for eating": "hens_and_pullets_laying_eggs_for_eating",
    "Breeding flock": "breeding_flock",
}


def read_june_survey(snap: Snapshot) -> Table:
    # The third row holds the years (the last column is a percentage change), and each series is a row labeled in the
    # first column.
    tb_raw = snap.read_excel(sheet_name="Poultry", header=None)
    labels = tb_raw.iloc[:, 0].astype(str).str.strip()
    data = {"year": tb_raw.iloc[2, 1:].values}
    for label, column in JUNE_SURVEY_ROWS.items():
        matches = labels[labels == label].index
        assert len(matches) == 1, f"Row {label!r} not found once in the June survey's Poultry sheet."
        data[column] = tb_raw.iloc[matches[0], 1:].values
    df = pd.DataFrame(data)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"]).reset_index(drop=True)
    df["year"] = df["year"].astype(int)
    # Values that are not available are marked "[x]".
    for column in JUNE_SURVEY_ROWS.values():
        df[column] = pd.to_numeric(df[column], errors="coerce")
    tb = snap.read_from_df(df)

    # Improve table format.
    tb = tb.format(["year"], short_name="june_survey")

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots and read their annual sheets.
    tables = [
        read_annual_sheet(snap=paths.load_snapshot(snapshot_name), sheet_name=sheet_name, short_name=short_name)
        for snapshot_name, sheets in SHEETS.items()
        for sheet_name, short_name in sheets.items()
    ]
    tables.append(read_june_survey(snap=paths.load_snapshot("uk_livestock_populations.ods")))

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)
    ds_meadow.save()
