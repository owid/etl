"""Load a snapshot and create a meadow dataset."""

import numpy as np

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

column_index = ["country", "year", "indicator", "dimension"]


def run() -> None:
    #
    # Load inputs.
    #
    short_names = [
        "epi",
        "gam",
        "kpa",
        "ncpi",
    ]
    tables = []
    for name in short_names:
        # Retrieve snapshot.
        short_name = f"unaids_{name}.zip"
        snap = paths.load_snapshot(short_name)

        # Load data from snapshot.
        tb = snap.read_csv()

        #
        # Process data.
        #
        tb = clean_table(tb)
        # Format table
        tb = tb.format(column_index, short_name=name)

        # Append current table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)

    # Save meadow dataset.
    ds_meadow.save()


def clean_table(tb):
    """Minor table cleaning."""
    paths.log.info(f"Formatting table {tb.m.short_name}")

    # Rename columns, only keep relevant
    columns = {
        "Indicator": "indicator",
        "Unit": "unit",
        "Subgroup": "dimension",
        "Area": "country",
        # "Area ID" : "",
        "Time Period": "year",
        "Source": "source",
        "Data value": "value",
        "Formatted": "value_rounded",
        "Data_Denominator": "data_denominator",
        "Footnote": "footnote",
    }
    tb = tb.rename(columns=columns)[columns.values()]

    # Drop duplicates
    tb = tb.drop_duplicates(subset=["country", "year", "indicator", "dimension"], keep="first")

    # Handle NaNs
    # tb.loc[:, "value"] = tb["value"].replace("...", np.nan)
    tb = tb.dropna(subset=["value"])

    return tb
