"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("external_causes.csv")

    # Load data from snapshot.
    tb = snap.read()

    # replace Suppressed (causes with less than 10 deaths this year)/ Unreliable with pd.NA
    tb = tb.replace("Suppressed", pd.NA)
    tb = tb.replace("Unreliable", pd.NA)

    tb["Deaths"] = tb["Deaths"].astype("Int64")
    tb = tb.drop(columns=["Notes", "Population", "ICD Sub-Chapter Code"], errors="raise")

    tb["year"] = 2023

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["cause_of_death_code"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
