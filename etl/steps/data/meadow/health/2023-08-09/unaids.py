"""Load a snapshot and create a meadow dataset."""

import ast
from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("unaids.csv"))

    # Load data from snapshot.
    log.info("health.unaids: loading data from snapshot")
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #
    # Split columns of type dictionary in multiple columns
    log.info("health.unaids: basic table cleaning")
    # Fields as dictionaries
    df["INDICATOR"] = df["INDICATOR"].apply(ast.literal_eval)
    df["SUBGROUP"] = df["SUBGROUP"].apply(ast.literal_eval)
    df["AREA"] = df["AREA"].apply(ast.literal_eval)
    df["UNIT"] = df["UNIT"].apply(ast.literal_eval)
    # Add new columns
    df["INDICATOR_DESCRIPTION"] = df["INDICATOR"].apply(lambda x: x["value"])
    df["INDICATOR"] = df["INDICATOR"].apply(lambda x: x["id"])
    df["SUBGROUP_DESCRIPTION"] = df["SUBGROUP"].apply(lambda x: x["value"])
    df["SUBGROUP"] = df["SUBGROUP"].apply(lambda x: x["id"])
    df["COUNTRY"] = df["AREA"].apply(lambda x: x["value"])
    df["UNIT"] = df["UNIT"].apply(lambda x: x["id"])
    # Remove unused columns
    df = df.drop(columns=["AREA"])

    # Rename column for year
    df = df.rename(columns={"TIME_PERIOD": "year"})

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    # Remove duplicates
    tb = tb.drop_duplicates(subset=["country", "year", "indicator", "subgroup"], keep="first")

    # Set index
    tb = tb.set_index(["country", "year", "indicator", "subgroup"], verify_integrity=True)

    log.info("health.unaids: fix observed values (NaNs and typos)")
    # Replace '...' with NaN
    tb["obs_value"] = tb["obs_value"].replace("...", np.nan)
    tb["obs_value"] = tb["obs_value"].replace("3488-56", np.nan)
    # Remove unwanted indicators
    log.info("health.unaids: remove unwanted indicators")
    id_desc_rm = [
        "National AIDS strategy/policy",
        "National AIDS strategy/policy includes dedicated budget for gender transformative interventions",
    ]
    tb = tb[~tb["indicator_description"].isin(id_desc_rm)]
    # Type
    tb = tb.astype(
        {
            "obs_value": float,
        }
    )
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
