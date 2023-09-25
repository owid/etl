"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("war_ucdp: start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    tables = []
    short_names = {
        "one_sided",
        "non_state",
        "battle_related_conflict",
        "battle_related_dyadic",
        "geo",
        "prio_armed_conflict",
    }
    for short_name in short_names:
        snap = cast(Snapshot, paths.load_dependency(short_name=f"ucdp_{short_name}.zip"))
        log.info(f"war_ucdp: creating table from {snap.path}")

        # Load data from snapshot.
        if short_name == "geo":
            df = pd.read_csv(snap.path, dtype={"gwnoa": "str"})
        else:
            df = pd.read_csv(snap.path)

        # Create a new table and ensure all columns are snake-case.
        tb = Table(df, short_name=short_name, underscore=True, camel_to_snake=True)
        # Add table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, default_metadata=snap.metadata)  # type: ignore

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("war_ucdp: end")
