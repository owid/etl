"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

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
        "one_sided": {
            "index": ["conflict_id", "dyad_id", "year"],
        },
        "non_state": {
            "index": ["conflict_id", "dyad_id", "year"],
        },
        "battle_related_conflict": {
            "index": ["conflict_id", "dyad_id", "year"],
        },
        "battle_related_dyadic": {
            "index": ["conflict_id", "dyad_id", "year"],
        },
        "geo": {
            "index": ["id"],
        },
        "prio_armed_conflict": {
            "index": ["conflict_id", "year"],
        },
    }
    for short_name, props in short_names.items():
        snap = paths.load_snapshot(short_name=f"ucdp.{short_name}.zip")
        log.info(f"war_ucdp: creating table from {snap.path}")

        # Load data from snapshot.
        if short_name == "geo":
            df = pd.read_csv(snap.path, dtype={"gwnoa": "str"})
        else:
            df = pd.read_csv(snap.path)

        # Create a new table and ensure all columns are snake-case.
        tb = Table(df, short_name=short_name, underscore=True, camel_to_snake=True)
        # Set index
        tb = tb.set_index(props["index"], verify_integrity=True)
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
