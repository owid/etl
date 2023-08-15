"""Load a snapshot and create a meadow dataset."""

import os
import tempfile
from pathlib import Path
from typing import cast

import chardet
import pandas as pd
from owid.catalog import Table
from owid.datautils.io import decompress_file
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def infer_encoding(path):
    """Infer encoding of a file."""
    detected = chardet.detect(Path(path).read_bytes())
    encoding = detected.get("encoding")
    return encoding


def run(dest_dir: str) -> None:
    log.info("cow: start")

    #
    # Load inputs & create tables
    #

    tables = []
    # CSVs to tables
    uri_shortname = {
        "cow.extra_state.csv": "extra_state",
        "cow.inter_state.csv": "inter_state",
        "cow.non_state.csv": "non_state",
    }
    for snapshot_uri, short_name in uri_shortname.items():
        snap = cast(Snapshot, paths.load_dependency(snapshot_uri))
        encoding = infer_encoding(snap.path)
        log.info(f"cow: creating table from {snap.path}")
        df = pd.read_csv(snap.path, encoding=encoding)
        tb = Table(df, short_name=short_name, underscore=True)
        tables.append(tb)
    # ZIP to table
    ## Intra-state
    snap = cast(Snapshot, paths.load_dependency("cow.intra_state.zip"))
    with tempfile.TemporaryDirectory() as tmpdir:
        decompress_file(snap.path, tmpdir)
        path = os.path.join(
            tmpdir, "INTRA-STATE WARS v5.1 CSV.csv"
        )  # other file: "INTRA-STATE_State_participants v5.1 CSV.csv"
        encoding = infer_encoding(path)
        log.info(f"cow: creating table from {snap.path}")
        df = pd.read_csv(path, encoding=encoding)
        tb = Table(df, short_name="intra_state", underscore=True)
        tables.append(tb)
    ## Inter-state (dydadic)
    snap = cast(Snapshot, paths.load_dependency("cow.inter_state_dyadic.zip"))
    with tempfile.TemporaryDirectory() as tmpdir:
        decompress_file(snap.path, tmpdir)
        path = os.path.join(
            tmpdir, "directed_dyadic_war.csv"
        )  # other file: "INTRA-STATE_State_participants v5.1 CSV.csv"
        encoding = infer_encoding(path)
        log.info(f"cow: creating table from {snap.path}")
        df = pd.read_csv(path, encoding=encoding)
        tb = Table(df, short_name="inter_state_dyadic", underscore=True)
        tables.append(tb)
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    log.info("cow: creating dataset")
    ds_meadow = create_dataset(dest_dir, tables=tables, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("cow: end")
