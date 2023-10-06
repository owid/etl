"""Load a snapshot and create a meadow dataset."""

import os
import tempfile
from pathlib import Path

import chardet
import owid.catalog.processing as pr
from owid.datautils.io import decompress_file
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

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

    # List of tables
    tables = []

    # CSVs to tables
    snapshots = [
        ("cow_extra_state.csv", ["warnum", "ccode1", "ccode2"]),
        ("cow_inter_state.csv", ["warnum", "ccode", "side"]),
        ("cow_non_state.csv", ["warnum", "sidea1"]),
    ]
    for snapshot_uri, snapshot_index in snapshots:
        snap = paths.load_snapshot(snapshot_uri)
        encoding = infer_encoding(snap.path)
        log.info(f"cow: creating table from {snap.path}")
        tb = snap.read(encoding=encoding, underscore=True)
        # tb = Table(df, short_name=snapshot_props["short_name"], underscore=True)
        tb = tb.set_index(snapshot_index, verify_integrity=True)
        tables.append(tb)

    # ZIP to table
    snapshots = [
        ("cow_intra_state.zip", "INTRA-STATE WARS v5.1 CSV.csv", ["warnum"]),
        ("cow_inter_state_dyadic.zip", "directed_dyadic_war.csv", ["warnum", "disno", "year", "statea", "stateb"]),
    ]
    for uri, fname, index in snapshots:
        snap = paths.load_snapshot(uri)
        with tempfile.TemporaryDirectory() as tmpdir:
            decompress_file(snap.path, tmpdir)
            path = os.path.join(tmpdir, fname)  # other file: "INTRA-STATE_State_participants v5.1 CSV.csv"
            encoding = infer_encoding(path)
            log.info(f"cow: creating table from {snap.path}")
            tb = pr.read_csv(
                path, encoding=encoding, metadata=snap.to_table_metadata(), origin=snap.m.origin, underscore=True
            )
            tb = tb.set_index(index, verify_integrity=True)
            tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    log.info("cow: creating dataset")
    ds_meadow = create_dataset(dest_dir, tables=tables, default_metadata=snap.metadata, check_variables_metadata=True)  # type: ignore

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("cow: end")
