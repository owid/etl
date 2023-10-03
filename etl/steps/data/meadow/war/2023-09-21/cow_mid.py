"""Load a snapshot and create a meadow dataset."""

import os
import tempfile

import owid.catalog.processing as pr
from owid.datautils.io import decompress_file
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("cow_mid.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("cow_mid.zip")
    # Decompress
    files = {
        "MIDA 5.0.csv": {
            "short_name": "mida",
            "index": ["dispnum"],
        },
        "MIDB 5.0.csv": {
            "short_name": "midb",
            "index": ["dispnum", "ccode", "styear", "endyear"],
        },
        "MIDI 5.0.csv": {
            "short_name": "midi",
            "index": ["dispnum", "incidnum"],
        },
        "MIDIP 5.0.csv": {
            "short_name": "midip",
            "index": ["dispnum", "incidnum", "ccode"],
        },
    }
    tables = []
    with tempfile.TemporaryDirectory() as tmp_dir:
        decompress_file(snap.path, tmp_dir)
        for filename, file_props in files.items():
            tb = pr.read_csv(
                os.path.join(tmp_dir, filename),
                metadata=snap.to_table_metadata(),
                origin=snap.metadata.origin,
                underscore=True,
            )
            tb.m.short_name = file_props["short_name"]
            tb = tb.set_index(file_props["index"], verify_integrity=True)
            tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, default_metadata=snap.metadata, check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("cow_mid.end")
