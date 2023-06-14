"""Load a snapshot and create a meadow dataset."""

import os
import tempfile
from typing import cast

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


def run(dest_dir: str) -> None:
    log.info("cow_mid.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("cow_mid.zip"))
    # Decompress
    files_shortname = {
        "MIDA 5.0.csv": "mida",
        "MIDB 5.0.csv": "midb",
        "MIDI 5.0.csv": "midi",
        "MIDIP 5.0.csv": "midip",
    }
    tables = []
    with tempfile.TemporaryDirectory() as tmp_dir:
        decompress_file(snap.path, tmp_dir)
        for filename, short_name in files_shortname.items():
            df = pd.read_csv(os.path.join(tmp_dir, filename))
            tb = Table(df, short_name=short_name, underscore=True)
            tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("cow_mid.end")
