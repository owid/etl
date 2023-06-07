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
    log.info("prio_v1.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("prio_v1.zip"))

    # Load data from snapshot and create tables
    files_relevant = {
        "COW_Interstate_War_Conflict_Years 1.0.xls": "inter_state",
        "COW_Intrastate_War_Conflict_Years 1.0.xls": "intra_state",
        "COW_Extrastate_War_Conflict_Years 1.0.xls": "extra_state",
    }
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Decompress main zip
        decompress_file(snap.path, tmp_dir)
        # Check zip file is in decompressed items & decompress its content
        path = os.path.join(tmp_dir, "COW Data.zip")
        if not os.path.isfile(path):
            raise ValueError(f"Could not find {path}! Content of zip folder from source may have changed!")
        decompress_file(path, tmp_dir)
        # Read files matching regex
        tables = [
            Table(pd.read_excel(os.path.join(tmp_dir, filename)), short_name=short_name, underscore=True)
            for filename, short_name in files_relevant.items()
        ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("prio_v1.end")
