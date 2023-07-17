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
    log.info("mars.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("war_mars.xls"))

    # Read excel
    dfs = pd.read_excel(snap.path, sheet_name=None)
    # Load dataframe
    if "ProjectMarsV1.1" not in dfs.keys():
        raise ValueError("Sheet 'ProjectMarsV1.1' not found.")
    df = dfs["ProjectMarsV1.1"]

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)
    # Ensure correct dtypes
    for col in ["startdate", "enddate"]:
        tb[col] = pd.to_datetime(tb[col])
    tb["mic_qc"] = tb["mic_qc"].replace(" ", pd.NA).astype("Int64")

    # Set index
    tb = tb.set_index("id")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("mars.end")
