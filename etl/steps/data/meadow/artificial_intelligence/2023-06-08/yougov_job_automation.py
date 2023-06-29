"""Load a snapshot and create a meadow dataset."""

from typing import cast

import shared
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("yougov_job_automation.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("yougov_job_automation.xlsx"))
    excel_object = shared.load_data(snap)
    df_all_sheets = shared.process_data(excel_object)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df_all_sheets, short_name=paths.short_name, underscore=True)
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("yougov_job_automation.end")
