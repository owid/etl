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
    log.info("yougov_robots.start")
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("yougov_robots.xlsx")

    #
    # Process data.
    #
    tb_all_sheets = shared.process_data(snap)

    tb_all_sheets = tb_all_sheets.underscore()
    tb_all_sheets = tb_all_sheets.set_index(
        ["which_one__if_any__of_the_following_statements_do_you_most_agree_with", "date", "group"],
        verify_integrity=True,
    ).sort_index()
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb_all_sheets], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("yougov_robots.end")
