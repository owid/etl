"""Load a snapshot and create a meadow dataset."""

import numpy as np
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("war_brecke.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("war_brecke.xlsx")

    # Load data from snapshot.
    tb = snap.read_excel(underscore=True)

    #
    # Process data.
    #
    tb["id"] = np.arange(1, len(tb) + 1)
    tb["id"] = tb["id"].copy_metadata(tb["totalfatalities"])

    # Set index
    tb = tb.set_index(["id"], verify_integrity=True)

    # Update shortname
    tb.m.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata, check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("war_brecke.end")
