"""Load a snapshot and create a meadow dataset."""

from typing import cast

import numpy as np
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("wrp_2021.start")
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("wrp_2021.zip"))

    # Read CSV from inside the zip archive (preserves origins on columns).
    with snap.extracted() as archive:
        tb = archive.read("lrf_wrp_2021_full_data.csv", low_memory=False)

    # Filter to countries present in both waves.
    countries_both_years = tb["country.in.both.waves"] == 1
    tb = tb.replace(" ", np.nan)
    tb = tb[countries_both_years].reset_index(drop=True)

    #
    # Process data.
    #
    # Ensure all columns are snake-case.
    tb = tb.underscore()
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("wrp_2021.end")
