"""Load a snapshot and create a meadow dataset."""

from typing import cast

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snaps = [
        paths.load_snapshot(f"comtrade_pandemics_{years}.csv") for years in ["1987_1998", "1999_2010", "2011_2022"]
    ]
    # Default metadata (metadata is common to all snapshots)
    snap_metadata = snaps[0].metadata

    # Load data from snapshot.
    tbs = [snap.read_csv(encoding="latin-1") for snap in snaps]
    tb = pr.concat(tbs, ignore_index=True, short_name=paths.short_name)

    # Sanity checks
    assert (tb["Period"] == tb["RefYear"]).all(), "`period` != `refyear`!"
    assert (
        tb.groupby(["RefYear", "ReporterDesc", "CmdCode"]).size().max() == 1
    ), "There should be, at most, one entry per (refyear, reporterdesc, cmdcode) triplet"

    #
    # Process data.
    #
    tb = tb.set_index(["RefYear", "ReporterDesc", "CmdCode"], verify_integrity=True)
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap_metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
