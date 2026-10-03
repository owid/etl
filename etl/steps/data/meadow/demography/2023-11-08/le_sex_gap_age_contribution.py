"""Load a snapshot and create a meadow dataset."""

import tempfile
from pathlib import Path

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("le_sex_gap_age_contribution.zip")

    # Load data from snapshot.
    with tempfile.TemporaryDirectory() as tmpdir:
        snap.extract(tmpdir)
        tb = pr.read_rda(
            Path(tmpdir) / "sex-gap-e0-pnas-2.0" / "dat" / "a6gap33cntrs.rda",
            "df6",
            metadata=snap.to_table_metadata(),
            origin=snap.metadata.origin,
        )
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["name", "year", "age_group"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
