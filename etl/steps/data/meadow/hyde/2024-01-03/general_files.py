"""Load a snapshot and create a meadow dataset."""
import os
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
    snap = paths.load_snapshot("general_files.zip")

    # Load data from snapshot.
    with snap.extract_to_tempdir() as tmpdir:
        # Get all files in the directory
        files = os.listdir(tmpdir)
        # Sanity check
        fname_expected = "HYDE_country_codes.xlsx"
        assert fname_expected in files, f"The directory should contain a file named {fname_expected} (empty string)!"
        # Read file
        tb = pr.read_excel(
            Path(tmpdir) / fname_expected,
            metadata=snap.to_table_metadata(),
            origin=snap.metadata.origin,
        )

    #
    # Process data.
    #
    # Keep good columns
    tb = tb[["ISO-CODE", "Country"]]

    # Filter spurious entries
    mask = (tb["ISO-CODE"] == 826) & (tb["Country"] != "United Kingdom  ")
    tb = tb[~mask]

    # Strip country names
    tb["Country"] = tb["Country"].str.strip()

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["iso_code"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
