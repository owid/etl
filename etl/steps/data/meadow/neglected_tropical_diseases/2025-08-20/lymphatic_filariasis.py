"""Load a snapshot and create a meadow dataset."""

import numpy as np

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("lymphatic_filariasis.xlsx")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    tb = tb.rename(columns={"Country": "country", "Year": "year", "Type of MDA": "type_of_mda"}, errors="raise")
    tb = tb.replace("No data", np.nan)
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "type_of_mda"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
