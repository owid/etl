"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from data, and how to rename them.
COLUMNS = {
    "Year": "year",
    "Enriched": "enriched",
    "Barn": "barn",
    "Free Range": "free_range",
    "Organic": "organic",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("uk_egg_statistics.ods")

    # Load data from snapshot.
    tb = pr.read_excel(snap.path, sheet_name="Packers_Annual", skiprows=2, metadata=snap.to_table_metadata())

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Remove spurious rows at the bottom of the file, containing footnotes.
    # To achieve that, detect rows where the year column is not a number.
    tb = tb[tb["year"].str.match(r"\d{4}", na=True)].reset_index(drop=True)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
