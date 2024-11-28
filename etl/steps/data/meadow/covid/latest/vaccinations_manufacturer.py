"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Columns rename
COLUMNS_RENAME = {"location": "country"}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("vaccinations_manufacturer.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Rename columns
    tb = tb.rename(columns=COLUMNS_RENAME)
    # Dtypes
    tb = set_dtypes(tb)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "date", "vaccine"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def set_dtypes(tb: Table) -> Table:
    """Set dtypes for indicators."""
    tb = tb.astype(
        {
            "date": "datetime64[ns]",
            "country": "string",
            "total_vaccinations": int,
        }
    )
    return tb
