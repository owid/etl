"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("height.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Data Long Format")

    # Sanity checks
    assert tb.groupby("ccode")["country.name"].nunique().max() == 1, "ccode is not unique"
    assert tb.groupby("country.name")["ccode"].nunique().max() == 1, "country.name is not unique"

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [
        tb.format(["country.name", "year"]),
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
