"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("vaccination_schedules.xlsx")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    tb = tb.rename(columns={"COUNTRYNAME": "country"})
    tb = tb.dropna(subset=["country"])
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.

    tables = [tb.format(["country", "year", "description"])]

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
