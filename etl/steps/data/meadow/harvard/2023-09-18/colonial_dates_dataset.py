"""Load a snapshot and create a meadow dataset."""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("colonial_dates_dataset.csv")

    # Load data from snapshot.
    tb = snap.read(
        encoding="cp1252",
        dtype={
            "country": "string",
            "colonizer": "string",
            "col": "string",
            "colstart_max": "Int64",
            "colend_max": "Int64",
            "colstart_mean": "Int64",
            "colend_mean": "Int64",
        },
    )

    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "colonizer"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
