"""Load a snapshot and create a meadow dataset."""



from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("afrobarometer_trust.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    # Set indices, verify integrity and sort.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table and ensure all columns are snake-case.
    tb = tb.underscore()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
