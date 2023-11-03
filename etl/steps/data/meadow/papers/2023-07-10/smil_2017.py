"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("smil_2017.csv")

    # Load data from snapshot.
    tb = snap.read_csv(underscore=False)

    #
    # Process data.
    #
    # Use the current names of the columns as the variable titles in the metadata.
    for column in tb.columns:
        tb[column].metadata.title = column

    # Ensure all columns are snake-case.
    tb = tb.underscore()

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata, check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_meadow.save()
