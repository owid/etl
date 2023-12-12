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
    tb = snap.read(underscore=False)

    #
    # Process data.
    #
    # Use the current names of the columns as the variable titles in the metadata.
    # for column in tb.drop(columns=["Country", "Year"]).columns:
    #     tb[column].metadata.title = column
    #     tb[column].metadata.unit = "TWh"
    #     tb[column].metadata.description_short = "Measured in terawatt-hours."

    # Ensure all columns are snake-case, set an appropriate index and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
