"""Load a snapshot and create a meadow dataset."""
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("who_statins.csv")

    # Load data from snapshot.
    tb = snap.read_csv()
    #
    # Process data.
    #
    #  Keep only the columns of interest.
    columns_of_interest = ["Location", "Period", "Value"]
    tb = tb[columns_of_interest]
    # Rename columns.
    tb.rename(
        columns={
            "Location": "country",
            "Period": "year",
            "Value": "General availability of statins in the public health sector",
        },
        inplace=True,
    )
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
