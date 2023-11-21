"""Load a snapshot and create a meadow dataset."""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_dependency("oecd_education.csv")

    # Load data from snapshot.
    tb = snap.read_csv()
    tb.rename(columns={"country_or_region": "country"}, inplace=True)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
