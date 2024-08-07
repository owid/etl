from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("refugee_data.zip")

    # Load data from snapshot.
    tb = snap.read_in_archive("population.csv", header=13, na_values=["-"])

    # drop duplicate country columns
    tb = tb.drop(columns=["Country of origin (ISO)", "Country of asylum (ISO)"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country_of_origin", "country_of_asylum", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
