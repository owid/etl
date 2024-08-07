"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("katic_leigh_2015_australia.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Top wealth data", usecols="B,I", skiprows=4)

    #
    # Process data.
    #
    # Rename Unnamed: 1 to year
    tb = tb.rename(
        columns={
            "Unnamed: 1": "year",
            "top 1% wealth": "share_p99p100_wealth",
        }
    )

    # Drop missing values
    tb = tb.dropna()

    # Add a country column with the value Australia
    tb["country"] = "Australia"

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
