"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its data.
    snap = paths.load_snapshot("organ_donation_and_transplantation.xlsx")
    tb = snap.read()

    # Population column has a bad formatting, so, for now, store it as a string.
    tb = tb.astype({"POPULATION": str})

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(keys=["country", "reportyear"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
