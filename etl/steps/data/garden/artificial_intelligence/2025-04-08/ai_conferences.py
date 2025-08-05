"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ai_conferences.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #

    # Convert number of attendees to absolute numbers.
    tb["Number of attendees (in thousands)"] = tb["Number of attendees (in thousands)"] * 1000
    tb = tb.rename(columns={"Number of attendees (in thousands)": "attendees", "Label": "conference"})
    tb = tb.format(["conference", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
