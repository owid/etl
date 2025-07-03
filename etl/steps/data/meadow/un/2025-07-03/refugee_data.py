"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    snap = paths.load_snapshot("refugee_data.zip")

    # Load data from snapshot.
    tb = snap.read_in_archive("persons_of_concern.csv", na_values=["-"])

    # drop duplicate country columns
    tb = tb.drop(columns=["Country of Origin ISO", "Country of Asylum ISO"])

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country_of_origin", "country_of_asylum", "year"])

    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
