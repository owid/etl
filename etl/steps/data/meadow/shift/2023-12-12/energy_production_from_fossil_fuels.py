"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("energy_production_from_fossil_fuels.csv")

    # Load data from snapshot.
    tb = snap.read(underscore=True)

    #
    # Process data.
    #
    # Set an appropriate index and sort conveniently.
    tb = tb.format(keys=["country", "year"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
