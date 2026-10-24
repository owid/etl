"""Load the climate-related aid snapshots (by donor and by recipient) and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    snapshot_names = ["climate_related_aid_given.csv", "climate_related_aid_received.csv"]
    tables = []
    for snapshot_name in snapshot_names:
        # Retrieve snapshot.
        snap = paths.load_snapshot(snapshot_name)

        # Load data from snapshot.
        tb = snap.read()

        #
        # Process data.
        #
        # Keep entity names (harmonized in garden) and drop the ISO/OWID codes.
        tb = tb.drop(columns=["Code"]).rename(columns={"Entity": "country", "Year": "year"}, errors="raise")
        tb["country"] = tb["country"].astype("category")

        # Improve table format.
        tb = tb.format(["country", "year"])

        # Append current table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)

    # Save meadow dataset.
    ds_meadow.save()
