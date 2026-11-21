"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("us_state_population.csv")
    tb = snap.read()

    #
    # Process data.
    #
    # Population estimates are as of July 1; keep just the year.
    tb["year"] = pr.to_datetime(tb["observation_date"]).dt.year
    tb = tb.drop(columns=["observation_date"])
    # Convert from thousands of people to people.
    tb["value"] *= 1000

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb.format(["state", "year"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
