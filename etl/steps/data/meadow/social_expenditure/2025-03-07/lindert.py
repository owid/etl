"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("lindert.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Make the table long, with columns "country" and "year".
    tb = tb.melt(id_vars=["country"], var_name="year", value_name="share_gdp")

    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
