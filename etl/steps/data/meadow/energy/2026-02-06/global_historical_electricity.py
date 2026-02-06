"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("global_historical_electricity.zip")

    # Load data from snapshot.
    tb = snap.read(filename="1-s2.0-S036054422300169X-mmc2.xlsx")

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
from owid.catalog.processing import read
tb = read("/Users/prosado/Downloads/pv-cost-new.csv")
tb
