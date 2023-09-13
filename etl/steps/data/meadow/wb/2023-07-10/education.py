import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Create a PathFinder instance for the current file
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """
    Main function to load, process and save all World Bank Education datasets.

    """
    snap: Snapshot = paths.load_dependency("education.csv")

    # Load data from snapshot
    df = pd.read_csv(snap.path)
    df.rename(columns={"Series": "indicator_name", "wb_seriescode": "indicator_code"}, inplace=True)

    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb = tb.set_index(["country", "year", "indicator_name"], verify_integrity=True)

    # Use metadata from the first snapshot, then edit the descriptions in the garden step
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save the dataset
    ds_meadow.save()
