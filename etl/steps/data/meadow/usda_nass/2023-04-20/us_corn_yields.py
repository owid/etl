"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from data, and how to rename them.
COLUMNS = {
    "Year": "year",
    "Value": "corn_yield",
}


def run(dest_dir: str) -> None:
    log.info("us_corn_yields.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("us_corn_yields.csv")

    # Load data from snapshot.
    df = pd.read_csv(snap.path)

    #
    # Process data.
    #
    # Sanity check.
    error = "Data does not have the expected characteristics."
    assert df[
        ["Program", "Period", "Geo Level", "State", "Commodity", "Data Item", "Domain", "Domain Category"]
    ].drop_duplicates().values.tolist() == [
        [
            "SURVEY",
            "YEAR",
            "NATIONAL",
            "US TOTAL",
            "CORN",
            "CORN, GRAIN - YIELD, MEASURED IN BU / ACRE",
            "TOTAL",
            "NOT SPECIFIED",
        ]
    ], error

    # Select and rename required columns, and add a country column.
    df = df[list(COLUMNS)].rename(columns=COLUMNS, errors="raise").assign(**{"country": "United States"})

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("us_corn_yields.end")
