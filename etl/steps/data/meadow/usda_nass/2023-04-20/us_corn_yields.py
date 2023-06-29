"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr
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
    tb = pr.read_csv(snap.path, metadata=snap.to_table_metadata())

    #
    # Process data.
    #
    # Sanity check.
    error = "Data does not have the expected characteristics."
    assert tb[
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
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise").assign(**{"country": "United States"}).underscore()

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=tb.metadata.dataset)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("us_corn_yields.end")
