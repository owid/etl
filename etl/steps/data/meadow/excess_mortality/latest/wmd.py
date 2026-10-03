"""Load a snapshot and create a meadow dataset.

In this step we perform sanity checks on the expected input fields and the values that they take."""

from owid.catalog import Dataset
from structlog import get_logger

from etl.data_helpers.misc import check_known_columns
from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Expected columns in input data
COLUMNS_EXPECTED = [
    "iso3c",
    "country_name",
    "year",
    "time",
    "time_unit",
    "deaths",
]


def run(dest_dir: str) -> None:
    log.info("wmd.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("wmd.csv")

    # Load data from snapshot.
    tb = snap.read_csv(underscore=True)
    check_known_columns(tb, COLUMNS_EXPECTED)
    # Set index
    tb = tb.set_index(["iso3c", "year", "time"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))

    # Ensure the version of the new dataset corresponds to the version of current step.
    ds_meadow.metadata.version = paths.version

    # Add the new table to the meadow dataset.
    ds_meadow.add(tb)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("wmd.end")
