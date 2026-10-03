"""Load a snapshot and create a meadow dataset.

In this step we perform sanity checks on the expected input fields and the values that they take."""

from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Column names
COLUMN_NAMES = [
    "country",
    "year",
    "time",
    "deaths",
]
COLUMN_NAMES_AGES = [
    "country",
    "year",
    "age",
    "sex",
    "time",
    "deaths",
]


def run(dest_dir: str) -> None:
    log.info("xm_karlinsky_kobak.start")

    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_all = paths.load_snapshot("xm_karlinsky_kobak.csv")
    snap_ages = paths.load_snapshot("xm_karlinsky_kobak_ages.csv")

    # Load data from snapshot.
    tb_all = load_table(snap_all, column_names=COLUMN_NAMES)
    tb_ages = load_table(snap_ages, column_names=COLUMN_NAMES_AGES)
    # Both files are part of the same Karlinsky and Kobak data product. Use a single origin object
    # so downstream concatenation does not render duplicate chart sources for the same source.
    origin = tb_all["deaths"].metadata.origins
    for col in tb_ages.columns:
        tb_ages[col].metadata.origins = origin

    #
    # Process data.
    #
    # Ensure all columns are snake-case.
    tb_all.metadata.short_name = paths.short_name
    tb_all = tb_all.underscore()
    tb_ages.metadata.short_name = f"{paths.short_name}_by_age"
    tb_ages = tb_ages.underscore()
    # Set index
    tb_all = tb_all.set_index(["country", "year", "time"], verify_integrity=True)
    tb_ages = tb_ages.set_index(["country", "year", "age", "sex", "time"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap_all.metadata))

    # Ensure the version of the new dataset corresponds to the version of current step.
    ds_meadow.metadata.version = paths.version

    # Add the new table to the meadow dataset.
    ds_meadow.add(tb_all)
    ds_meadow.add(tb_ages)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("xm_karlinsky_kobak.end")


def load_table(snap: Snapshot, column_names: list[str]) -> Table:
    """Load the data from the latest version of the dataset."""
    tb = snap.read_csv(names=column_names, encoding="latin-1")
    # Check columns
    assert tb.reset_index().shape[1] == len(column_names) + 1, (
        "Check columns in source! There seems to be more (or less) columns."
    )
    return tb
