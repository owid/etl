"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_snapshot_metadata

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select and how to name them.
COLUMNS = {
    # Additional column.
    "region": "region",
    # Original column names and new names.
    "Time": "year",
    "Anomaly (deg C)": "temperature_anomaly",
    "Lower confidence limit (2.5%)": "lower_limit",
    "Upper confidence limit (97.5%)": "upper_limit",
}

# Names of snapshot files.
REGION_FILE_NAMES = {
    "Global": "near_surface_temperature_global.csv",
    "Northern hemisphere": "near_surface_temperature_northern_hemisphere.csv",
    "Southern hemisphere": "near_surface_temperature_southern_hemisphere.csv",
}


def run(dest_dir: str) -> None:
    log.info("near_surface_temperature.start")

    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snapshots = {region: paths.load_dependency(file_name) for region, file_name in REGION_FILE_NAMES.items()}

    # Load data from snapshots.
    df = pd.concat(
        [pd.read_csv(snapshot.path).assign(**{"region": region}) for region, snapshot in snapshots.items()],
        ignore_index=True,
    )

    #
    # Process data.
    #
    # Select and rename required columns.
    df = df.rename(columns=COLUMNS, errors="raise")[COLUMNS.values()]

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["region", "year"], verify_integrity=True).sort_index()

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snapshots["Global"].metadata))

    # Ensure the short name and version of the new dataset correspond to the ones of the current step.
    ds_meadow.metadata.short_name = paths.short_name
    ds_meadow.metadata.version = paths.version

    # Add the new table to the meadow dataset.
    ds_meadow.add(tb)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("near_surface_temperature.end")
