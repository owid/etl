"""Load near surface temperature dataset (northern hemisphere, southern hemisphere, and global) by Met Office Hadley
Centre and create a single table.

"""

import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Get naming conventions.
N = PathFinder(__file__)

# Snapshot and Meadow dataset versions.
SNAPSHOT_VERSION = "2023-01-02"
MEADOW_VERSION = SNAPSHOT_VERSION
MEADOW_SHORT_NAME = "near_surface_temperature"

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
    #
    # Load data.
    #
    # Load snapshots.
    snapshots = {
        region: Snapshot(f"met_office_hadley_centre/{SNAPSHOT_VERSION}/{file_name}")
        for region, file_name in REGION_FILE_NAMES.items()
    }
    df = pd.concat(
        [pd.read_csv(snapshot.path).assign(**{"region": region}) for region, snapshot in snapshots.items()],
        ignore_index=True,
    )

    #
    # Prepare data.
    #
    # Select and rename required columns.
    df = df.rename(columns=COLUMNS, errors="raise")[COLUMNS.values()]

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["region", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create new meadow dataset, using metadata from one of the snapshots.
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snapshots["Global"].metadata))
    ds.metadata.version = MEADOW_VERSION
    ds.metadata.short_name = MEADOW_SHORT_NAME

    # Create new table with metadata and underscore all columns.
    tb = Table(df, short_name=MEADOW_SHORT_NAME, underscore=True)

    # Add table to new meadow dataset and save dataset.
    ds.add(tb)
    ds.save()
