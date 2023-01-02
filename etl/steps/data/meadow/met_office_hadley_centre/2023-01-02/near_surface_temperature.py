"""Load near surface temperature by Met Office Hadley Centre and prepare a table.

"""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import Names
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()

# Get naming conventions.
N = Names(__file__)

# Snapshot and Meadow dataset versions.
SNAPSHOT_VERSION = "2023-01-02"
MEADOW_VERSION = SNAPSHOT_VERSION

# Columns to select and how to name them.
COLUMNS = {
    "Time": "year",
    "Anomaly (deg C)": "temperature_anomaly",
    "Lower confidence limit (2.5%)": "lower_limit",
    "Upper confidence limit (97.5%)": "upper_limit",
}


def run(dest_dir: str) -> None:
    log.info("near_surface_temperature.start")

    # Load snapshot.
    snap = Snapshot("met_office_hadley_centre/2023-01-02/near_surface_temperature.csv")
    df = pd.read_csv(snap.path)

    # Select and rename required columns.
    df = df.rename(columns=COLUMNS, errors="raise")[COLUMNS.values()]

    # Create new meadow dataset.
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = MEADOW_VERSION

    # Create new table with metadata and underscore all columns.
    tb = Table(df, short_name=snap.metadata.short_name, underscore=True)

    # Add table to new meadow dataset and save dataset.
    ds.add(tb)
    ds.save()

    log.info("near_surface_temperature.end")
