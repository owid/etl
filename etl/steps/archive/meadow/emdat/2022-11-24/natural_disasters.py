"""Load snapshot of EM-DAT natural disasters data and prepare a table with basic metadata.

"""

import warnings

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Snapshot version.
SNAPSHOT_VERSION = "2022-11-24"
# Current Meadow dataset version.
VERSION = SNAPSHOT_VERSION

# Get naming conventions.
N = PathFinder(__file__)

# Columns to extract from raw data, and how to rename them.
COLUMNS = {
    "Country": "country",
    "Year": "year",
    "Disaster Group": "group",
    "Disaster Subgroup": "subgroup",
    "Disaster Type": "type",
    "Disaster Subtype": "subtype",
    "Disaster Subsubtype": "subsubtype",
    "Event Name": "event",
    "Region": "region",
    "Continent": "continent",
    "Total Deaths": "total_dead",
    "No Injured": "injured",
    "No Affected": "affected",
    "No Homeless": "homeless",
    "Total Affected": "total_affected",
    "Reconstruction Costs ('000 US$)": "reconstruction_costs",
    "Insured Damages ('000 US$)": "insured_damages",
    "Total Damages ('000 US$)": "total_damages",
    "Start Year": "start_year",
    "Start Month": "start_month",
    "Start Day": "start_day",
    "End Year": "end_year",
    "End Month": "end_month",
    "End Day": "end_day",
}


def run(dest_dir: str) -> None:
    # Load snapshot.
    snap = Snapshot(f"emdat/{SNAPSHOT_VERSION}/natural_disasters.xlsx")
    with warnings.catch_warnings(record=True):
        df = pd.read_excel(snap.path, sheet_name="emdat data", skiprows=6)

    # Select and rename columns.
    df = df[list(COLUMNS)].rename(columns=COLUMNS)

    # Sanity check.
    error = "Expected only 'Natural' in 'group' column."
    assert set(df["group"]) == set(["Natural"]), error

    # Create a new dataset and reuse snapshot metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_snapshot_metadata(snap.metadata)
    ds.metadata.version = VERSION

    # Create a table with metadata from dataframe.
    table_metadata = TableMeta(
        short_name=snap.metadata.short_name,
        title=snap.metadata.name,
        description=snap.metadata.description,
    )
    tb = Table(df, metadata=table_metadata, underscore=True)

    # Add table to new dataset and save dataset.
    ds.add(tb)
    ds.save()
