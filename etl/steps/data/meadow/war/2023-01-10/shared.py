from typing import Dict, Optional

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_snapshot_metadata

log = get_logger()
# Default column renaming
# Most of the snapshot datasets have the same column names. For these cases, we will use the following default mapping. Optionally, the user can redefine the mapping for a specific dataset.
COLUMN_RENAME = {
    "Bulk ID": "bulk_id",
    "Conflict name": "conflict_name",
    "Conflict participants": "conflict_participants",
    "Type of conflict": "type_of_conflict",
    "Start year": "start_year",
    "End year": "end_year",
    "Continent": "continent",
    "Total explicit deaths": "total_deaths",
    "Explicit Deaths->Explicit Mil->Explicit Direct": "deaths_military_direct",
    "Explicit Deaths->Explicit Mil->Explicit Indirect": "deaths_military_indirect",
    "Explicit Deaths->Explicit Mil->I/D-Not Explicit": "deaths_military_unclear",
    "Explicit Deaths->Mil/Civ-Not Explicit->Explicit Direct": "deaths_unclear_direct",
    "Explicit Deaths->Mil/Civ-Not Explicit->Explicit Indirect": "deaths_unclear_indirect",
    "Explicit Deaths->Mil/Civ-Not Explicit->I/D-Not Explicit": "deaths_unclear_unclear",
    "Explicit Deaths->Explicit Civ->Explicit Direct": "deaths_civilian_direct",
    "Explicit Deaths->Explicit Civ->Explicit Indirect": "deaths_civilian_indirect",
    "Explicit Deaths->Explicit Civ->I/D-Not Explicit": "deaths_civilian_unclear",
    "D/W-Not Explicit->Explicit Mil->Explicit Direct": "casualties_military_direct",
    "D/W-Not Explicit->Explicit Mil->Explicit Indirect": "casualties_military_indirect",
    "D/W-Not Explicit->Explicit Mil->I/D-Not Explicit": "casualties_military_unclear",
    "D/W-Not Explicit->Mil/Civ-Not Explicit->Explicit Direct": "casualties_unclear_direct",
    "D/W-Not Explicit->Mil/Civ-Not Explicit->Explicit Indirect": "casualties_unclear_indirect",
    "D/W-Not Explicit->Mil/Civ-Not Explicit->I/D-Not Explicit": "casualties_unclear_unclear",
    "D/W-Not Explicit->Explicit Civ->Explicit Direct": "casualties_civilian_direct",
    "D/W-Not Explicit->Explicit Civ->Explicit Indirect": "casualties_civilian_indirect",
    "D/W-Not Explicit->Explicit Civ->I/D-Not Explicit": "casualties_civilian_unclear",
    "Source full reference": "source_full_reference",
    "Source page number or URL": "source_page_number_or_url",
    "Notes, inc. key quote": "notes_inc_key_quote",
    "Upload image": "upload_image",
    "Update": "update",
}

COLUMNS_REQUIRED = [
    "bulk_id",
    "conflict_name",
    "conflict_participants",
    "start_year",
    "end_year",
    "continent",
    "total_deaths",
]


def clean_data(df: pd.DataFrame, column_rename: Optional[Dict[str, str]]) -> pd.DataFrame:
    if not column_rename:
        column_rename = COLUMN_RENAME

    # sanity checks
    columns_expected = set(column_rename)
    columns_given = set(df.columns)
    # check if all columns are expected
    columns_unexpected = columns_given.difference(columns_expected)
    if columns_unexpected:
        raise ValueError(f"Unexpected columns found! {columns_unexpected}")
    # sanity check: check if there is any missing column
    columns_missing = set(columns_expected).difference(columns_given)
    if columns_missing:
        raise ValueError(f"There are missing columns! {columns_missing}")

    # rename columns
    df = df.rename(columns=column_rename, errors="raise")

    # table might contain some columns with only NaNs. These should be dropped, as they don't contain any data.
    # Note that *not* all should be dropped, some are required for correct functioning!
    df = df.dropna(how="all", axis=1)
    for col in COLUMNS_REQUIRED:
        if col not in df.columns:
            raise ValueError(f"Missing required column{col}")
    return df


def run_pipeline(dest_dir: str, paths: PathFinder, column_rename: Optional[Dict[str, str]] = None):
    log.info(f"{paths.short_name}.start")
    # read snapshot dataset
    snap = paths.load_dependency(paths.short_name + ".csv")
    df = pd.read_csv(snap.path, header=4)
    # clean and transform data
    df = clean_data(df, column_rename)

    # create new dataset and reuse snapshot metadata
    ds = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))
    ds.metadata.version = paths.version

    # # create table with metadata from dataframe and underscore all columns
    tb = Table(df, short_name=snap.metadata.short_name, underscore=True)

    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()
    log.info(f"{paths.short_name}.start")
