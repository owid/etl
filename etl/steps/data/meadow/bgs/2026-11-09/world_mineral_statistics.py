"""Load a snapshot and create a meadow dataset."""

import json
import zipfile

import pandas as pd
from owid.catalog.processing import read_from_df
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def process_geojson_data(geojson_data: dict) -> pd.DataFrame:
    """
    Process GeoJSON FeatureCollection from the new BGS OGC API.

    The new API returns structured JSON instead of HTML tables, making this
    much simpler than the previous implementation.

    Args:
        geojson_data: Dictionary containing GeoJSON FeatureCollection

    Returns:
        DataFrame with cleaned and standardized mineral statistics data
    """
    # Extract features from the GeoJSON
    features = geojson_data.get("features", [])
    log.info(f"Processing {len(features):,} records from GeoJSON data")

    # Convert features to a list of dictionaries (just the properties)
    records = [feature.get("properties", {}) for feature in features]

    # Create DataFrame from records
    df = pd.DataFrame(records)

    # Rename columns to match the expected format
    column_mapping = {
        "bgs_statistic_type_trans": "category",
        "country_trans": "country",
        "bgs_commodity_trans": "commodity",
        "bgs_sub_commodity_trans": "sub_commodity",
        "year": "year",
        "quantity": "value",
        "units": "unit",
        "concat_figure_notes_text": "note",
        "concat_table_notes_text": "general_notes",
    }

    # Select only the columns we need and rename them
    df = df[list(column_mapping.keys())].rename(columns=column_mapping)

    # Convert year from ISO date format to just the year
    # The API returns dates like "2006-01-01"
    df["year"] = pd.to_datetime(df["year"]).dt.year

    # Handle missing sub_commodity (use "Unknown" as in the old code)
    df["sub_commodity"] = df["sub_commodity"].fillna("Unknown")

    # Convert note and general_notes to strings (they may be None)
    df["note"] = df["note"].fillna("").astype(str)
    df["general_notes"] = df["general_notes"].fillna("").astype(str)

    # Ensure proper data types
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Remove any rows with missing values
    df = df.dropna(subset=["value"]).reset_index(drop=True)

    log.info(f"Processed {len(df):,} valid records")

    return df


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("world_mineral_statistics.zip")

    # Read compressed folder from snapshot.
    with zipfile.ZipFile(snap.path, "r") as _zipfile:
        # Read the JSON data inside the compressed folder.
        with _zipfile.open(f"{snap.metadata.short_name}.json") as json_file:
            json_data = json_file.read()

    #
    # Process data.
    #
    # Convert the JSON string into a dictionary
    data = json.loads(json_data)

    # Process GeoJSON data to create a dataframe.
    df_all = process_geojson_data(geojson_data=data)

    # Create a table with the snapshot data and metadata.
    tb = read_from_df(data=df_all, metadata=snap.to_table_metadata(), origin=snap.metadata.origin, underscore=True)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["category", "country", "commodity", "sub_commodity", "year"], verify_integrity=False)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
