"""Load a snapshot and create a meadow dataset.

Formatting of the data file. We also map COUNTRY IDs to COUNTRY NAMES. This facilitates the country harmonization proces in Garden."""

from typing import Dict

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("wgm_2018: start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("wgm_2018.xlsx")

    # Load all data from snapshot.
    log.info("wgm_2018: load data")
    dfs = pd.read_excel(snap.path, sheet_name=None)
    # Load data
    df = dfs["Full dataset"]
    df = df.astype({column: str for column in df.columns if column not in ["wgt", "projwt"]})
    # Load metadata
    df_metadata = dfs["Data dictionary"]

    #
    # Process data.
    #
    log.info("wgm_2018: create tables")
    # Data table
    df = add_country_column(df, df_metadata)
    tb = Table(df, short_name=paths.short_name, underscore=True)
    # Metadata table
    tb_meta = Table(df_metadata, short_name=f"{paths.short_name}_metadata", underscore=True)
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    log.info("wgm_2018: create dataset")
    ds_meadow = create_dataset(dest_dir, tables=[tb, tb_meta], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("wgm_2018: end")


def add_country_column(df: pd.DataFrame, df_meta: pd.DataFrame) -> pd.DataFrame:
    log.info("wgm_2018: mapping COUNTRY IDs to COUNTRY NAMEs")
    # Get COUNTRY ID to COUNTRY NAME mapping
    mapping = _build_mapping_country_id_to_country_name(df_meta)
    # Add country column
    df["country"] = df["WP5"].map(mapping)
    assert df["country"].isna().sum() == 0, "Some country IDs were not mapped to a name!"
    return df


def _build_mapping_country_id_to_country_name(df: pd.DataFrame) -> Dict[str, str]:
    print(df.head())
    # Build mapping COUNTRY ID to COUNTRY NAME
    MAPPING_COUNTRY_VALUES = {}
    mapping_str = df.loc[(df["Variable Name"] == "WP5"), "Variable Type & Codes*"].item()
    mapping_list = mapping_str.split(", ")
    for mapping in mapping_list:
        if mapping:
            country_id, country_name = mapping.split("=")
            MAPPING_COUNTRY_VALUES[country_id] = country_name
    return MAPPING_COUNTRY_VALUES
