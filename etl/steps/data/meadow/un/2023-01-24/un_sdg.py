"""Load a snapshot and create a meadow dataset."""

import re

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
    log.info("un_sdg.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("un_sdg.feather")

    # Load data from snapshot.
    df = pd.read_feather(snap.path)

    log.info("un_sdg.load_and_clean")
    df = load_and_clean(df)
    log.info("Size of dataframe", rows=df.shape[0], colums=df.shape[1])
    df = df.reset_index(drop=True).drop(columns="index")
    tb = Table(df, short_name=paths.short_name, underscore=True)
    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
    # ds.metadata.version = paths.version
    # ds.add(tb)
    # ds.update_metadata(paths.metadata_path)
    ds.save()
    log.info("un_sdg.end")


def load_and_clean(original_df: pd.DataFrame) -> pd.DataFrame:
    # Load and clean the data
    log.info("un_sdg.reading_in_original_data")
    original_df = original_df.copy(deep=False)

    # removing values that aren't numeric e.g. Null and N values
    original_df.dropna(subset=["Value"], inplace=True)
    original_df.dropna(subset=["TimePeriod"], how="all", inplace=True)
    original_df = original_df.loc[pd.to_numeric(original_df["Value"], errors="coerce").notnull()]
    original_df.rename(columns={"GeoAreaName": "Country", "TimePeriod": "Year"}, inplace=True)
    original_df = original_df.rename(columns=lambda k: re.sub(r"[\[\]]", "", k))  # type: ignore
    return original_df
