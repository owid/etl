"""Load a snapshot and create a meadow dataset."""

from typing import List

import numpy as np
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
    log.info("income_groups: start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("income_groups.xlsx")

    # Load data from snapshot.
    dfs = pd.read_excel(snap.path, sheet_name=None)

    #
    # Process data.
    #
    # Load sheet with historical WB classifications
    df = dfs["Country Analytical History"]
    # Minor formatting (only keep cells with relevant data)
    df = extract_data_from_excel(df)
    # Pivot to have years as rows
    # We could leave this for Garden, but catalog.Table won't accept columns starting with a number. We could change these
    # to be 2020 -> _2020, but it feels inefficient
    df = df.melt(id_vars=["country_code", "country"], var_name="year", value_name="classification")
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("income_groups: end")


def extract_data_from_excel(df: pd.DataFrame) -> pd.DataFrame:
    # Sanity shape check
    log.info("income_groups: sanity check on table dimensions")
    assert (num_rows := df.shape[0]) == (
        num_rows_expected := 239
    ), f"Invalid number of rows. Expected was {num_rows_expected}, but found {num_rows}"
    assert (num_cols := df.shape[1]) >= (
        num_cols_expected := 37
    ), f"Invalid number of columns. Expected was >{num_cols_expected}, but found {num_cols}"
    # Check data starts in correct row
    log.info("income_groups: sanity check on data location in excel file")
    assert (
        df.loc[10, "World Bank Analytical Classifications"] == "Afghanistan"
    ), "Row 10, column 'World Bank Analytical Classifications' expected to have value 'Afghanistan'"
    # Get year values (columns)
    log.info("income_groups: extract only relevant data from excel sheet")
    years = _get_years(df)
    df.columns = ["country_code", "country"] + years
    # Drop all but data (based on column 'code' having a value)
    df = df.dropna(subset=["country_code"])
    return df


def _get_years(df: pd.DataFrame) -> List[int]:
    # Get years
    row_years = 4
    years = list(df.loc[row_years])
    # Format sanity check
    assert np.isnan(years[0]), f"The first column in row {row_years} is expected to be NaN. Instead found {years[0]}"
    assert (
        years[1] == "Data for calendar year :"
    ), f"The second column in row {row_years} is expected to be 'Data for calendar year :'. Instead found {years[1]}"
    assert all(
        isinstance(year, int) for year in years[2:]
    ), f"Columns 3 to the end in row {row_years} should be numbers. Check: {years[2:]}"
    return years[2:]
