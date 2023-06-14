"""Load a snapshot and create a meadow dataset."""

import warnings
from typing import cast

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def combine_data_from_relevant_sheets(data: pd.ExcelFile) -> pd.DataFrame:
    # Initialize an empty dataframe that will keep data for all crops.
    df = pd.DataFrame({"country": [], "year": []})
    for sheet_name in data.sheet_names:
        if sheet_name == "ConsumableCaloriesChange":
            col_1 = "percentage consumable kcal change across 10 crops"
            col_2 = "percentage consumable kcal change across total food comsumption"
            renaming = {"Unnamed: 0": "country", col_1: "change_across_10_crops", col_2: "change_across_all_food"}
        elif sheet_name in [
            "BARLEY",
            "CASSAVA",
            "MAIZE",
            "OILPALM",
            "RAPESEED",
            "RICE",
            "SORGHUM",
            "SOYBEAN",
            "SUGARCANE",
            "WHEAT",
        ]:
            col_1 = "change_in_yield (tons/ha/year)"
            col_2 = "percent change wrt current"
            renaming = {
                "Unnamed: 0": "country",
                col_1: f"{sheet_name.lower()}_change_in_yield",
                col_2: f"{sheet_name.lower()}_change_with_respect_to_current",
            }
        else:
            continue

        # Parse sheet and drop rows for which there is no data.
        df_i = data.parse(sheet_name).dropna(subset=[col_1, col_2], how="all")

        # Rename columns conveniently.
        df_i = df_i.rename(columns=renaming, errors="raise")

        # Add a year column.
        # Note: It's unclear what year to assign, since some of the metrics are computed over a range of years.
        df_i["year"] = 2013

        # Merge data from this sheet to the complete dataframe.
        df = pd.merge(df, df_i, on=["country", "year"], how="outer")

    return df


def run(dest_dir: str) -> None:
    log.info("ray_et_al_2019.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("ray_et_al_2019.xlsx"))

    # Load data from snapshot.
    data = pd.ExcelFile(snap.path)

    # Parse and combine data from all sheets.
    df = combine_data_from_relevant_sheets(data=data)

    # Set an appropriate index and sort conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("ray_et_al_2019.end")
