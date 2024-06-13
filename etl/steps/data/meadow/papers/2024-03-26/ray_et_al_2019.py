"""Load a snapshot and create a meadow dataset."""

import warnings

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def combine_data_from_relevant_sheets(data: pd.ExcelFile) -> Table:
    # Initialize an empty table that will keep data for all crops.
    tb = Table({"country": [], "year": []})
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
        tb_i = data.parse(sheet_name).dropna(subset=[col_1, col_2], how="all")

        # Rename columns conveniently.
        tb_i = tb_i.rename(columns=renaming, errors="raise")

        # Add a year column.
        # Note: It's unclear what year to assign, since some of the metrics are computed over a range of years.
        tb_i["year"] = 2013

        # Merge data from this sheet to the complete table.
        tb = tb.merge(tb_i, on=["country", "year"], how="outer", short_name=paths.short_name)

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its data.
    snap = paths.load_snapshot("ray_et_al_2019.xlsx")
    data = snap.ExcelFile()

    #
    # Process data.
    #
    # Parse and combine data from all sheets.
    tb = combine_data_from_relevant_sheets(data=data)

    # Set an appropriate index and sort conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
