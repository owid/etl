"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define the variables we want to extract
VARIABLES = [
    "wg",
    "wg_per_cap",
    "pwg",
    "pwg_per_cap",
    "plas_debris_em",
    "plas_burn_em",
    "plas_em",
    "plas_debris_em_per_cap",
    "plas_burn_em_per_cap",
    "plas_em_per_cap",
    "plas_litter_em",
    "plas_uncol_em",
    "plas_collection_em",
    "plas_disp_em",
    "plas_recy_em",
    "col_cov_pct_gen_exc_lit",
    "col_del_pct_gen",
    "litter_em_pct_gen",
    "uncol_em_pct_gen",
    "collection_em_pct_gen",
    "cont_disp_pct_disp",
    "uncont_disp_pct_disp",
    "man_cont_pct_gen",
    "open_burn_MSW_pct_gen",
    "people_no_col",
    "people_no_col_pct_pop",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("cottom_plastic_waste.zip")

    # Load data from Excel files within the zip archive
    # Data is in 03_Outputs sheet, with header in row 1 (0-indexed)
    with snap.open_archive():
        tb_national = snap.read_from_archive(
            "files of interest/SD_03_Cottom_et_al_V1.1.0-G-1223_SPOT_MFA_Outputs_National.xlsx",
            sheet_name="03_Outputs",
            header=1,
        )
        tb_regional = snap.read_from_archive(
            "files of interest/SD_04_Cottom_et_al_V1.1.0-G-1223_SPOT_MFA_Outputs_Global_Regional_Income.xlsx",
            sheet_name="03_Outputs",
            header=1,
        )

    #
    # Process data.
    #
    # Process national data
    tb_national = process_data(tb_national, is_national=True)

    # Process regional data
    tb_regional = process_data(tb_regional, is_national=False)

    tb = pr.concat([tb_national, tb_regional], ignore_index=True)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def process_data(tb: Table, is_national: bool) -> Table:
    """
    Process the plastic waste data from Excel files.

    Parameters
    ----------
    tb : Table
        Raw data from Excel file
    is_national : bool
        True if processing national data, False for regional data

    Returns
    -------
    Table
        Processed table with standardized format
    """
    # Filter for only "Mean" statistic
    tb = tb[tb["Statistic"] == "Mean"].copy()

    # Get all variable columns (these are already the column names in the Excel)
    # Keep only the ones we defined in VARIABLES list
    available_vars = [col for col in tb.columns if col in VARIABLES]

    if is_national:
        # National data has Country and ISO3 columns
        id_cols = ["Country"]
        cols_to_keep = id_cols + available_vars
        tb = tb[cols_to_keep].copy()
        tb = tb.rename(columns={"Country": "country"})
    else:
        # Regional data - filter by Aggregation_lv1
        # Keep Aggregation_lv1 temporarily for filtering
        id_cols = ["Aggregation_lv1", "Aggregation_lv2"]
        cols_to_keep = id_cols + available_vars
        tb = tb[cols_to_keep].copy()

        # Filter by Aggregation_lv1
        tb = tb[tb["Aggregation_lv1"].isin(["World", "Income category", "UN Region"])].copy()

        # Now drop Aggregation_lv1 and rename Aggregation_lv2
        tb = tb.drop(columns=["Aggregation_lv1"])

        tb = tb.rename(columns={"Aggregation_lv2": "country"})
        # Remove rows with NaN country
        tb = tb.dropna(subset=["country"])

    # The data appears to be for a single year (2020 based on Population_2020)
    # Add year column
    tb["year"] = 2020

    return tb
