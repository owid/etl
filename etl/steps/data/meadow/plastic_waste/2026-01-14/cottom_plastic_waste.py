"""Load a snapshot and create a meadow dataset."""

import tempfile
import zipfile
from pathlib import Path

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
    # Extract Excel files from zip
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(snap.path, "r") as zip_ref:
            zip_ref.extractall(temp_dir)

        # Find the Excel files within the extracted structure
        temp_path = Path(temp_dir)
        national_file = list(temp_path.glob("**/SD_03_Cottom_et_al_V1.1.0-G-1223_SPOT_MFA_Outputs_National.xlsx"))
        regional_file = list(
            temp_path.glob("**/SD_04_Cottom_et_al_V1.1.0-G-1223_SPOT_MFA_Outputs_Global_Regional_Income.xlsx")
        )

        if not national_file or not regional_file:
            raise FileNotFoundError(
                "Could not find the expected Excel files in the zip archive. "
                f"Found files: {list(temp_path.rglob('*.xlsx'))}"
            )
        # Load data from Excel files (data is in 03_Outputs sheet)
        # Header is in row 1 (0-indexed)
        df_national = pd.read_excel(national_file[0], sheet_name="03_Outputs", header=1)
        df_regional = pd.read_excel(regional_file[0], sheet_name="03_Outputs", header=1)

    #
    # Process data.
    #
    # Process national data
    df_national = process_data(df_national, is_national=True)

    # Process regional data
    df_regional = process_data(df_regional, is_national=False)

    df = pd.concat([df_national, df_regional], ignore_index=True)
    tb = Table(df, short_name="cottom_plastic_waste")

    # Add origins to all columns
    for col in tb.columns:
        tb[col].metadata.origins = [snap.metadata.origin]

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


def process_data(df: pd.DataFrame, is_national: bool) -> Table:
    """
    Process the plastic waste data from Excel files.

    Parameters
    ----------
    df : pd.DataFrame
        Raw data from Excel file
    is_national : bool
        True if processing national data, False for regional data

    Returns
    -------
    Table
        Processed table with standardized format
    """
    # Filter for only "Mean" statistic
    df = df[df["Statistic"] == "Mean"].copy()

    # Get all variable columns (these are already the column names in the Excel)
    # Keep only the ones we defined in VARIABLES list
    available_vars = [col for col in df.columns if col in VARIABLES]

    if is_national:
        # National data has Country and ISO3 columns
        id_cols = ["Country"]
        cols_to_keep = id_cols + available_vars
        df = df[cols_to_keep].copy()
        df = df.rename(columns={"Country": "country"})
    else:
        # Regional data has Aggregation_lv2 as country
        id_cols = ["Aggregation_lv2"]
        cols_to_keep = id_cols + available_vars
        df = df[cols_to_keep].copy()
        df = df.rename(columns={"Aggregation_lv2": "country"})
        # Remove rows with NaN country
        df = df.dropna(subset=["country"])
        # Exclude countries that are in national data
        df = df[~df["country"].isin(["USA", "Canada", "China", "India", "Micronesia"])]

    # The data appears to be for a single year (2020 based on Population_2020)
    # Add year column
    df["year"] = 2020

    return df
