"""Load a snapshot and create a meadow dataset."""

import tempfile
import zipfile
from pathlib import Path

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

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
    "open_burn_msw_pct_gen",
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
        regional_file = list(temp_path.glob("**/SD_04_Cottom_et_al_V1.1.0-G-1223_SPOT_MFA_Outputs_Global_Regional_Income.xlsx"))

        if not national_file or not regional_file:
            raise FileNotFoundError(
                "Could not find the expected Excel files in the zip archive. "
                f"Found files: {list(temp_path.rglob('*.xlsx'))}"
            )

        # Load data from Excel files (data is in 03_Outputs sheet)
        df_national = pd.read_excel(national_file[0], sheet_name="03_Outputs")
        df_regional = pd.read_excel(regional_file[0], sheet_name="03_Outputs")

    #
    # Process data.
    #
    # Process national data
    tb_national = process_data(df_national, is_national=True)
    tb_national = tb_national.format(["country", "year"])

    # Process regional data
    tb_regional = process_data(df_regional, is_national=False)
    tb_regional = tb_regional.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=[tb_national, tb_regional],
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
    # Filter for the variables we want
    df = df[df["variable"].isin(VARIABLES)].copy()

    # Get year columns (assuming format like Y2000, Y2001, etc.)
    year_columns = [col for col in df.columns if col.startswith("Y")]

    # Melt the dataframe to long format
    id_vars = ["ISO3", "country", "variable"] if is_national else ["country", "variable"]
    tb = df.melt(
        id_vars=id_vars,
        value_vars=year_columns,
        var_name="year",
        value_name="value",
    )

    # Clean year column (remove 'Y' prefix)
    tb["year"] = tb["year"].str.replace("Y", "").astype(int)

    # Pivot to wide format with variables as columns
    if is_national:
        tb = tb.pivot(index=["country", "year"], columns="variable", values="value").reset_index()
    else:
        tb = tb.pivot(index=["country", "year"], columns="variable", values="value").reset_index()

    # Reset column names
    tb.columns.name = None

    # Create table
    tb = Table(tb, short_name="cottom_plastic_waste_national" if is_national else "cottom_plastic_waste_regional")

    return tb
