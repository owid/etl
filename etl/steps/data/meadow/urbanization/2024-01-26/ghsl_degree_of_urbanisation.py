"""Load a snapshot and create a meadow dataset."""

import zipfile

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ghsl_degree_of_urbanisation.zip")

    # Load data from snapshot.
    zf = zipfile.ZipFile(snap.path)
    # Load the Excel file
    excel_file = pr.ExcelFile(zf.open("GHS_DUC_GLOBE_R2023A_V1_0.xlsx"))
    #
    # Process data.
    #
    # List all sheet names in the Excel file
    sheet_names = excel_file.sheet_names

    # Filter sheet names that start with 'country'
    country_sheet_names = [sheet for sheet in sheet_names if sheet.lower().startswith("country")]

    tbs = []
    for sheet_name in country_sheet_names:
        # Read the first two rows of the sheet to combine the column headers with the values in the first row
        first_row = pr.read_excel(zf.open("GHS_DUC_GLOBE_R2023A_V1_0.xlsx"), sheet_name=sheet_name, nrows=1)
        # Clean up the column names by replacing 'Unnamed: x' with the previous value that is not unnamed
        cleaned_column_names = []
        for col in first_row.columns:
            if "Unnamed" in col:
                # Replace with the previous value that is not unnamed
                cleaned_column_names.append(cleaned_column_names[-1])
            else:
                cleaned_column_names.append(col)

        # Combine the column names with the first row values to create new column headers
        new_column_headers = [
            f"{header}-{value}" if pd.notna(value) else header
            for header, value in zip(cleaned_column_names, first_row.iloc[0])
        ]

        # Read the sheet (but skipe the first two rows)
        tb = pr.read_excel(
            zf.open("GHS_DUC_GLOBE_R2023A_V1_0.xlsx"),
            sheet_name=sheet_name,
            skiprows=1,
            metadata=snap.to_table_metadata(),
        )

        # Assign the new column headers and read the rest of the data, skipping the first two rows
        tb.columns = new_column_headers

        # Drop the unnecessary columns from the dataframe
        tb = tb.drop(["GADM code", "GADM ISO", "Selected GADM Level", "GADM level type"], axis=1)
        # Remove rows where all values in the row are NaN
        tb = tb.dropna(how="all")
        # Melt the dataframe to have 'GADM NAME' as the identifier, and create 'indicator' and 'value' columns
        tb_melted = tb.melt(id_vars=["GADM NAME"], var_name="indicator", value_name="value")

        # Extract year from the sheet name
        year = int(sheet_name.split()[-1])
        tb_melted["year"] = year
        tb_melted = tb_melted.rename(columns={"GADM NAME": "country"})
        tbs.append(tb_melted)

    # Concatenate all the DataFrames in the list
    final_tb = pr.concat(tbs, axis=0, ignore_index=True)
    final_tb = final_tb.set_index(["country", "year", "indicator"], verify_integrity=True).sort_index()
    final_tb["value"] = pd.to_numeric(final_tb["value"], errors="coerce")

    # Add origins metadata.
    final_tb["value"].metadata.origins = [snap.m.origin]

    #
    # Save outputs.
    #

    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[final_tb], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
