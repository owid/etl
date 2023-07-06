"""Load a snapshot and create a meadow dataset."""

from pathlib import Path
from typing import cast

import owid.catalog.processing as pr
from owid.catalog import Table, TableMeta

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def extract_capacity_from_sheet(excel_object: pr.ExcelFile, sheet_name: str, metadata: TableMeta) -> Table:
    # The name of the energy source is given in the very first cell.
    # To get that, I load the file, skipping all rows from the bottom.
    # The first column is the content of the first cell.
    technology = excel_object.parse(sheet_name=sheet_name, skipfooter=10000).columns[0]  # type: ignore

    # Check that the extracted name of the technology is long enough, and does not contain digits.
    assert len(technology) >= 6
    assert all([not character.isdigit() for character in technology])

    # The format of this dataset is inconvenient and requires some adjustment that may not work on the next update.
    tb = excel_object.parse(sheet_name=sheet_name, skiprows=4, metadata=metadata)  # type: ignore

    # There are two tables put together: One for capacity and one for production.
    # Keep only columns for capacity that start with a year.
    columns_to_keep = [tb.columns[0]]
    for column in tb.columns[1:]:
        if str(column).startswith("PROD"):
            # Stop considering further columns, as they correspond to production data.
            break
        if str(column).isnumeric() and (len(str(column)) == 4):
            columns_to_keep.append(column)

    tb = tb[columns_to_keep].rename(columns={"CAP (MW)": "country"}, errors="raise")

    # Remove empty rows.
    # Note: There may still be rows for which certain years do not have data; they will be removed later.
    tb = tb.dropna(subset="country").reset_index(drop=True)

    # Restructure dataframe.
    tb = tb.melt(id_vars="country", var_name="year", value_name="capacity")

    # Drop any row for which there is no data.
    tb = tb.dropna(subset=["country", "year", "capacity"], how="any").reset_index(drop=True)

    # Add technology (referring to the name of the energy source) as a new column.
    tb["technology"] = technology

    return tb


def extract_capacity_from_all_sheets(data_file: Path, metadata: TableMeta) -> Table:
    # Select sheets that contain data (their names are numbers).
    excel_object = pr.ExcelFile(data_file)
    sheet_names = [sheet for sheet in excel_object.sheet_names if sheet.strip().isdigit()]

    # Extract data sheet by sheet.
    all_data = Table(metadata=metadata)
    for sheet_name in sheet_names:
        data = extract_capacity_from_sheet(excel_object=excel_object, sheet_name=sheet_name, metadata=metadata)
        all_data = pr.concat([all_data, data], ignore_index=True)

    # Set a short name to the new table.
    all_data.metadata.short_name = paths.short_name

    # Set an appropriate index and sort conveniently.
    # Note: In a previous version, some rows were repeated (with identical values of capacity).
    # If that happens again, the following command will fail (because there are duplicated indexes).
    # Visually inspect duplicates and drop them.
    # all_data[all_data.duplicated(subset=["country", "year", "technology"], keep=False)].sort_values(["country", "year", "technology"])
    # all_data = all_data.drop_duplicates(subset=["country", "year", "technology"], keep="first")
    all_data = (
        all_data.set_index(["country", "year", "technology"], verify_integrity=True).sort_index().sort_index(axis=1)
    )

    # Ensure all values of capacity are numeric.
    # If this fails, the file has not been properly parsed (there are columns with values "e" or "u").
    all_data["capacity"] = all_data["capacity"].astype(float)

    return all_data


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("renewable_electricity_capacity_and_generation.xlsm"))

    # Load and prepare data from snapshot.
    tb = extract_capacity_from_all_sheets(data_file=snap.path, metadata=snap.to_table_metadata())

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
