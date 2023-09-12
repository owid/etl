"""Extract capacity data from IRENA's Renewable Electricity Capacity and Generation 2022 dataset.

"""

from typing import cast

import pandas as pd
from owid import catalog
from owid.walden import catalog as WaldenCatalog

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_walden_metadata

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def extract_capacity_from_sheet(excel_object: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    # The name of the energy source is given in the very first cell.
    # To get that, I load the file, skipping all rows from the bottom.
    # The first column is the content of the first cell.
    technology = excel_object.parse(sheet_name, skipfooter=10000).columns[0]  # type: ignore

    # The format of this dataset is inconvenient and requires some adjustment that may not work on the next update.
    df = excel_object.parse(sheet_name, skiprows=4)  # type: ignore

    # There are two tables put together: One for capacity and one for production.
    # Keep only columns for capacity.
    columns_to_keep = [df.columns[0]]
    for column in df.columns[1:]:
        if str(column).startswith("PROD"):
            break
        if not str(column).startswith("Unnamed"):
            columns_to_keep.append(column)
    df = df[columns_to_keep].rename(columns={"CAP (MW)": "country"})

    # Remove empty rows.
    df = df.dropna(subset="country").reset_index(drop=True)

    # Restructure dataframe.
    df = df.melt(id_vars="country", var_name="year", value_name="capacity")

    # Add technology (referring to the name of the energy source) as a new column.
    df["technology"] = technology

    return cast(pd.DataFrame, df)


def extract_capacity_from_all_sheets(data_file: str) -> pd.DataFrame:
    # Select sheets that contain data (their names are numbers).
    excel_object = pd.ExcelFile(data_file)
    sheet_names = [sheet for sheet in excel_object.sheet_names if sheet.strip().isdigit()]

    # Extract data sheet by sheet.
    all_data = pd.DataFrame()
    for sheet_name in sheet_names:
        data = extract_capacity_from_sheet(excel_object=excel_object, sheet_name=sheet_name)
        all_data = pd.concat([all_data, data], ignore_index=True)

    # Some rows are repeated (it seems that with identical values, at least for the case found, Uruguay on sheet 18).
    # Therefore, drop duplicates.
    # Set an appropriate index and sort conveniently.
    all_data = (
        all_data.drop_duplicates(subset=["country", "year", "technology"], keep="first")
        .set_index(["technology", "country", "year"], verify_integrity=True)
        .sort_index()
        .sort_index(axis=1)
    )

    return all_data


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Retrieve raw data from Walden.
    ds_walden: WaldenCatalog.Dataset = paths.load_dependency("renewable_electricity_capacity_and_generation")
    local_file = ds_walden.ensure_downloaded()

    #
    # Process data.
    #
    # Extract capacity data.
    df = extract_capacity_from_all_sheets(data_file=local_file)

    #
    # Save outputs.
    #
    # Create a new Meadow dataset and reuse walden metadata.
    ds = catalog.Dataset.create_empty(dest_dir, metadata=convert_walden_metadata(ds_walden))
    ds.metadata.version = paths.version

    # Create a new table with metadata from Walden.
    table_metadata = catalog.TableMeta(
        short_name=ds_walden.short_name,
        title=ds_walden.name,
        description=ds_walden.description,
    )
    tb = catalog.Table(df, metadata=table_metadata, underscore=True)

    # Add table to the dataset and save dataset.
    ds.add(tb)
    ds.save()
