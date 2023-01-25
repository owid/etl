"""Extract capacity data from IRENA's Renewable Electricity Capacity and Generation 2022 dataset.

"""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from shared import CURRENT_DIR

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_walden_metadata

# Details of input dataset.
WALDEN_VERSION = "2022-10-07"
WALDEN_DATASET_NAME = "renewable_electricity_capacity_and_generation"
# Details of output dataset.
VERSION = "2022-10-20"
DATASET_NAME = WALDEN_DATASET_NAME
# Get naming conventions.
N = PathFinder(str(CURRENT_DIR / DATASET_NAME))


def prepare_pv_capacity_data(data_file: str) -> None:
    """Prepare yearly solar photovoltaic capacity data.

    Parameters
    ----------
    data_file : str
        Path to raw data (IRENA's excel file on renewable electricity capacity and generation).

    Returns
    -------
    df : pd.DataFrame
        PV capacity.

    """
    pass


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
    sheet_names = [sheet for sheet in excel_object.sheet_names if sheet.isdigit()]

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
    # Retrieve raw data from Walden.
    walden_ds = WaldenCatalog().find_one(namespace="irena", short_name=WALDEN_DATASET_NAME, version=WALDEN_VERSION)
    local_file = walden_ds.ensure_downloaded()

    # Extract capacity data.
    df = extract_capacity_from_all_sheets(data_file=local_file)

    # Create a new Meadow dataset and reuse walden metadata.
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = VERSION

    # Create a new table with metadata from Walden.
    table_metadata = TableMeta(
        short_name=walden_ds.short_name,
        title=walden_ds.name,
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)

    # Underscore all table columns.
    tb = underscore_table(tb)

    # Add table to the dataset and save dataset.
    ds.add(tb)
    ds.save()
