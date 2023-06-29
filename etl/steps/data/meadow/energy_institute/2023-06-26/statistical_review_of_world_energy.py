"""Load snapshots of data files of the Statistical Review of World Energy, and create a meadow dataset.

The Energy Institute provides different data files:
* Narrow-format (long-format) ~15MB xlsx file.
* Narrow-format (long-format) ~11MB csv file.
* Panel-format (wide-format) ~4MB xlsx file.
* Panel-format (wide-format) ~3MB csv file.
* Additionally, they provide the main data file, which is a human-readable xlsx file with many sheets.

For some reason, the latter file (which is much harder to parse programmatically) contains some variables
that are not given in the other data files (e.g. data on coal reserves).

Therefore, we extract most of the data from the wide-format csv file, and the required additional variables
from the main excel file.

"""

import re
from typing import cast

# +
import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# -

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def parse_coal_reserves(data: pd.ExcelFile) -> pd.DataFrame:
    sheet_name = "Coal - Reserves"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    df = data.parse(sheet_name, skiprows=1)

    # The year of the data is written in the header of the sheet.
    # Extract it using a regular expression.
    _year = re.findall(r"\d{4}", df.columns[0])
    assert len(_year) == 1, f"Year could not be extracted from the header of the sheet {sheet_name}."
    year = int(_year[0])

    # Re-create the original column names, assuming the zeroth column is for countries.
    df.columns = ["country"] + [
        "Coal reserves - " + " ".join(df[column].iloc[0:2].fillna("").astype(str).tolist()).strip()
        for column in df.columns[1:]
    ]

    # The units should be written in the header of the first column.
    assert df.iloc[1][0] == "Million tonnes", f"Units (or sheet format) may have changed in sheet {sheet_name}"
    # Zeroth column should correspond to countries.
    assert "Total World" in df["country"].tolist()

    # Drop header rows.
    df = df.drop([0, 1, 2]).reset_index(drop=True)

    # Drop empty columns and rows.
    df = df.dropna(axis=1, how="all")
    df = df.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    df = df.dropna(subset=df.drop(columns="country").columns, how="all")

    # Add a column for the year of the data.
    df = df.assign(**{"year": year})

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index().reset_index()

    return df


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_csv = cast(Snapshot, paths.load_dependency("statistical_review_of_world_energy.csv"))
    snap_excel = cast(Snapshot, paths.load_dependency("statistical_review_of_world_energy.xlsx"))
    # Most data comes from the wide-format csv file, and some additional variables from the excel file.
    tb_csv = pr.read_csv(snap_csv.path, metadata=snap.to_table_metadata(), underscore=True)
    additional_data = pd.ExcelFile(snap_excel.path)

    #
    # Process data.
    #
    # Parse coal reserves sheet.
    coal_reserves = parse_coal_reserves(data=additional_data)

    # TODO: Combine all data and decide how to handle metadata.

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
