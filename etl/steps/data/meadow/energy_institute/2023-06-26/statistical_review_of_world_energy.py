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

import owid.catalog.processing as pr
from owid.catalog import Table, TableMeta
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def parse_coal_reserves(data: pr.ExcelFile, metadata: TableMeta) -> Table:
    sheet_name = "Coal - Reserves"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=1, metadata=metadata)

    # The year of the data is written in the header of the sheet.
    # Extract it using a regular expression.
    _year = re.findall(r"\d{4}", tb.columns[0])  # type: ignore
    assert len(_year) == 1, f"Year could not be extracted from the header of the sheet {sheet_name}."
    year = int(_year[0])

    # Re-create the original column names, assuming the zeroth column is for countries.
    tb.columns = ["country"] + [
        "Coal reserves - " + " ".join(tb[column].iloc[0:2].fillna("").astype(str).tolist()).strip()
        for column in tb.columns[1:]
    ]

    # The units should be written in the header of the first column.
    assert tb.iloc[1][0] == "Million tonnes", f"Units (or sheet format) may have changed in sheet {sheet_name}"
    # Zeroth column should correspond to countries.
    assert "Total World" in tb["country"].tolist()

    # Drop header rows.
    tb = tb.drop([0, 1, 2]).reset_index(drop=True)

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns="country").columns, how="all")

    # Clean country names (remove spurious spaces and "of which: " in one of the last rows).
    tb["country"] = tb["country"].str.replace("of which:", "").str.strip()

    # For consistency with all other tables, rename some countries to their most common names in the dataset.
    # Their names will be properly harmonized in the garden step.
    # For consistency with all other tables, rename some countries to their most common names in the dataset.
    # Their names will be properly harmonized in the garden step.
    country_mapping = {
        "European Union": "Total EU",
        "Middle East": "Total Middle East",
        "Non-OECD": "Total Non-OECD",
        "OECD": "Total OECD",
        "Turkey": "Turkiye",
    }
    tb["country"] = map_series(tb["country"], country_mapping)

    # Add a column for the year of the data.
    tb = tb.assign(**{"year": year})

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().reset_index()

    # Make column names snake case.
    tb = tb.underscore()

    return tb


def parse_oil_spot_crude_prices(data: pr.ExcelFile, metadata: TableMeta):
    sheet_name = "Oil - Spot crude prices"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=0, metadata=metadata, na_values=["-"])

    # Re-create the original column names, assuming the zeroth column is for years.
    tb.columns = ["year"] + [
        "Oil spot crude prices - " + " ".join(tb[column].iloc[0:2].fillna("").astype(str).tolist()).strip()
        for column in tb.columns[1:]
    ]

    # The units should be written in the header of the first column.
    assert tb.iloc[2][0] == "US dollars per barrel", f"Units (or sheet format) may have changed in sheet {sheet_name}"

    # Drop header rows.
    tb = tb.drop([0, 1, 2, 3]).reset_index(drop=True)

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns=tb.columns[0]).columns, how="all")

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.set_index(["year"], verify_integrity=True).sort_index().reset_index()

    # Make column names snake case.
    tb = tb.underscore()

    return tb


def parse_oil_crude_prices(data: pr.ExcelFile, metadata: TableMeta):
    sheet_name = "Oil crude prices since 1861"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=3, metadata=metadata, na_values=["-"])

    # Rename columns.
    tb.columns = ["year"] + ["Oil crude prices - " + column for column in tb.columns[1:]]

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns=tb.columns[0]).columns, how="all")

    # Make column names snake case.
    tb = tb.underscore()

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.set_index(["year"], verify_integrity=True).sort_index().reset_index()

    return tb


def parse_gas_prices(data: pr.ExcelFile, metadata: TableMeta) -> Table:
    sheet_name = "Gas Prices "
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=1, metadata=metadata, na_values="-")

    # Re-create the original column names, assuming the zeroth column is for countries.
    tb.iloc[0] = tb.iloc[0].ffill()
    tb.columns = ["year"] + [
        "  - ".join(tb[column].iloc[0:3].fillna("").astype(str).tolist()).strip() for column in tb.columns[1:]
    ]

    # Remove numbers from column names (they are references to footnotes).
    tb.columns = [re.sub(r"\d", "", column).strip() for column in tb.columns]

    # Drop header rows.
    tb = tb.drop([0, 1, 2]).reset_index(drop=True)

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns=tb.columns[0]).columns, how="all")

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.set_index(["year"], verify_integrity=True).sort_index().reset_index()

    # Make column names snake case.
    tb = tb.underscore()

    return tb


def parse_coal_prices(data: pr.ExcelFile, metadata: TableMeta) -> Table:
    sheet_name = "Coal Prices"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=1, metadata=metadata, na_values=["-"])

    # Remove spurious columns.
    tb = tb.drop(columns=[column for column in tb.columns if column.startswith("Unnamed:") or len(column.strip()) == 0])

    assert tb.columns[0] == "US dollars per tonne", f"Units (or sheet format) may have changed in sheet {sheet_name}"

    # Rename year column and remove spurious symbols used to signal footnotes.
    tb.columns = ["year"] + tb.columns[1:].tolist()
    tb.columns = [re.sub(r"\†|\‡|\^|\*|\#", "", column).strip() for column in tb.columns]

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns=tb.columns[0]).columns, how="all")

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.set_index(["year"], verify_integrity=True).sort_index().reset_index()

    # Make column names snake case.
    tb = tb.underscore()

    return tb


def create_table_of_fossil_fuel_prices(
    tb_oil_spot_crude_prices: Table, tb_oil_crude_prices: Table, tb_gas_prices: Table, tb_coal_prices: Table
) -> Table:
    # Combine additional tables of fossil fuel prices.
    tb_prices = tb_oil_spot_crude_prices.copy()
    for tb_additional in [tb_oil_crude_prices, tb_gas_prices, tb_coal_prices]:
        # Add current table to combined table.
        tb_prices = tb_prices.merge(tb_additional, how="outer", on=["year"]).copy_metadata(from_table=tb_prices)
    # Rename table appropriately.
    tb_prices.metadata.short_name += "_fossil_fuel_prices"
    tb_prices = tb_prices.set_index(["year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return tb_prices


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap = cast(Snapshot, paths.load_dependency("statistical_review_of_world_energy.csv"))
    snap_additional = cast(Snapshot, paths.load_dependency("statistical_review_of_world_energy.xlsx"))

    # Most data comes from the wide-format csv file, and some additional variables from the excel file.
    tb = pr.read_csv(snap.path, metadata=snap.to_table_metadata(), underscore=True)
    data_additional = pr.ExcelFile(snap_additional.path)

    #
    # Process data.
    #
    # Parse coal reserves sheet.
    tb_coal_reserves = parse_coal_reserves(data=data_additional, metadata=snap_additional.to_table_metadata())

    # Parse oil spot crude prices.
    tb_oil_spot_crude_prices = parse_oil_spot_crude_prices(
        data=data_additional, metadata=snap_additional.to_table_metadata()
    )

    # Parse oil crude prices.
    tb_oil_crude_prices = parse_oil_crude_prices(data=data_additional, metadata=snap_additional.to_table_metadata())

    # Parse gas prices.
    tb_gas_prices = parse_gas_prices(data=data_additional, metadata=snap_additional.to_table_metadata())

    # Parse coal prices.
    tb_coal_prices = parse_coal_prices(data=data_additional, metadata=snap_additional.to_table_metadata())

    # Combine main table and coal reserves.
    tb = tb.merge(tb_coal_reserves, how="outer", on=["country", "year"]).copy_metadata(from_table=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Create combined table of fossil fuel prices.
    tb_prices = create_table_of_fossil_fuel_prices(
        tb_oil_spot_crude_prices=tb_oil_spot_crude_prices,
        tb_oil_crude_prices=tb_oil_crude_prices,
        tb_gas_prices=tb_gas_prices,
        tb_coal_prices=tb_coal_prices,
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb, tb_prices], default_metadata=snap.metadata)

    ds_meadow.save()
