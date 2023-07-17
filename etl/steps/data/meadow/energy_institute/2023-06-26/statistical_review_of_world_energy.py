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

import copy
import re
from typing import cast

import owid.catalog.processing as pr
from owid.catalog import License, Source, Table, TableMeta, VariableMeta
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
    new_columns = ["country"] + [
        "Coal reserves - " + " ".join(tb[column].iloc[0:2].fillna("").astype(str).tolist()).strip()
        for column in tb.columns[1:]
    ]
    tb = tb.rename(columns={column: new_columns[i] for i, column in enumerate(tb.columns)})

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


def parse_oil_reserves(data: pr.ExcelFile, metadata: TableMeta) -> Table:
    sheet_name = "Oil - Proved reserves history"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=2, metadata=metadata)

    # Check that units are the expected ones.
    assert (
        tb.columns[0] == "Thousand million barrels"
    ), f"Units (or sheet format) may have changed in sheet {sheet_name}"
    # Check that zeroth column should correspond to countries.
    assert "Total World" in tb[tb.columns[0]].tolist()

    # Rename country column.
    tb.columns = ["country"] + tb.columns[1:].tolist()

    # The last few columns show growth rate and share of reserves; they will be parsed with a name that does not correspond to a year.
    # To remove them, drop all columns that are not either "country" or a year.
    tb = tb.drop(
        columns=[column for column in tb.columns if re.match(r"^\d{4}$", str(column)) is None if column != "country"]
    )

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
        "European Union #": "Total EU",
        "Non-OECD": "Total Non-OECD",
        "Non-OPEC": "Total Non-OPEC",
        "OECD": "Total OECD",
        "OPEC": "Total OPEC",
    }
    tb["country"] = map_series(tb["country"], country_mapping, warn_on_unused_mappings=True, show_full_warning=True)

    # Transpose table to have a year column.
    tb = tb.melt(id_vars=["country"], var_name="year", value_name="oil_reserves_bbl")

    # Ensure numeric column has the right format.
    tb = tb.astype({"oil_reserves_bbl": float})

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().reset_index()

    # Ensure column names are snake case.
    tb = tb.underscore()

    return tb


def parse_gas_reserves(data: pr.ExcelFile, metadata: TableMeta) -> Table:
    sheet_name = "Gas - Proved reserves history "
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=2, metadata=metadata)

    # Check that units are the expected ones.
    assert tb.columns[0] == "Trillion cubic metres", f"Units (or sheet format) may have changed in sheet {sheet_name}"
    # Check that zeroth column should correspond to countries.
    assert "Total World" in tb[tb.columns[0]].tolist()

    # Rename country column.
    tb.columns = ["country"] + tb.columns[1:].tolist()

    # The last few columns show growth rate and share of reserves; they will be parsed with a name that does not correspond to a year.
    # To remove them, drop all columns that are not either "country" or a year.
    tb = tb.drop(
        columns=[column for column in tb.columns if re.match(r"^\d{4}$", str(column)) is None if column != "country"]
    )

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
        "Non-OECD": "Total Non-OECD",
        "OECD": "Total OECD",
    }
    tb["country"] = map_series(tb["country"], country_mapping, warn_on_unused_mappings=True, show_full_warning=True)

    # Transpose table to have a year column.
    tb = tb.melt(id_vars=["country"], var_name="year", value_name="gas_reserves_tcm")

    # Ensure numeric column has the right format.
    tb = tb.astype({"gas_reserves_tcm": float})

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().reset_index()

    # Ensure column names are snake case.
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
    tb_oil_spot_crude_prices: Table,
    tb_oil_crude_prices: Table,
    tb_gas_prices: Table,
    tb_coal_prices: Table,
    prices_data_source: Source,
    prices_data_license: License,
) -> Table:
    # Combine additional tables of fossil fuel prices.
    tb_prices = tb_oil_spot_crude_prices.copy()
    for tb_additional in [tb_oil_crude_prices, tb_gas_prices, tb_coal_prices]:
        # Add current table to combined table.
        tb_prices = tb_prices.merge(tb_additional, how="outer", on=["year"]).copy_metadata(from_table=tb_prices)
    # Rename table appropriately.
    tb_prices.metadata.short_name += "_fossil_fuel_prices"

    # Add sources and licenses to prices variables.
    for column in tb_prices.columns:
        tb_prices[column].metadata.sources = [prices_data_source]
        tb_prices[column].metadata.licenses = [prices_data_license]

    # Set an appropriate index and sort conveniently.
    tb_prices = tb_prices.set_index(["year"], verify_integrity=True).sort_index().sort_index(axis=1)

    return tb_prices


def parse_thermal_equivalent_efficiency(data: pr.ExcelFile, metadata: TableMeta) -> Table:
    sheet_name = "Approximate conversion factors"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=1, metadata=metadata, na_values=["-"])

    # In the "Approximate conversion factors" sheet, there is a table, called
    # "Thermal equivalent efficiency factors used to convert non-fossil electricity to primary energy."
    # Find the row where that table starts.
    table_start_row = tb[tb.iloc[:, 0].astype(str).str.startswith("Year")].index

    assert (
        len(table_start_row) == 1
    ), "Table of thermal equivalent efficiency factors not found, format may have changed."

    # Select relevant rows and drop empty columns.
    tb = tb.iloc[table_start_row[0] :].dropna(axis=1, how="all").reset_index(drop=True)

    # Use the first row to name columns, and clean column names.
    tb.columns = [column.replace("*", "").strip() for column in tb.iloc[0].values]
    tb = tb.drop(0).reset_index(drop=True)

    # Attempt to get the efficiency of the range of years 1965 - 2000 from the last row.
    older_efficiency_value = tb.iloc[-1].dropna().values[0]
    year_start, year_end, efficiency_factor = re.findall(r"(\d{4}).*(\d{4}).*([\d\.]{4})%.*", older_efficiency_value)[0]

    # Create a table of old efficiencies.
    years = list(range(int(year_start), int(year_end) + 1))
    older_efficiency = Table({"year": years, "efficiency_factor": [float(efficiency_factor) * 0.01] * len(years)})

    # Drop the final row, that includes the efficiency of older years.
    tb = tb.drop(tb.iloc[-1].name).reset_index(drop=True)

    # The data is given in two columns, extract both and put together into one column.
    efficiency = tb[["Year"]].melt()[["value"]].rename(columns={"value": "year"})
    efficiency["efficiency_factor"] = tb[["Efficiency Factor"]].melt()["value"]

    # Concatenate older and curren values of efficiency.
    efficiency = pr.concat([older_efficiency, efficiency], ignore_index=True)

    # Sanity checks.
    assert efficiency["year"].diff().dropna().unique().tolist() == [
        1
    ], "Year columns for efficiency factors was not well extracted."
    assert (
        efficiency["efficiency_factor"].diff().dropna() >= 0
    ).all(), "Efficiency is expected to be monotonically increasing."
    assert (efficiency["efficiency_factor"] > 0.35).all() & (
        efficiency["efficiency_factor"] < 0.5
    ).all(), "Efficiency out of expected range."

    # Set an appropriate index and sort conveniently.
    efficiency = efficiency.set_index(["year"], verify_integrity=True).sort_index()

    # Prepare table and variable metadata.
    efficiency.metadata.short_name = "statistical_review_of_world_energy_efficiency_factors"
    efficiency["efficiency_factor"].metadata = VariableMeta(
        title="Thermal equivalent efficiency factors",
        description="Thermal equivalent efficiency factors used to convert non-fossil electricity to primary energy.",
        sources=metadata.dataset.sources,
        licenses=metadata.dataset.licenses,
    )

    return efficiency


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

    # Parse gas reserves sheeet.
    tb_gas_reserves = parse_gas_reserves(data=data_additional, metadata=snap_additional.to_table_metadata())

    # Parse oil reserves sheeet.
    tb_oil_reserves = parse_oil_reserves(data=data_additional, metadata=snap_additional.to_table_metadata())

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

    # Parse thermal equivalent efficiency factors.
    tb_efficiency_factors = parse_thermal_equivalent_efficiency(
        data=data_additional, metadata=snap_additional.to_table_metadata()
    )

    # Combine main table and coal, gas, and oil reserves.
    for tb_reserves in [tb_coal_reserves, tb_gas_reserves, tb_oil_reserves]:
        tb = tb.merge(tb_reserves, how="outer", on=["country", "year"])

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Adapt source and license for prices data, which is based on S&P Global Platts.
    prices_data_license = copy.deepcopy(snap.metadata.license)
    prices_data_license.name = "© S&P Global Inc. 2023"  # type: ignore
    prices_data_source = copy.deepcopy(snap.metadata.source)
    prices_data_source.name = "Energy Institute Statistical Review of World Energy based on S&P Global Platts (2023)"
    prices_data_source.published_by = (
        "Energy Institute Statistical Review of World Energy based on S&P Global Platts (2023)"
    )

    # Create combined table of fossil fuel prices.
    tb_prices = create_table_of_fossil_fuel_prices(
        tb_oil_spot_crude_prices=tb_oil_spot_crude_prices,
        tb_oil_crude_prices=tb_oil_crude_prices,
        tb_gas_prices=tb_gas_prices,
        tb_coal_prices=tb_coal_prices,
        prices_data_source=prices_data_source,
        prices_data_license=prices_data_license,
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb, tb_prices, tb_efficiency_factors],
        default_metadata=snap.metadata,
        check_variables_metadata=True,
    )
    ds_meadow.save()
