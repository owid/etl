"""Load the Statistical Review of World Energy files and create a meadow dataset.

Since the 2025 release, the Energy Institute reports energy using Total Energy Supply (TES, with the
physical energy content method) instead of the old "substitution" primary energy consumption.

The energy indicators (total energy supply, generation, production and consumption by fuel,
electricity by fuel, etc.) are taken from the narrow-format consolidated CSV: it is much simpler to
parse than the main workbook, and its values are identical to the workbook's. The workbook is only
used for the data that the consolidated dataset does not include: coal/oil/gas reserves, fossil fuel
prices, minerals, and the thermal-equivalent efficiency factors.
"""

import re

import owid.catalog.processing as pr
from owid.catalog import License, Table
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def pre_harmonize_country_names(tb: Table) -> Table:
    # For consistency with the consolidated dataset, rename some countries to the names used there.
    # Their names will be properly harmonized in the garden step.
    country_mapping = {
        "Eastern Africa": "Total Eastern Africa",
        "Middle Africa": "Total Middle Africa",
        "Western Africa": "Total Western Africa",
        "Central America": "Total Central America",
        "European Union": "Total EU",
        "Non-OECD": "Total Non-OECD",
        "OECD": "Total OECD",
        "OPEC": "Total OPEC",
        "Non-OPEC": "Total Non-OPEC",
        "Turkey": "Turkiye",
        # The minerals sheets spell Türkiye with special characters, unlike the consolidated dataset.
        "Türkiye": "Turkiye",
        "DR Congo": "Democratic Republic of Congo",
        "Other North Africa": "Other Northern Africa",
        # The following entities appear in minerals data. Pre-harmonize them to be consistent with the CSV.
        "United States": "US",
        "Russia": "Russian Federation",
        "Burma": "Myanmar",
    }
    tb["country"] = map_series(
        tb["country"],
        country_mapping,
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=False,
        show_full_warning=True,
    )

    return tb


def parse_coal_reserves(data: pr.ExcelFile) -> Table:
    sheet_name = "Coal - Reserves"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=3)

    # The year of the data is written in the header of the sheet.
    # Extract it using a regular expression.
    _year = re.findall(r"\d{4}", tb.columns[0])  # ty: ignore
    assert len(_year) == 1, f"Year could not be extracted from the header of the sheet {sheet_name}."
    year = int(_year[0])

    # Re-create the original column names, assuming the zeroth column is for countries.
    new_columns = ["country"] + [
        "Coal reserves - " + " ".join(tb[column].iloc[0:2].fillna("").astype(str).tolist()).strip()
        for column in tb.columns[1:]
    ]
    tb = tb.rename(columns={column: new_columns[i] for i, column in enumerate(tb.columns)}, errors="raise")

    # The units should be written in the header of the first column.
    assert tb.iloc[1, 0] == "Million tonnes", f"Units (or sheet format) may have changed in sheet {sheet_name}"
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
    tb = tb.format()

    return tb


def parse_oil_reserves(data: pr.ExcelFile) -> Table:
    sheet_name = "Oil - Proved reserves history"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=4)

    # Check that units are the expected ones.
    assert tb.columns[0] == "Thousand million barrels", (
        f"Units (or sheet format) may have changed in sheet {sheet_name}"
    )
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
    country_mapping = {
        "European Union#": "Total EU",
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
    tb = tb.format()

    return tb


def parse_gas_reserves(data: pr.ExcelFile) -> Table:
    sheet_name = "Gas - Proved reserves history"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=4)

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
    tb = tb.format()

    return tb


def parse_oil_spot_crude_prices(data: pr.ExcelFile):
    sheet_name = "Spot crude prices"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=0, na_values=["-"])

    # Re-create the original column names, assuming the zeroth column is for years.
    tb.columns = ["year"] + [
        "Oil spot crude prices - " + " ".join(tb[column].iloc[0:2].fillna("").astype(str).tolist()).strip()
        for column in tb.columns[1:]
    ]

    # The units should be written in the header of the first column.
    assert tb.iloc[2, 0] == "US dollars per barrel", f"Units (or sheet format) may have changed in sheet {sheet_name}"

    # Drop header rows.
    tb = tb.drop([0, 1, 2, 3]).reset_index(drop=True)

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns=tb.columns[0]).columns, how="all")

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.format(keys=["year"])

    return tb


def parse_oil_crude_prices(data: pr.ExcelFile):
    sheet_name = "Oil crude prices since 1861"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=3, na_values=["-"])

    # Rename columns.
    tb.columns = ["year"] + ["Oil crude prices - " + column for column in tb.columns[1:]]

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns=tb.columns[0]).columns, how="all")

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.format(keys=["year"])

    return tb


def parse_gas_prices(data: pr.ExcelFile) -> Table:
    sheet_name = "Gas Prices "
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=0, na_values="-")

    # Re-create the original column names, assuming the zeroth column is for countries.
    tb.iloc[0] = tb.iloc[0].ffill()
    units = tb.iloc[1].ffill()
    error = f"Units (or sheet format) may have changed in sheet {sheet_name}"
    # NOTE: In the 2026 release, hydrogen prices (given in "$/kg") were removed; the sheet now only has
    # LNG and Natural Gas (in "US dollars per million Btu") and Ammonia (in "$/mt").
    assert set(units.dropna()) == {"$/mt", "US dollars per million Btu"}, error
    tb = tb.drop(1)
    tb.columns = ["year"] + [
        "  - ".join(tb[column].iloc[0:3].fillna("").astype(str).tolist()).strip() for column in tb.columns[1:]
    ]
    # Remove numbers from column names (they are references to footnotes).
    tb.columns = [re.sub(r"\d", "", column).strip().strip(" -").replace("\n", " ") for column in tb.columns]
    # Remove spurious spaces.
    tb.columns = [re.sub(r"\s{2,3}", " ", column) for column in tb.columns]

    # Drop header rows.
    tb = tb.drop([0, 2, 3]).reset_index(drop=True)

    # Drop empty columns and rows.
    tb = tb.dropna(axis=1, how="all")
    tb = tb.dropna(how="all").reset_index(drop=True)

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.drop(columns=tb.columns[0]).columns, how="all")

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.format(keys=["year"])

    return tb


def parse_coal_prices(data: pr.ExcelFile) -> Table:
    sheet_name = "Coal & Uranium - Prices"
    # Unfortunately, using header=[...] doesn't work, so the header must be extracted in a different way.
    tb = data.parse(sheet_name, skiprows=0, na_values=["-"])

    # Re-create the original column names, assuming the zeroth column is for countries.
    tb.iloc[0] = tb.iloc[0].ffill()
    units = tb.iloc[1].ffill()
    error = f"Units (or sheet format) may have changed in sheet {sheet_name}"
    assert set(units.dropna()) == {"$/lb", "US dollars per tonne"}, error
    tb = tb.drop(1)
    tb.columns = ["year"] + [
        "  - ".join(tb[column].iloc[0:2].fillna("").astype(str).tolist()).strip() for column in tb.columns[1:]
    ]
    # Remove numbers from column names (they are references to footnotes).
    tb.columns = [re.sub(r"\d", "", column).strip().strip(" -").replace("\n", " ") for column in tb.columns]
    # Remove spurious spaces.
    tb.columns = [re.sub(r"\s{2,3}", " ", column) for column in tb.columns]

    # Drop unnecessary rows.
    tb = tb.drop([0, 2])

    # There are many rows of footers at the end, occupying values of the zeroth column.
    # Remove all those rows, for which all columns are nan except the country column.
    tb = tb.dropna(subset=tb.columns[1:], how="all").reset_index(drop=True)

    # Ensure index columns are not repeated, and sort rows and columns conveniently.
    tb = tb.format(keys=["year"])

    return tb


def _parse_mineral_sheet(data_spreadsheet: pr.ExcelFile, sheet_name: str) -> Table:
    # Extract mineral name.
    mineral = sheet_name.replace(" P-R", "")

    # Define production and reserves column names, following the naming of the consolidated dataset.
    mineral_short_name = mineral.lower().replace(" ", "_")
    production_column = f"{mineral_short_name}_production_kt"
    reserves_column = f"{mineral_short_name}_reserves_kt"

    # Parse sheet.
    tb_production_and_reserves = data_spreadsheet.parse(sheet_name=sheet_name, skiprows=2)
    error = f"Sheet {sheet_name} has changed."
    assert tb_production_and_reserves.columns[0].startswith("Thousand tonnes"), error
    tb_production_and_reserves = tb_production_and_reserves.rename(
        columns={tb_production_and_reserves.columns[0]: "country"}, errors="raise"
    )

    # Remove empty rows and rows of footnotes.
    tb_production_and_reserves = tb_production_and_reserves.dropna(
        subset=tb_production_and_reserves.columns[1:], how="all"
    ).reset_index(drop=True)

    # Remove numbers from country names (they are references to footnotes).
    tb_production_and_reserves["country"] = [
        re.sub(r"\d", "", country).strip().strip(" -").replace("\n", " ")
        for country in tb_production_and_reserves["country"]
    ]
    # Remove spurious spaces.
    tb_production_and_reserves["country"] = [
        re.sub(r"\s{2,3}", " ", country) for country in tb_production_and_reserves["country"]
    ]

    # Pre-harmonize country names.
    tb_production_and_reserves = pre_harmonize_country_names(tb=tb_production_and_reserves)

    # Extract reserves data.
    _column_reserves = [column for column in tb_production_and_reserves.columns if str(column).startswith("At end of")]
    assert len(_column_reserves) == 1, f"Could not extract reserves column for {sheet_name}"
    column_reserves = _column_reserves[0]
    reserves_year = int(re.findall(r"\d{4}", column_reserves)[0])
    tb_reserves = (
        tb_production_and_reserves[["country", column_reserves]]
        .assign(**{"year": reserves_year})
        .rename(columns={column_reserves: reserves_column}, errors="raise")
    )

    # Ignore columns of share, change, and reserves.
    tb_production_and_reserves = tb_production_and_reserves[
        ["country"] + [column for column in tb_production_and_reserves.columns if isinstance(column, int)]
    ]

    # Create a table of production data.
    tb_production = tb_production_and_reserves.melt(id_vars=["country"], var_name="year", value_name=production_column)

    # Combine production and reserves data.
    tb_mineral = tb_production.merge(tb_reserves, on=["country", "year"], how="outer")

    # Ensure production and reserves columns are numeric (some sheets carry footnote markers as strings).
    for column in [production_column, reserves_column]:
        tb_mineral[column] = pr.to_numeric(tb_mineral[column], errors="coerce")

    return tb_mineral


def parse_minerals_data(data_spreadsheet: pr.ExcelFile) -> Table:
    # NOTE: In the 2026 release, the Energy Institute stopped publishing several minerals that were
    # available in earlier releases (Platinum Group Metals, Bauxite, Aluminium, Tin and Vanadium).
    minerals = [
        "Cobalt",
        "Lithium",
        "Natural Graphite",
        "Rare Earth metals",
        "Copper",
        "Manganese",
        "Nickel",
        "Zinc",
    ]
    tables_minerals = [
        _parse_mineral_sheet(data_spreadsheet=data_spreadsheet, sheet_name=f"{mineral} P-R") for mineral in minerals
    ]
    tb_minerals = pr.multi_merge(tables=tables_minerals, on=["country", "year"], how="outer")

    # Improve table format.
    tb_minerals = tb_minerals.format()

    return tb_minerals


def parse_consolidated_dataset(tb: Table) -> Table:
    # The narrow (long) format consolidated dataset has one row per country, year and indicator.
    # Pivot it into a wide table with one column per indicator.
    tb["value"] = pr.to_numeric(tb["value"], errors="coerce")
    tb = tb.pivot(index=["country", "year"], columns="var", values="value", join_column_levels_with="_")

    # Sanity checks.
    error = "Duplicated (country, year) rows found in the consolidated dataset."
    assert tb[tb.duplicated(subset=["country", "year"])].empty, error
    assert "tes_ej" in tb.columns, "Expected total energy supply column 'tes_ej' not found in the consolidated dataset."

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_consolidated = paths.load_snapshot("statistical_review_of_world_energy.csv")
    snap_spreadsheet = paths.load_snapshot("statistical_review_of_world_energy.xlsx")

    # Read the narrow-format consolidated dataset (the source of truth for energy indicators), and the workbook.
    tb_consolidated = snap_consolidated.read_csv(underscore=True, low_memory=False)
    data_spreadsheet = snap_spreadsheet.ExcelFile()

    #
    # Process data.
    #
    # Extract all energy indicators from the consolidated dataset.
    tb = parse_consolidated_dataset(tb=tb_consolidated)

    # Parse coal, gas and oil reserves (only available in the workbook).
    tb_coal_reserves = parse_coal_reserves(data=data_spreadsheet)
    tb_gas_reserves = parse_gas_reserves(data=data_spreadsheet)
    tb_oil_reserves = parse_oil_reserves(data=data_spreadsheet)

    # Parse minerals production and reserves (only available in the workbook).
    tb_minerals = parse_minerals_data(data_spreadsheet=data_spreadsheet)

    # Parse fossil fuel prices (only available in the workbook).
    tb_oil_spot_crude_prices = parse_oil_spot_crude_prices(data=data_spreadsheet)
    tb_oil_crude_prices = parse_oil_crude_prices(data=data_spreadsheet)
    tb_gas_prices = parse_gas_prices(data=data_spreadsheet)
    tb_coal_prices = parse_coal_prices(data=data_spreadsheet)

    # NOTE: Since the 2026 release, the "Approximate conversion factors" sheet no longer includes the
    # thermal-equivalent efficiency factors that were used to convert non-fossil electricity to
    # substitution-method primary energy. We no longer need them, since we report Total Energy Supply.

    # Combine the main table with coal, gas, and oil reserves, and minerals.
    tb = pr.multi_merge(
        [
            tb,
            tb_coal_reserves.reset_index(),
            tb_gas_reserves.reset_index(),
            tb_oil_reserves.reset_index(),
            tb_minerals.reset_index(),
        ],
        how="outer",
        on=["country", "year"],
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format(sort_columns=True)

    # Create combined table of fossil fuel prices.
    tb_prices = pr.multi_merge(
        [
            tb_oil_spot_crude_prices.reset_index(),
            tb_oil_crude_prices.reset_index(),
            tb_gas_prices.reset_index(),
            tb_coal_prices.reset_index(),
        ],
        how="outer",
        on=["year"],
    )

    # Set an appropriate index and sort conveniently.
    tb_prices = tb_prices.format(
        keys=["year"], sort_columns=True, short_name="statistical_review_of_world_energy_prices"
    )

    # Prices variables need to cite S&P Global Platts, and include their license.
    year_published = tb_prices[tb_prices.columns[0]].metadata.origins[0].date_published[0:4]
    for column in tb_prices.columns:
        assert len(tb_prices[column].metadata.origins) == 1, f"Unexpected origins in column {column}"
        tb_prices[column].metadata.origins[
            0
        ].attribution = (
            f"Energy Institute based on S&P Global Platts - Statistical Review of World Energy ({year_published})"
        )
        tb_prices[column].metadata.origins[
            0
        ].citation_full = (
            f"Energy Institute based on S&P Global Platts - Statistical Review of World Energy ({year_published})"
        )
        tb_prices[column].metadata.licenses.append(License(name=f"© S&P Global Inc. {year_published}"))

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb, tb_prices], default_metadata=snap_consolidated.metadata)
    ds_meadow.save()
