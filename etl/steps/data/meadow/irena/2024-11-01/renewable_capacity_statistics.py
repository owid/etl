"""Load a snapshot and create a meadow dataset."""

from typing import Dict

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Expected sheet names.
EXPECTED_SHEET_NAMES = ["Country", "All Data", "Regional", "Global", "About"]
# Expected columns in the All Data sheet.
EXPECTED_COLUMNS = [
    "Region",
    "Sub-region",
    "Country",
    "ISO3 code",
    "M49 code",
    "RE or Non-RE",
    "Group Technology",
    "Technology",
    "Sub-Technology",
    "Producer Type",
    "Year",
    "Electricity Generation (GWh)",
    "Electricity Installed Capacity (MW)",
    "Heat Generation (TJ)",
    "Off-grid Biogas for Cooking (1,000 inhabitants)",
    "Off-grid Biogas Production (1,000 m3)",
    "Off-grid Electricity Access (1,000 inhabitants)",
    "Public Flows (2021 USD M)",
    "SDG 7a1 Intl. Public Flows (2021 USD M)",
    "SDG 7b1 RE capacity per capita (W/inhabitant)",
]


def sanity_check_inputs(tables: Dict[str, Table]) -> None:
    # Sanity checks.
    error = "Sheet names have changed."
    assert set(tables) == set(EXPECTED_SHEET_NAMES), error

    error = "Columns have changed in the 'All Data' sheet."
    assert set(EXPECTED_COLUMNS) == set(tables["All Data"].columns), error

    # Ensure data from "Country" and "All Data" sheets agree.
    check = tables["All Data"].merge(
        tables["Country"].rename(columns={"Electricity Installed Capacity (MW)": "check"})[
            ["Country", "Year", "Sub-Technology", "Producer Type", "check"]
        ],
        how="inner",
        on=["Country", "Year", "Sub-Technology", "Producer Type"],
    )
    check = (
        check[["Country", "Year", "Sub-Technology", "Electricity Installed Capacity (MW)", "check"]]
        .dropna()
        .reset_index(drop=True)
    )
    error = "Unexpected mismatch between data from 'Country' and 'All Data' sheets."
    assert check[check["Electricity Installed Capacity (MW)"] != check["check"]].empty, error


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load snapshot and read data from all its sheets.
    snap = paths.load_snapshot("renewable_capacity_statistics.xlsx")
    data = snap.ExcelFile()
    tables = {sheet: data.parse(sheet) for sheet in data.sheet_names}

    # Sanity checks.
    sanity_check_inputs(tables)

    # Combine global, regional, and country-level data.
    tb_global = (
        tables["Global"]
        .assign(**{"Country": "World"})
        .rename(
            columns={"Sum of Electricity Installed Capacity (MW)": "Electricity Installed Capacity (MW)"},
            errors="raise",
        )
    )
    tb_regional = (
        tables["Regional"]
        .rename(columns={"Region": "Country"}, errors="raise")
        .rename(
            columns={"Sum of Electricity Installed Capacity (MW)": "Electricity Installed Capacity (MW)"},
            errors="raise",
        )
    )
    tb_all_data = tables["All Data"].drop(columns=["Region", "Sub-region", "ISO3 code", "M49 code"], errors="raise")
    tb = pr.concat([tb_all_data, tb_global, tb_regional], ignore_index=True)

    # Format table.
    tb = tb.format(keys=["country", "year", "group_technology", "technology", "sub_technology", "producer_type"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
