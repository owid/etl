"""Load a snapshot and create a meadow dataset."""

from typing import Dict

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Expected sheet names.
EXPECTED_SHEET_NAMES = ["Country", "Pivot", "Region", "Global", "About"]
# Expected columns in the All Data sheet, and how to rename them (to make them consistent with other sheets).
EXPECTED_COLUMNS = {
    "Region": "Region",
    "Sub-region": "Sub-region",
    "Country": "Country",
    "ISO3 code": "ISO3 code",
    "M49 code": "M49 code",
    "RE or Non-RE": "RE or Non-RE",
    "Group Technology": "Group Technology",
    "Technology": "Technology",
    "Sub-Technology": "Sub-Technology",
    "Producer Type": "Producer Type",
    "Year": "Year",
    "Sum of Electricity Generation (GWh)": "Electricity Generation (GWh)",
    "Sum of Electricity Installed Capacity (MW)": "Electricity Installed Capacity (MW)",
    "Sum of Heat Generation (TJ)": "Heat Generation (TJ)",
    "Sum of Public Flows (2022 USD M)": "Public Flows (2022 USD M)",
    "Sum of SDG 7a1 Intl. Public Flows (2022 USD M)": "SDG 7a1 Intl. Public Flows (2022 USD M)",
    "Sum of SDG 7b1 RE capacity per capita (W/inhabitant)": "SDG 7b1 RE capacity per capita (W/inhabitant)",
    # "Off-grid Biogas for Cooking (1,000 inhabitants)",
    # "Off-grid Biogas Production (1,000 m3)",
    # "Off-grid Electricity Access (1,000 inhabitants)",
}


def read_data_from_snapshot(snap: Snapshot) -> Dict[str, Table]:
    data = snap.ExcelFile()
    tables = {}
    for sheet in data.sheet_names:
        if sheet == "Pivot":
            _table = data.parse(sheet, skiprows=2, na_values=["(blank)", "(empty)"])
        else:
            _table = data.parse(sheet)
        tables[sheet.strip()] = _table

    return tables


def sanity_check_inputs(tables: Dict[str, Table]) -> None:
    # Sanity checks.
    # NOTE: For convenience, remove spurious spaces in sheet names.
    error = "Sheet names have changed."
    assert set(tables) == set(EXPECTED_SHEET_NAMES), error

    error = "Columns have changed in the 'Pivot' sheet."
    assert set(EXPECTED_COLUMNS) == set(tables["Pivot"].columns), error

    # Ensure data from "Country" and "Pivot" sheets agree.
    check = tables["Pivot"].merge(
        tables["Country"].rename(columns={"Electricity Installed Capacity (MW)": "check"}, errors="raise")[
            ["Country", "Year", "Sub-Technology", "Producer Type", "check"]
        ],
        how="inner",
        on=["Country", "Year", "Sub-Technology", "Producer Type"],
    )
    check = (
        check[["Country", "Year", "Sub-Technology", "Sum of Electricity Installed Capacity (MW)", "check"]]
        .dropna()
        .reset_index(drop=True)
    )
    error = "Unexpected mismatch between data from 'Country' and 'Pivot' sheets."
    assert check[check["Sum of Electricity Installed Capacity (MW)"] != check["check"]].empty, error


def run() -> None:
    #
    # Load inputs.
    #
    # Load snapshot and read data from all its sheets.
    snap = paths.load_snapshot("renewable_energy_statistics.xlsx")

    # Read data from snapshot.
    tables = read_data_from_snapshot(snap=snap)

    # Sanity checks.
    sanity_check_inputs(tables=tables)

    # Combine global, regional, and country-level data.
    tb_global = (
        tables["Global"]
        .assign(**{"Country": "World"})
        .rename(
            columns={"Electricity Installed Capacity (MW)": "Electricity Installed Capacity (MW)"},
            errors="raise",
        )
    )
    tb_regional = (
        tables["Region"]
        .rename(columns={"Region": "Country"}, errors="raise")
        .rename(
            columns={"Electricity Installed Capacity (MW)": "Electricity Installed Capacity (MW)"},
            errors="raise",
        )
    )
    tb_all_data = (
        tables["Pivot"]
        .rename(columns=EXPECTED_COLUMNS, errors="raise")
        .drop(columns=["Region", "Sub-region", "ISO3 code", "M49 code"], errors="raise")
        .dropna(how="all")
        .reset_index(drop=True)
        .astype({"Year": int})
    )
    tb = pr.concat([tb_all_data, tb_global, tb_regional], ignore_index=True)

    # Format table.
    tb = tb.format(keys=["country", "year", "group_technology", "technology", "sub_technology", "producer_type"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
