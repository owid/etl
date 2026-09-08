"""Load a snapshot and create a meadow dataset.

NOTE: The 2026 release restructured the file. Data used to be wide (one column per measure) and is now long,
with a "Flow" column naming the measure, a "Value" column, and a "Unit" column. Here we pivot it back to one
column per measure, so that each column carries a single unit. The measure columns are named after the units
the producer actually uses in this release; note that electricity generation is now given in MWh, whereas
previous releases gave it in GWh.

The release also replaced the technology taxonomy (see the "Tech Mapping" sheet, which relates the new product
names to the legacy ones) and switched country labels to abbreviated forms. Both are handled in the garden step.

"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Expected sheet names.
EXPECTED_SHEET_NAMES = ["About", "Country", "Region", "Global", "Tech Mapping"]

# Expected columns in each of the data sheets.
EXPECTED_COLUMNS = {
    "Country": [
        "Region",
        "Sub-region",
        "Country",
        "ISO3 Code",
        "M49 Code",
        "Flow",
        "RE or Non-RE",
        "Group Technology",
        "Sub-Technology",
        "Technology",
        "Plant Type",
        "Producer Type",
        "Year",
        "Value",
        "Unit",
    ],
    "Region": ["Region", "RE or Non-RE", "Flow", "Group Technology", "Year", "Value", "Unit"],
    "Global": ["RE or Non-RE", "Flow", "Group Technology", "Year", "Value", "Unit"],
}

# Measures ("flows") given by the producer, the unit each is given in, and the column to store each of them in.
# NOTE: Asserting the unit of each flow is what would catch a repeat of the GWh -> MWh change of the 2026 release.
FLOWS = {
    "Electrical Capacity": {"unit": "MW", "column": "Electricity Installed Capacity (MW)"},
    "Electrical Production": {"unit": "MWh", "column": "Electricity Generation (MWh)"},
    "Heat Production": {"unit": "TJ", "column": "Heat Generation (TJ)"},
}

# Flow whose data we publish, and which therefore must not contain duplicate rows.
MAIN_FLOW = "Electrical Capacity"

# Dimension columns that identify a series in the country-level sheet.
COUNTRY_DIMENSIONS = [
    "Country",
    "Year",
    "RE or Non-RE",
    "Group Technology",
    "Technology",
    "Sub-Technology",
    "Producer Type",
]


def strip_whitespace(tb: Table) -> Table:
    # Some category labels carry trailing spaces (e.g. "Fossil fuels "), which would otherwise be treated as
    # categories of their own.
    for column in tb.columns:
        if tb[column].dtype == object:
            metadata = tb[column].metadata
            tb[column] = tb[column].str.strip()
            tb[column].metadata = metadata

    return tb


def read_data_from_snapshot(snap: Snapshot) -> dict[str, Table]:
    data = snap.ExcelFile()
    tables = {sheet: strip_whitespace(data.parse(sheet)) for sheet in data.sheet_names}

    return tables


def sanity_check_inputs(tables: dict[str, Table]) -> None:
    error = "Sheet names have changed."
    assert set(tables) == set(EXPECTED_SHEET_NAMES), error

    for sheet, columns in EXPECTED_COLUMNS.items():
        error = f"Columns have changed in the '{sheet}' sheet."
        assert list(tables[sheet].columns) == columns, error

    for sheet in EXPECTED_COLUMNS:
        tb = tables[sheet]

        error = f"Flows have changed in the '{sheet}' sheet."
        assert set(tb["Flow"]) <= set(FLOWS), error

        # Each flow must be given in exactly the unit we assume when naming its column.
        for flow, units in tb.groupby("Flow", observed=True)["Unit"].unique().items():
            error = f"Unexpected unit for flow '{flow}' in the '{sheet}' sheet: {sorted(units)}."
            assert list(units) == [FLOWS[str(flow)]["unit"]], error

    # "Plant Type" is fully determined by "Producer Type" (e.g. "On-grid CHP" always has plant type "CHP"),
    # so it can be dropped without losing information.
    error = "Column 'Plant Type' is no longer determined by 'Producer Type'."
    assert (tables["Country"].groupby("Producer Type", observed=True)["Plant Type"].nunique() == 1).all(), error

    # The producer ships a handful of duplicate rows (same series, same year, two values). They are summed below,
    # which is only acceptable while they stay out of the data we actually publish.
    for sheet, dimensions in [
        ("Country", COUNTRY_DIMENSIONS),
        ("Region", ["Region", "Year", "RE or Non-RE", "Group Technology"]),
        ("Global", ["Year", "RE or Non-RE", "Group Technology"]),
    ]:
        tb = tables[sheet]
        duplicated = tb[tb["Flow"] == MAIN_FLOW].duplicated(subset=dimensions)
        error = f"Unexpected duplicate '{MAIN_FLOW}' rows in the '{sheet}' sheet."
        assert not duplicated.any(), error


def pivot_flows_to_columns(tb: Table, dimensions: list[str]) -> Table:
    # Sum the (few) duplicate rows the producer ships, and give each flow its own column.
    tb = tb.groupby(dimensions + ["Flow"], observed=True, dropna=False, as_index=False).agg({"Value": "sum"})
    tb = tb.pivot(index=dimensions, columns="Flow", values="Value", join_column_levels_with="_")
    tb = tb.rename(columns={flow: FLOWS[flow]["column"] for flow in FLOWS}, errors="ignore")

    return tb


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

    #
    # Process data.
    #
    # Combine country-level, regional, and global data.
    tb_country = pivot_flows_to_columns(tb=tables["Country"], dimensions=COUNTRY_DIMENSIONS)
    tb_regional = pivot_flows_to_columns(
        tb=tables["Region"], dimensions=["Region", "Year", "RE or Non-RE", "Group Technology"]
    ).rename(columns={"Region": "Country"}, errors="raise")
    tb_global = pivot_flows_to_columns(
        tb=tables["Global"], dimensions=["Year", "RE or Non-RE", "Group Technology"]
    ).assign(**{"Country": "World"})
    tb = pr.concat([tb_country, tb_regional, tb_global], ignore_index=True)

    # Improve dtypes of low-cardinality columns.
    for column in ["Country", "RE or Non-RE", "Group Technology", "Technology", "Sub-Technology", "Producer Type"]:
        tb[column] = tb[column].astype("category")

    # Format table.
    tb = tb.format(keys=["country", "year", "group_technology", "technology", "sub_technology", "producer_type"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
