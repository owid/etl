"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Column layout of Table III (value added at current prices, 1800-2022): year in column 0,
# main sectors in columns 1-7 and GDP at factor prices in column 8. Columns 10 onwards
# repeat the series at constant (1910/12) prices and are not used here.
VA_COLUMNS = {
    0: "year",
    1: "agriculture",
    2: "manufacturing_industry",
    3: "building_construction",
    4: "transport_communication",
    5: "private_services",
    6: "public_services",
    7: "services_of_dwellings",
    8: "gdp",
}

# Column layout of Table VIII (employment in persons, 1850-2022): year in column 0,
# main sectors in columns 1-6 and total employment in column 7.
EMP_COLUMNS = {
    0: "year",
    1: "agriculture",
    2: "manufacturing_industry",
    3: "building_construction",
    4: "transport_communication",
    5: "private_services",
    6: "public_services",
    7: "total",
}

# Header rows above the data in each table.
N_HEADER_ROWS = 5


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_va = paths.load_snapshot("swedish_historical_national_accounts_value_added.xlsx")
    snap_emp = paths.load_snapshot("swedish_historical_national_accounts_employment.xlsx")

    # Load data from snapshots.
    tb_va = snap_va.read(sheet_name="Table III", header=None)
    tb_emp = snap_emp.read(sheet_name="Table VIII", header=None)

    #
    # Process data.
    #
    tb_va = prepare_table(tb_va, columns=VA_COLUMNS, short_name="value_added")
    tb_emp = prepare_table(tb_emp, columns=EMP_COLUMNS, short_name="employment")

    # Improve tables format.
    tables = [
        tb_va.format(["country", "year"], short_name="value_added"),
        tb_emp.format(["country", "year"], short_name="employment"),
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap_va.metadata)

    # Save meadow dataset.
    ds_meadow.save()


def prepare_table(tb: Table, columns: dict, short_name: str) -> Table:
    """Extract the data block of a SHNA table and add a country column."""
    tb = tb.iloc[N_HEADER_ROWS:, list(columns.keys())].rename(columns=columns)

    # Ensure numeric values and drop rows without a year or without any data.
    data_columns = [column for column in tb.columns if column != "year"]
    for column in data_columns:
        tb[column] = pr.to_numeric(tb[column], errors="coerce")
    tb["year"] = pr.to_numeric(tb["year"], errors="coerce")
    tb = tb.dropna(subset=["year"])
    tb["year"] = tb["year"].astype(int)
    tb = tb.dropna(subset=data_columns, how="all")

    tb["country"] = "Sweden"

    return tb
