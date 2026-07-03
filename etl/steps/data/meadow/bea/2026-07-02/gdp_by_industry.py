"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Value added workbook inside the snapshot archive, and its annual current-dollar table.
DATA_FILE = "ValueAdded.xlsx"
SHEET = "TVA105-A"

# Rows of the table to keep, by their line number: gross domestic product, private
# industries, government, and the top-level industries. The remaining lines are industry
# detail or addenda.
LINES = {
    1: "Gross domestic product",
    2: "Private industries",
    3: "Agriculture, forestry, fishing, and hunting",
    6: "Mining",
    10: "Utilities",
    11: "Construction",
    12: "Manufacturing",
    34: "Wholesale trade",
    35: "Retail trade",
    40: "Transportation and warehousing",
    49: "Information",
    54: "Finance, insurance, real estate, rental, and leasing",
    65: "Professional and business services",
    74: "Educational services, health care, and social assistance",
    81: "Arts, entertainment, recreation, accommodation, and food services",
    88: "Other services, except government",
    89: "Government",
}

# Row of the sheet holding the year labels.
HEADER_ROW = 7


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gdp_by_industry.zip")

    # Load the annual value added by industry table (current prices, millions of dollars).
    with snap.extracted() as archive:
        tb = archive.read(DATA_FILE, sheet_name=SHEET, header=None)

    #
    # Process data.
    #
    # Columns are years, labeled in the header row.
    header = tb.iloc[HEADER_ROW]
    year_columns = {column: int(header[column]) for column in tb.columns[2:] if pd.notna(header[column])}
    tb = tb.iloc[HEADER_ROW + 1 :].rename(columns={0: "line", 1: "industry", **year_columns})

    # Keep the selected lines and check their titles are the expected industries.
    tb["line"] = pr.to_numeric(tb["line"], errors="coerce")
    tb = tb[tb["line"].isin(LINES)]
    industry_metadata = tb["industry"].metadata
    tb["industry"] = tb["industry"].str.strip()
    tb["industry"].metadata = industry_metadata
    error = "The line numbers of the value added table no longer match the expected industries."
    assert dict(zip(tb["line"], tb["industry"])) == LINES, error

    # Reshape to long format and ensure numeric values.
    tb = tb.melt(
        id_vars=["line", "industry"], value_vars=list(year_columns.values()), var_name="year", value_name="value_added"
    )
    tb["value_added"] = pr.to_numeric(tb["value_added"], errors="coerce")
    tb = tb.dropna(subset=["value_added"])

    # Improve tables format.
    tables = [tb.format(["industry", "year"], short_name=paths.short_name)]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
