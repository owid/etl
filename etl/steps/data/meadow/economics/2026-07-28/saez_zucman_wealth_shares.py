"""Load a snapshot and create a meadow dataset.

The source is a multi-sheet replication workbook. We read only the wealth-shares sheet
("DataF1-F2(Wealth)"), which has a three-row header (source method, population concept,
wealth bracket) and yearly data from row 9 onwards. We keep the revised Saez-Zucman
September 2020 series (columns 7-12): top 10% / 1% / 0.1% shares for two population
concepts, tax units and equal-split adults.
"""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SHEET = "DataF1-F2(Wealth)"

# Column index in the raw sheet -> descriptive indicator name.
# "share_top_0p1" == top 0.1%; suffix marks the population concept.
COLUMNS = {
    0: "year",
    7: "share_top_10_tax_units",
    8: "share_top_1_tax_units",
    9: "share_top_0p1_tax_units",
    10: "share_top_10_equal_split",
    11: "share_top_1_equal_split",
    12: "share_top_0p1_equal_split",
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("saez_zucman_wealth_shares.xlsx")

    # Read the wealth sheet without a header; the real headers span rows 6-8 and data starts at row 9.
    tb = snap.read_excel(sheet_name=SHEET, header=None)

    #
    # Process data.
    #
    # Keep only the year column and the revised Saez-Zucman September 2020 series.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Keep only the yearly data region (rows where the first column is a numeric year).
    tb = tb[pr.to_numeric(tb["year"], errors="coerce").notna()].copy()
    tb["year"] = tb["year"].astype(int)

    # Drop years with no wealth data at all (the sheet pads a few empty years at the ends).
    value_cols = [c for c in COLUMNS.values() if c != "year"]
    tb = tb.dropna(subset=value_cols, how="all")

    # Reshaping a header-less sheet can drop a column's origin; every value column comes from
    # this snapshot, so restore the origin explicitly.
    for col in value_cols:
        tb[col].metadata.origins = [snap.metadata.origin]

    # This is a US-only source; add the country column explicitly.
    tb["country"] = "United States"
    tb["country"] = tb["country"].astype("category")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
