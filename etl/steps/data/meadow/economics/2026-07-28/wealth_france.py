"""Load a snapshot and create a meadow dataset.

The source is the authors' replication workbook. We read only the wealth-distribution
summary sheet ("sum_stat_w"), whose header spans several rows (percentile labels, a
units sub-header and machine codes) with yearly data from row 9 onwards. We keep the
year and the wealth-share columns: bottom 50% (P0-50), middle 40% (P50-90),
top 10% (P90-100) and top 1% (P99-100).
"""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SHEET = "sum_stat_w"

# Column index in the raw sheet -> descriptive indicator name.
COLUMNS = {
    0: "year",
    3: "share_bottom_50",  # P0-50
    4: "share_middle_40",  # P50-90
    5: "share_top_10",  # P90-100
    6: "share_top_1",  # P99-100
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("wealth_france.xlsx")

    # Read the sheet without a header; the real headers span rows 6-8 and data starts at row 9.
    tb = snap.read_excel(sheet_name=SHEET, header=None)

    #
    # Process data.
    #
    # Keep only the year column and the wealth-share columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Keep only the yearly data region (rows where the first column is a numeric year).
    tb = tb[pr.to_numeric(tb["year"], errors="coerce").notna()].copy()
    tb["year"] = tb["year"].astype(int)

    # Drop years with no wealth data at all.
    value_cols = [c for c in COLUMNS.values() if c != "year"]
    tb = tb.dropna(subset=value_cols, how="all")

    # Reshaping a header-less sheet can drop a column's origin; every value column comes from
    # this snapshot, so restore the origin explicitly.
    for col in value_cols:
        tb[col].metadata.origins = [snap.metadata.origin]

    # This is a France-only source; add the country column explicitly.
    tb["country"] = "France"
    tb["country"] = tb["country"].astype("category")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
