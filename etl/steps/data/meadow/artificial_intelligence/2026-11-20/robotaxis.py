"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

NUMERIC_COLS = ["TotalTrips", "TotalPassengersCarried", "TotalPMT"]


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("robotaxis.csv")

    # Load data from snapshot.
    tb = snap.read(low_memory=False)

    #
    # Process data.
    #
    # Parse the value columns. Recent files write numbers with thousands separators ("1,528,823").
    for col in NUMERIC_COLS:
        blank = tb[col].isna() | tb[col].astype("string").str.strip().eq("")
        tb[col] = pr.to_numeric(
            tb[col].replace(",", "", regex=True).replace(r"^\s*$", None, regex=True), errors="coerce"
        )
        # Anything non-blank that fails to parse is an unexpected token (e.g. a redaction marker).
        unparsed = tb[col].isna() & ~blank
        assert not unparsed.any(), f"Non-numeric values in {col} (possible redactions) — investigate before publishing."

    # Drop rows the carriers left blank on every metric (e.g. Cruise's months after its suspension).
    tb = tb.dropna(subset=NUMERIC_COLS, how="all")

    # A row with only some metrics filled would turn into a silent zero in garden's sum — fail instead.
    partial = tb[NUMERIC_COLS].isna().any(axis=1)
    assert not partial.any(), f"Rows with only some metrics reported:\n{tb[partial]}"
    assert tb[["Year", "Month", "TCPID"]].notna().all().all(), "Rows with values but no Year, Month or TCPID."

    # Drop rows that are completely identical across all columns.
    tb = tb.drop_duplicates()

    # Create a date column from year and month (last day of the month).
    first_of_month = tb["Year"].astype(int) * 10000 + tb["Month"].astype(int) * 100 + 1
    tb["date"] = pr.to_datetime(first_of_month.astype(str), format="%Y%m%d") + pd.offsets.MonthEnd(1)
    tb = tb.rename(columns={"TCPID": "tcpid"})

    # Keep per-carrier rows so values can be verified before aggregation in garden.
    tb = tb[["tcpid", "date"] + NUMERIC_COLS]
    tables = [tb.format(["tcpid", "date"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
