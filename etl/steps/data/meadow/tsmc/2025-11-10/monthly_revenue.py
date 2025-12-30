"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Load snapshot and create meadow dataset."""
    # Load inputs.
    snap = paths.load_snapshot("monthly_revenue.xlsx")

    # Read the consolidated sheet from Excel file
    tb = snap.read(sheet_name="Consolidated", engine="openpyxl", header=None)

    # Process the data
    # The header is in row 6 (0-indexed), data starts from row 7
    # Skip first 7 rows and set column names manually
    tb = tb.iloc[7:].reset_index(drop=True)

    # Set proper column names
    columns = ["year", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "total"]
    tb.columns = columns
    # Remove rows with NaN year
    tb = tb.dropna(subset=["year"])

    # Filter out any note rows
    tb = tb[tb["year"].astype(str).str.match(r"^\d{4}$", na=False)]

    # Convert year to integer
    tb["year"] = tb["year"].astype(int)

    # Convert all revenue columns to float
    for col in columns[1:]:
        tb[col] = pd.to_numeric(tb[col], errors="coerce")

    # Create a long format table for better analysis
    # Keep the original wide format as one table
    tb_wide = tb.copy()
    tb_wide = tb_wide[["year", "total"]]
    tb_wide = tb_wide.rename(columns={"total": "revenue"})

    # Create long format: year, month, revenue
    tb_long = tb.melt(
        id_vars=["year"],
        value_vars=["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"],
        var_name="month",
        value_name="revenue",
    )

    # Remove rows with NaN revenue
    tb_long = tb_long.dropna(subset=["revenue"])

    # Map month names to numbers
    month_map = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }
    tb_long["month"] = tb_long["month"].map(month_map)

    # Create a date column
    tb_long["date"] = pd.to_datetime(tb_long[["year", "month"]].assign(day=1))

    tb_long = tb_long.drop(columns=["year", "month"])

    tb_long["revenue"] = tb_long["revenue"] * 1_000_000  # Convert from millions to actual value
    tb_wide["revenue"] = tb_wide["revenue"] * 1_000_000  # Convert from millions to actual value

    tb_long["revenue"].metadata.origins = [snap.metadata.origin]
    tb_wide["revenue"].metadata.origins = [snap.metadata.origin]

    # Ensure all columns are snake-case, set short_name, and sort conveniently.
    tb_wide = tb_wide.format(["year"], short_name="tsmc_yearly_revenue")
    tb_long = tb_long.format(["date"], short_name="tsmc_monthly_revenue")

    # Save outputs.
    ds_meadow = paths.create_dataset(tables=[tb_wide, tb_long], check_variables_metadata=True)
    ds_meadow.save()
