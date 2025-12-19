"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def parse_quarters_from_row(df: pd.DataFrame, row_idx: int) -> list:
    """Parse quarter columns from header row."""
    quarters = []
    for col in df.columns[1:]:  # Skip first column
        val = df.iloc[row_idx, col]
        if pd.notna(val) and isinstance(val, str) and "Q" in val:
            quarters.append(val)
    return quarters


def extract_section(df: pd.DataFrame, start_row: int, end_row: int, quarters: list) -> pd.DataFrame:
    """Extract a section of data and convert to long format."""
    # Get the metric names from first column
    section_df = df.iloc[start_row:end_row, :].copy()
    section_df = section_df.dropna(how="all", axis=0)

    results = []

    for idx in range(len(section_df)):
        metric_name = section_df.iloc[idx, 0]
        if pd.isna(metric_name) or metric_name == "" or "(" in str(metric_name):
            continue

        # Get values for each quarter
        for col_idx, quarter in enumerate(quarters):
            value = section_df.iloc[idx, col_idx + 1]  # +1 to skip first column
            if pd.notna(value) and value != "-":
                results.append({"quarter": quarter, "metric": str(metric_name).strip(), "value": value})

    return pd.DataFrame(results)


def run() -> None:
    """Load snapshot and create meadow dataset."""
    # Load inputs.
    snap = paths.load_snapshot("operating_data.xlsx")

    # Read the quarterly sheet
    tb = snap.read_excel(sheet_name="Quarterly", engine="openpyxl", header=None)

    # Parse quarters from row 4 (0-indexed)
    quarters = parse_quarters_from_row(tb, 4)

    # Extract different sections
    # Capacity: row 7
    capacity_data = []
    for col_idx, quarter in enumerate(quarters):
        value = tb.iloc[7, col_idx + 1]
        if pd.notna(value) and value != "-":
            capacity_data.append({"quarter": quarter, "metric": "Annual Capacity", "value": value})

    # Wafer shipments: row 10
    shipments_data = []
    for col_idx, quarter in enumerate(quarters):
        value = tb.iloc[10, col_idx + 1]
        if pd.notna(value) and value != "-":
            shipments_data.append({"quarter": quarter, "metric": "Quarterly Wafer Shipments", "value": value})

    # Revenue by Technology: rows 15-28
    tech_data = extract_section(tb, 15, 29, quarters)
    tech_data["category"] = "technology"

    # Revenue by Platform: rows 37-42
    platform_data = extract_section(tb, 37, 43, quarters)
    platform_data["category"] = "platform"

    # Revenue by Geography: rows 45-49
    geo_data = extract_section(tb, 45, 50, quarters)
    geo_data["category"] = "geography"

    # Combine all data
    all_data = pd.concat(
        [
            pd.DataFrame(capacity_data),
            pd.DataFrame(shipments_data),
            tech_data,
            platform_data,
            geo_data,
        ],
        ignore_index=True,
    )

    # Add category for capacity and shipments
    all_data.loc[all_data["metric"] == "Annual Capacity", "category"] = "capacity"
    all_data.loc[all_data["metric"] == "Quarterly Wafer Shipments", "category"] = "shipments"

    # Parse quarter to year and quarter number
    all_data["year"] = all_data["quarter"].str.extract(r"(\d{2})$")[0].astype(int)
    all_data["year"] = all_data["year"].apply(lambda x: 1900 + x if x >= 94 else 2000 + x)

    all_data["quarter_num"] = all_data["quarter"].str.extract(r"(\d)Q")[0].astype(int)

    # Create a date column (first day of the quarter)
    all_data["month"] = (all_data["quarter_num"] - 1) * 3 + 1
    all_data["date"] = pd.to_datetime(all_data[["year", "month"]].assign(day=1))

    # Clean value column - remove commas, ">" symbols, "~" symbols, and convert to float
    all_data["value"] = (
        all_data["value"]
        .astype(str)
        .str.replace(",", "")
        .str.replace(">", "")
        .str.replace("~", "")
        .str.replace("%", "")
        .str.strip()
    )
    all_data["value"] = pd.to_numeric(all_data["value"], errors="coerce")
    # Drop intermediate columns
    all_data = all_data.drop(columns=["quarter_num", "month", "quarter"])

    # Create table
    tb = Table(all_data, short_name="operating_data")
    tb["value"].metadata.origins = [snap.metadata.origin]

    # Format table
    tb = tb.format(["date", "year", "category", "metric"])

    # Save outputs.
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
