"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Load IEA Energy and AI snapshot and create a meadow dataset."""
    paths.log.info("energy_ai_iea.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("energy_ai_iea.xlsx")

    # Process Regional Data sheet
    df_regional = process_regional_data(snap)

    tb = Table(pd.DataFrame(df_regional), short_name="energy_ai_iea")

    # Set index - year, country, and metric form the primary key
    tb = tb.format(["country", "year", "metric", "scenario"])
    # Add metadata
    for col in tb.columns:
        tb[col].metadata.origins = [snap.metadata.origin]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

    paths.log.info("energy_ai_iea.end")


def process_regional_data(snap) -> pd.DataFrame:
    """Process Regional Data sheet into long format."""
    # Constants
    YEAR_ROW = 3
    YEAR_COLS = slice(2, 7)
    DATA_START_ROW = 4
    VALUE_COL = 1
    VALUE_START_COL = 2
    PROJECTION_YEAR = 2030

    METRIC_HEADERS = {
        "Total installed capacity (GW)",
        "Total electricity consumption (TWh)",
    }

    # All possible metric headers in the sheet (to detect when we enter a new section)
    ALL_METRIC_HEADERS = {
        "Total installed capacity (GW)",
        "IT installed capacity (GW)",
        "Power usage effectiveness",
        "Load factor (%)",
        "Total electricity consumption (TWh)",
        "IT electricity consumption (TWh)",
    }

    df = snap.read(sheet_name="Regional Data", header=None, safe_types=False)

    # Extract year headers (row 3: 2020, 2023, 2024, 2030)
    years = df.iloc[YEAR_ROW, YEAR_COLS].values
    years = [int(y) if pd.notna(y) and y != 0 else None for y in years]

    data_rows = []
    current_metric = None

    for idx in range(DATA_START_ROW, len(df)):
        row = df.iloc[idx]
        cell_value = row[VALUE_COL]

        if pd.isna(cell_value):
            continue

        cell_str = str(cell_value)

        # Check if this row is any metric header
        if cell_str in ALL_METRIC_HEADERS:
            # Only set current_metric if it's one we want to keep
            current_metric = cell_str if cell_str in METRIC_HEADERS else None
            continue

        # Skip if we haven't found a metric yet or if cell is invalid
        if current_metric is None or cell_str == "nan":
            continue

        country = cell_str

        # Extract values for each year
        for i, year in enumerate(years):
            if year is None:
                continue

            value = row[VALUE_START_COL + i]
            if pd.notna(value) and value != 0:
                scenario = "base" if year == PROJECTION_YEAR else "historical"
                data_rows.append({
                    "year": year,
                    "country": country,
                    "metric": current_metric,
                    "value": value,
                    "scenario": scenario,
                })

    return pd.DataFrame(data_rows)
