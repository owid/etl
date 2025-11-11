"""Load a snapshot and create a meadow dataset."""

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("wsts_historical_billings.xlsx")

    # Load data from snapshot - Monthly Data sheet
    tb_monthly = snap.read(sheet_name="Monthly Data", header=None)
    # Load data from snapshot - 3MMA sheet
    tb_3mma = snap.read(sheet_name="3MMA", header=None)

    #
    # Process data.
    #
    # Process monthly data
    tb_monthly = process_monthly_data(tb_monthly)
    tb_monthly = Table(tb_monthly, metadata=snap.to_table_metadata())
    tb_monthly.metadata.short_name = "wsts_historical_billings_monthly"
    # Process 3-month moving average data
    tb_3mma = process_3mma_data(tb_3mma)
    tb_3mma = Table(tb_3mma, metadata=snap.to_table_metadata())
    tb_3mma.metadata.short_name = "wsts_historical_billings_3mma"

    # Ensure metadata is correctly associated for both tables
    for tb in [tb_monthly, tb_3mma]:
        for column in tb.columns:
            tb[column].metadata.origins = [snap.metadata.origin]

    # Format tables
    tb_monthly = tb_monthly.format(["region", "year", "period"])
    tb_3mma = tb_3mma.format(["region", "year", "month"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=[tb_monthly, tb_3mma], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def process_monthly_data(tb: Table) -> Table:
    """
    Process the monthly data sheet from WSTS Historical Billings Report.

    The sheet contains:
    - Row 3: Column headers (January through December, Total Year, Q1-Q4)
    - Data starts at row 4 with year followed by regional data
    - Format: Year row, then 5 region rows (Americas/Europe/Japan/Asia Pacific/Worldwide), repeat
    """
    # Extract column headers from row 3 (index 3)
    # headers[0:12] = months (Jan-Dec)
    # headers[12] = 'Total Year'
    # headers[13:17] = quarters (Q1-Q4)
    headers = tb.iloc[3, 1:].tolist()

    # Create list to store processed data
    data_rows = []

    # Process data starting from row 4
    current_year = None
    for idx in range(4, len(tb)):
        row = tb.iloc[idx]
        first_col = row[0]

        # Check if this is a year row (could be string or int/float)
        if pd.notna(first_col):
            try:
                year_val = int(first_col) if isinstance(first_col, str) else first_col
                if isinstance(year_val, (int, float)) and year_val >= 1986:
                    current_year = int(year_val)
                    continue
            except (ValueError, TypeError):
                pass

        # Check if this is a region row
        if pd.notna(first_col) and isinstance(first_col, str) and current_year is not None:
            # This is a region row
            region = first_col.strip()

            # Extract monthly values (columns 1-12, corresponding to headers[0:12])
            for i in range(12):
                month = headers[i]
                value = row[i + 1]  # Column offset: row[1] = January, row[2] = February, etc.
                if pd.notna(value):
                    data_rows.append(
                        {
                            "year": current_year,
                            "region": region,
                            "period": month,
                            "period_type": "monthly",
                            "value": int(value) * 1000,
                        }
                    )

            # Extract Total Year value (column 13, headers[12])
            value = row[13]
            if pd.notna(value):
                data_rows.append(
                    {
                        "year": current_year,
                        "region": region,
                        "period": "Total Year",
                        "period_type": "yearly",
                        "value": int(value) * 1000,
                    }
                )

    return Table(pd.DataFrame(data_rows))


def process_3mma_data(tb: Table) -> Table:
    """
    Process the 3-month moving average data sheet from WSTS Historical Billings Report.

    The sheet contains:
    - Row 3: Column headers (January through December)
    - Data starts at row 4 with year followed by regional data
    - Format: Year row, then 5 region rows (Americas/Europe/Japan/Asia Pacific/Worldwide), repeat
    - Values are 3-month moving averages (can be decimals)
    """
    # Extract column headers from row 3 (index 3)
    # headers[0:12] = months (Jan-Dec)
    headers = tb.iloc[3, 1:].tolist()

    # Create list to store processed data
    data_rows = []

    # Process data starting from row 4
    current_year = None
    for idx in range(4, len(tb)):
        row = tb.iloc[idx]
        first_col = row[0]

        # Check if this is a year row (could be string or int/float)
        if pd.notna(first_col):
            try:
                year_val = int(first_col) if isinstance(first_col, str) else first_col
                if isinstance(year_val, (int, float)) and year_val >= 1986:
                    current_year = int(year_val)
                    continue
            except (ValueError, TypeError):
                pass

        # Check if this is a region row
        if pd.notna(first_col) and isinstance(first_col, str) and current_year is not None:
            # This is a region row
            region = first_col.strip()

            # Extract 3MMA values for each month (columns 1-12, corresponding to headers[0:12])
            for i in range(12):
                month = headers[i]
                if pd.notna(month):  # Only process valid month headers
                    value = row[i + 1]  # Column offset: row[1] = January, row[2] = February, etc.
                    if pd.notna(int(float(value))) and int(float(value)) != 0:
                        data_rows.append(
                            {
                                "year": current_year,
                                "region": region,
                                "month": month,
                                "value_3mma": int(float(value)) * 1000,
                            },
                        )

    return Table(pd.DataFrame(data_rows))
