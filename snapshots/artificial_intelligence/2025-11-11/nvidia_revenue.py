"""Script to create a snapshot of NVIDIA quarterly revenue data."""

import io
from pathlib import Path

import click
import pandas as pd
import pdfplumber
import requests
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# URLs for NVIDIA quarterly revenue PDFs
PDF_URLS = {
    "Q4FY25": "https://s201.q4cdn.com/141608511/files/doc_financials/2025/Q425/Rev_by_Mkt_Qtrly_Trend_Q425.pdf",
    "Q4FY24": "https://s201.q4cdn.com/141608511/files/doc_financials/2024/Q4FY24/Rev_by_Mkt_Qtrly_Trend_Q424.pdf",
    "Q4FY23": "https://s201.q4cdn.com/141608511/files/doc_financials/2023/Q423/Q423-Qtrly-Revenue-by-Market-slide.pdf",
    "Q4FY22": "https://s201.q4cdn.com/141608511/files/doc_financials/2022/q4/Rev_by_Mkt_Qtrly_Trend_Q422.pdf",
    "Q4FY21": "https://s201.q4cdn.com/141608511/files/doc_financials/annual/2021/Rev_by_Mkt_Qtrly_Trend_Q421.pdf",
    "Q4FY20": "https://s201.q4cdn.com/141608511/files/doc_financials/quarterly_reports/2020/Q420/Rev_by_Mkt_Qtrly_Trend_Q420.pdf",
    "Q4FY19": "https://s201.q4cdn.com/141608511/files/doc_financials/quarterly_reports/2019/Q419/Rev_by_Mkt_Qtrly_Trend_Q419.pdf",
    "Q4FY18": "https://s201.q4cdn.com/141608511/files/doc_financials/quarterly_reports/2018/Rev_by_Mkt_Qtrly_Trend_Q418.pdf",
    "Q4FY17": "https://s201.q4cdn.com/141608511/files/doc_financials/quarterly_reports/2017/Rev_by_Mkt_Qtrly_Trend_Q417.pdf",
    "Q4FY16": "https://s201.q4cdn.com/141608511/files/doc_financials/quarterly_reports/2016/Rev_by_Mkt_Qtrly_Trend_Q416.FINAL.pdf",
}


def download_pdf(url: str) -> bytes:
    """Download PDF from URL and return bytes."""
    log.info("downloading_pdf", url=url)
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.content


def extract_table_from_pdf(pdf_bytes: bytes, quarter: str) -> pd.DataFrame | None:
    """Extract revenue table from PDF using pdfplumber."""
    log.info("extracting_table", quarter=quarter)

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            # Try to extract tables from first page
            page = pdf.pages[0]
            tables = page.extract_tables()

            if not tables:
                log.warning("no_tables_found", quarter=quarter)
                return None

            # Select the best table
            # Some PDFs have multiple tables - pick the one that looks like revenue data
            table = None
            for candidate_table in tables:
                if len(candidate_table) > 5:  # Should have multiple segments
                    header = candidate_table[0]
                    # Check if header contains quarter information (Q1, Q2, Q3, Q4)
                    header_str = str(header)
                    if any(q in header_str for q in ["Q1", "Q2", "Q3", "Q4"]):
                        # Check that it's a properly structured table (not all in one cell)
                        if len(header) > 2:  # Should have multiple columns
                            table = candidate_table
                            break

            if table is None:
                # Fallback to first table if no good candidate found
                log.warning("using_first_table_as_fallback", quarter=quarter)
                table = tables[0]

            # Fix concatenated values in older PDFs
            # Some PDFs have all values in the second column as a single string
            # Check if second column contains None values (indicating concatenation issue)
            if len(table) > 1 and len(table[1]) > 1:
                # Check if most cells in the first data row are None
                first_data_row = table[1]
                none_count = sum(1 for cell in first_data_row[1:] if cell is None)

                # If more than half of the data cells are None, we have concatenation
                if none_count > len(first_data_row) / 2:
                    log.info("fixing_concatenated_values", quarter=quarter)
                    fixed_table = [table[0]]  # Keep header row

                    for row in table[1:]:
                        if row[1] and isinstance(row[1], str):
                            # Split concatenated values
                            # Remove $ and commas, then split by whitespace
                            values_str = row[1].replace("$", "").replace(",", "").strip()
                            values = values_str.split()

                            # Reconstruct row: [segment_name, value1, value2, ...]
                            fixed_row = [row[0]] + values
                            fixed_table.append(fixed_row)
                        else:
                            fixed_table.append(row)

                    table = fixed_table

            # Convert to DataFrame
            df = pd.DataFrame(table[1:], columns=table[0])

            # Fix Q4FY21 issue: segment names are None for some rows
            # Use text extraction as backup to get segment names
            if df.iloc[:, 0].isna().any():
                log.info("fixing_missing_segment_names", quarter=quarter)
                text = page.extract_text()
                # Known segments in order
                segments = ["Gaming", "Professional Visualization", "Data Center", "Auto", "OEM & Other", "TOTAL"]
                segment_idx = 0
                for i, row in df.iterrows():
                    if pd.isna(row.iloc[0]) and segment_idx < len(segments):
                        # Find which segment this should be by checking the text
                        for seg in segments[segment_idx:]:
                            if seg in text:
                                df.iloc[i, 0] = seg
                                segment_idx = segments.index(seg) + 1
                                break

            return df

    except Exception as e:
        log.error("extraction_failed", quarter=quarter, error=str(e))
        return None


def clean_revenue_value(value: str | float) -> float | None:
    """Clean revenue value string and convert to float."""
    if pd.isna(value) or value == "" or value is None:
        return None

    # If already a number, return it
    if isinstance(value, (int, float)):
        return float(value)

    # Remove $ and commas, handle empty strings
    value = str(value).strip().replace("$", "").replace(",", "").replace(" ", "")

    if value == "" or value == "-" or value == "—":
        return None

    try:
        return float(value)
    except ValueError:
        return None


def transform_to_long_format(df: pd.DataFrame) -> pd.DataFrame:
    """Transform wide format table to long format with date, segment, revenue columns."""
    # First column is the segment name
    segment_col = df.columns[0]

    # Rest of columns are quarters
    quarter_cols = [col for col in df.columns if col != segment_col and col != "source_quarter"]

    records = []
    for _, row in df.iterrows():
        segment = row[segment_col]

        # Skip rows with NaN or non-string segments
        if pd.isna(segment) or not isinstance(segment, str):
            continue

        # Clean up segment name
        segment = segment.replace("\n", " ").strip()

        # Skip empty segments
        if not segment:
            continue

        for quarter_col in quarter_cols:
            revenue_str = row[quarter_col]
            revenue = clean_revenue_value(revenue_str)

            if revenue is not None:
                records.append(
                    {
                        "quarter": quarter_col,
                        "segment": segment,
                        "revenue_millions": revenue,
                        "source_quarter": row.get("source_quarter", ""),
                    }
                )

    return pd.DataFrame(records)


def extract_nvidia_revenue() -> pd.DataFrame:
    """Extract NVIDIA quarterly revenue data from all PDFs."""
    log.info("starting_extraction", total_pdfs=len(PDF_URLS))

    all_data = []

    for quarter, url in PDF_URLS.items():
        try:
            # Download PDF
            pdf_bytes = download_pdf(url)

            # Extract table
            df = extract_table_from_pdf(pdf_bytes, quarter)

            if df is not None:
                # Add source quarter column
                df["source_quarter"] = quarter
                all_data.append(df)

        except Exception as e:
            log.error("processing_failed", quarter=quarter, error=str(e))
            continue

    if not all_data:
        raise ValueError("No data extracted from PDFs")

    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)

    # Transform to long format
    log.info("transforming_to_long_format")
    long_df = transform_to_long_format(combined_df)

    q_info = long_df["quarter"].str.extract(r"Q(?P<q>\d)\s+FY(?P<y>\d{2})")

    # Build a PeriodIndex from year + quarter
    years = 2000 + q_info["y"].astype(int)  # "24" -> 2024, "25" -> 2025
    quarters = q_info["q"].astype(int)

    long_df["date"] = pd.PeriodIndex(
        year=years,
        quarter=quarters,
        freq="Q",  # use "Q-MAR" or similar if your FY ends in March, etc.
    ).to_timestamp()

    # Normalize segment names
    log.info("normalizing_segment_names")
    segment_mapping = {
        "Datacenter": "Data Center",
        "Total": "TOTAL",
        "OEM & IP": "OEM & Other",
    }
    long_df["segment"] = long_df["segment"].replace(segment_mapping)

    # Remove duplicates - keep the one from the most recent source quarter
    log.info("removing_duplicates")
    long_df = long_df.sort_values(["date", "segment", "source_quarter"], ascending=[True, True, False])
    long_df = long_df.drop_duplicates(subset=["date", "segment"], keep="first")

    # Sort by date and segment, and drop source_quarter column
    long_df = long_df.sort_values(["date", "segment"]).reset_index(drop=True)
    long_df = long_df.drop(columns=["source_quarter"])

    log.info("extraction_complete", rows=len(long_df), date_range=f"{long_df['date'].min()} to {long_df['date'].max()}")

    return long_df


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    """Create snapshot of NVIDIA quarterly revenue data."""
    # Create a new snapshot.
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/nvidia_revenue.csv")

    # Extract data from PDFs
    df = extract_nvidia_revenue()

    # Save the data to the snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()
