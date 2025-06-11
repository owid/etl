"""Load a snapshot and create a meadow dataset.


This script extracts literacy rate data from Buringh and van Zanden's historical dataset covering
the period 1451-1800, processing the table from page 51 of the PDF.

"""

import re

import pandas as pd
import PyPDF2
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Extract and process literacy rate data from the PDF."""
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("literacy_1451_1800.pdf")

    # Read the PDF using PyPDF2
    reader = PyPDF2.PdfReader(snap.path)

    # Extract text from page 51 (0-based index 50)
    page_51 = reader.pages[50]
    text_page_51 = page_51.extract_text()

    #
    # Process data.
    #
    # Split extracted text into lines for processing
    lines = text_page_51.split("\n")

    # Initialize storage for parsed table data
    table_data = []
    countries = []

    # Parse lines to extract country names and literacy rate data
    # Look for known country patterns and extract their data
    country_patterns = [
        r"Great Britain\s+(.+)",
        r"Ireland\s+(.+)",
        r"France\s+(.+)",
        r"Belgium\s+(.+)",
        r"Netherlands\s+(.+)",
        r"Germany\s+(.+)",
        r"Italy\s+(.+)",
        r"Spain\s+(.+)",
        r"Sweden\s+(.+)",
        r"Poland\s+(.+)",
        r"Western Europe\s+(.+)",
    ]

    # Join all lines to handle potential line breaks
    full_text = " ".join(lines)

    for pattern in country_patterns:
        match = re.search(pattern, full_text, re.IGNORECASE)
        if match:
            country = pattern.replace(r"\s+(.+)", "").replace("\\", "")
            data_part = match.group(1)
            # Extract numbers and dashes from the data part
            numbers = re.findall(r"\d+|-", data_part)

            if numbers and len(numbers) >= 6:  # Ensure we have at least 6 data points
                countries.append(country)
                table_data.append(numbers[:6])  # Take first 6 values

    # Ensure all data rows have the same number of columns
    max_cols = max(len(row) for row in table_data)
    # Pad shorter rows with None values
    for row in table_data:
        while len(row) < max_cols:
            row.append(None)

    # Define column names for the time periods
    columns = ["1500", "1451-1500", "1501-1600", "1601-1700", "1701-1800", "1800"][:max_cols]

    # Create DataFrame with countries as index
    df = pd.DataFrame(table_data, columns=columns, index=countries)

    df = df.drop(columns={"1500", "1800"})  # Drop the first and last columns as this data comes from a different source
    # Reset index
    df = df.reset_index()

    # Rename the index column to 'country'
    df = df.rename(columns={"index": "country"})

    # Transform to have a column for year, country and literacy
    df = df.melt(
        id_vars=["country"],
        value_vars=[col for col in df.columns if col != "country"],
        var_name="year",
        value_name="literacy_rate",
    )
    # Convert literacy rate values to numeric, handling non-numeric entries
    df["literacy_rate"] = pd.to_numeric(df["literacy_rate"], errors="coerce")

    tb = Table(df, short_name=paths.short_name, metadata=snap.to_table_metadata())
    tb = tb.format(["country", "year"])
    # Set metadata origin for the literacy rate column
    tb["literacy_rate"].metadata.origins = [snap.metadata.origin]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset with the processed table
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset to disk
    ds_meadow.save()
