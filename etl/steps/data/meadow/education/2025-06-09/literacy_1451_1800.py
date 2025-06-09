"""Load a snapshot and create a meadow dataset.

This script extracts literacy rate data from historical dataset covering
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
    """Extract and process literacy rate data the PDF."""
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
    for line in lines:
        line = line.strip()
        if line and not line.startswith("Allen") and not line.startswith("1500") and not line.startswith("1600"):
            # Split by multiple spaces to separate country name from numerical data
            parts = re.split(r"\s{2,}", line)
            if len(parts) > 1:
                country = parts[0].strip()
                # Extract all numbers and dashes from the remaining parts
                numbers = []
                for part in parts[1:]:
                    # Find all digits and dash characters
                    nums = re.findall(r"\d+|-", part)
                    numbers.extend(nums)

                # Only add rows that have both country name and data
                if country and numbers:
                    countries.append(country)
                    table_data.append(numbers)

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
    # Remove first row (which might be header data) and reset index
    df = df.iloc[1:].reset_index()

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
