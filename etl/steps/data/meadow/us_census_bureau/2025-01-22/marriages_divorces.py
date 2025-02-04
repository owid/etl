"""Load a snapshot and create a meadow dataset."""

import re

import pandas as pd
import pdfplumber
from owid.catalog.tables import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("marriages_divorces.pdf")
    origins = [snap.metadata.origin]

    #
    # Load data and process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    with pdfplumber.open(snap.path) as pdf:
        text = pdf.pages[10].extract_text()
        # Extracting rows related to "Marriage" and "Divorce" rates in the text

        # Isolating relevant lines for the marriage and divorce table
        relevant_lines = re.findall(r"(\d{4})\s+__\s+(\d+\.\d+)\s+(\d+\.\d+)", text)

        # Creating a DataFrame from the extracted data
        columns = ["year", "marriage_rate", "divorce_rate"]
        data = pd.DataFrame(relevant_lines, columns=columns)

        # Converting data types to appropriate formats
        data["year"] = data["year"].astype(int)
        data["marriage_rate"] = data["marriage_rate"].astype(float)
        data["divorce_rate"] = data["divorce_rate"].astype(float)
        data["country"] = "United States"

    tb = Table(data, underscore=False)
    for col in tb.columns:
        tb[col].metadata.origins = origins

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
