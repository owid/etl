"""Load a snapshot and create a meadow dataset."""

import pandas as pd
import pdfplumber
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("geyer_2017.pdf")

    # Define the path to your file
    file_path = snap.path

    # Define table settings
    table_settings = {"vertical_strategy": "text", "horizontal_strategy": "text"}
    #
    # Process data.
    #

    # Initialize an empty list to hold the table rows
    all_rows = []
    page_start = 7
    page_end = 8

    # Open the PDF file
    with pdfplumber.open(file_path) as pdf:
        for i in [page_start - 1, page_end - 1]:  # Iterate over pages 7 and 8 (0-indexed)
            # Extract the page
            page = pdf.pages[i]

            # Extract the first table with custom settings
            table = page.extract_table(table_settings)

            if i == 7 - 1:  # If it's the first page (7), include headers
                all_rows.extend(table)  # type: ignore
            else:  # If it's the subsequent pages, exclude headers
                all_rows.extend(table[1:])  # type: ignore

    # Convert the table data into a DataFrame
    df = pd.DataFrame(all_rows[3:], columns=all_rows[0])
    df["country"] = "World"
    df = df.rename(columns={"Year Global": "year", "Prod": "plastic_production"})
    tb = Table(df, short_name=paths.short_name, underscore=True)
    tb["plastic_production"].metadata.origins = [snap.metadata.origin]

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
