"""Load a snapshot and create a meadow dataset."""

import pdfplumber

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("bmj_2022.pdf")

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

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
