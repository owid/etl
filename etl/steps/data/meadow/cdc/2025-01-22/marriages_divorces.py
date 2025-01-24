"""Load a snapshot and create a meadow dataset."""

import re

import pandas as pd
import pdfplumber
import pytesseract
import tesserocr
from owid.catalog.tables import Table
from pdf2image import convert_from_path
from PIL import Image

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
    # Open the PDF and extract the text from the relevant page
    with pdfplumber.open(snap.path) as pdf:
        text = pdf.pages[26].extract_text()

    # Convert PDF to a list of PIL images (one image per page)
    pages = convert_from_path(snap.path, dpi=300)  # Increase dpi for better OCR accuracy
    # We'll just process the first page as an example.
    # Perform OCR on the 27th page of the PDF
    page_27_image = pages[26]  # Page numbers are zero-indexed
    ocr_data_page_27 = pytesseract.image_to_data(page_27_image, output_type=pytesseract.Output.DATAFRAME)

    # Clean the OCR DataFrame for page 27
    ocr_data_page_27 = ocr_data_page_27.dropna(subset=["text"])  # Remove rows with no detected text
    ocr_data_page_27 = ocr_data_page_27[ocr_data_page_27["text"].str.strip() != ""]  # Remove empty text entries

    # Display the first few rows of the OCR data for page 27 for inspection
    ocr_data_page_27.head()
    print(ocr_data_page_27["text"].values)

    # Drop a
    rows = re.findall(r"([^\n]+)", text)  # Split rows by newlines
    # Regex to match rows with year, marriage/divorce numbers and rates
    pattern = re.compile(r"(\d{4})[-\s]+(\d{1,4},?\d{3})\s(\d+\.\d+)\s(\d{1,4},?\d{3})\s(\d+\.\d+)")

    # Parse rows and log unprocessed ones
    strict_parsed_rows = []
    unprocessed_rows = []

    for row in rows:
        match = re.search(pattern, row)
        if match:
            strict_parsed_rows.append(
                {
                    "year": match.group(1),
                    "marriage_number": match.group(2),
                    "marriage_rate": match.group(3),
                    "divorce_number": match.group(4),
                    "divorce_rate": match.group(5),
                }
            )
        else:
            unprocessed_rows.append(row)
    print(unprocessed_rows)
    # Create DataFrame
    df = pd.DataFrame(strict_parsed_rows)
    # Add country column
    df["country"] = "United States"
    print(len(df))

    # Sort by year

    tb = Table(df, underscore=False)
    for col in tb.columns:
        tb[col].metadata.origins = origins
    tb = tb.format(["country", "year"], short_name=paths.short_name)
    print(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the original dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
