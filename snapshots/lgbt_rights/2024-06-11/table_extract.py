"""
Extract Table 1 from Appendix in https://hal.science/hal-03778162v1/document, which is a PDF document. It generates a CSV file with the extracted table, without headers.
The table is in pages 24 to 27.

The table has the following columns:
- Territory
- Last decriminalization
- Last criminalization
- Former criminalization(s)
- Former criminalization(s)

The table will probably not be updated, but in that case you need to follow these steps:
    1. Download the latest version from https://hal.science/hal-03778162.
    2. Copy the file to this directory. Rename it to "Decriminalizing homosexuality.pdf", in case it has a different name.
    3. Change the pages in the function extract table_from_pdf I call in the run function.
    4. Run this script.
    5. Run the snapshot:
        python snapshots/lgbt_rights/{version}/criminalization_mignot.py --path-to-file snapshots/lgbt_rights/{version}/table1.csv
"""

from pathlib import Path

import pandas as pd
import pdfplumber

PARENT_DIR = str(Path(__file__).parent.absolute())
FILE_PATH = f"{PARENT_DIR}/Decriminalizing homosexuality.pdf"


def run():
    df = extract_table_from_pdf(FILE_PATH, 23, 27)
    df.to_csv(f"{PARENT_DIR}/table1.csv", index=False)


def extract_table_from_pdf(pdf_path: str, start_page: int, end_page: int):
    """
    Extract table from a PDF file within the specified page range.

    Args:
        pdf_path (str): The path to the PDF file.
        start_page (int): The starting page (inclusive).
        end_page (int): The ending page (inclusive).

    Returns:
        pd.DataFrame: The extracted table.

    """
    with pdfplumber.open(pdf_path) as pdf:
        table = []
        for i, page in enumerate(pdf.pages):
            if i < start_page:
                continue
            if i > end_page:
                break
            extracted_table = page.extract_table()
            if extracted_table:
                table.extend(extracted_table[2:])  # Exclude the first 3 rows of each page
    return pd.DataFrame(table[1:], columns=table[0])


if __name__ == "__main__":
    run()
