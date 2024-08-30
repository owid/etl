"""
Extract Table A2 from Appendix A in https://onlinelibrary.wiley.com/doi/ftr/10.1111/roiw.12453, which is a PDF document. It generates a CSV file with the extracted table.
The table is in page 2.

The table will probably not be updated, but in that case you need to follow these steps:
    1. Download the latest version from https://onlinelibrary.wiley.com/action/downloadSupplement?doi=10.1111%2Froiw.12453&file=roiw12453-sup-0001-AppendixA.pdf.
    2. Copy the file to this directory.
    3. Change the pages in the function extract table_from_pdf I call in the run function.
    5. Clean the CSV file by keeping these columns: year,top_0_1,top_0_5,top_1
    6. Run this script.
    7. Run the snapshot:
        python snapshots/chartbook/{version}/davies_di_matteo_2020_canada.py --path-to-file snapshots/chartbook/{version}/davies_di_matteo_2020.csv
"""

from pathlib import Path

import pandas as pd
import pdfplumber

PARENT_DIR = str(Path(__file__).parent.absolute())
FILE_PATH = f"{PARENT_DIR}/roiw12453-sup-0001-appendixa.pdf"


def run():
    df = extract_table_from_pdf(FILE_PATH, 1, 1)
    df.to_csv(f"{PARENT_DIR}/davies_di_matteo_2020.csv", index=False)


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
