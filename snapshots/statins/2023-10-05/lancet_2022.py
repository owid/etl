"""Script to create a snapshot of dataset."""

from io import BytesIO
from pathlib import Path

import click
import pandas as pd
import pdfplumber
import requests
from owid.datautils.io import df_to_file

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    """
    Main function to fetch, extract, process, and store statin usage data.

    This function initiates a snapshot instance, fetches data from the url, extracts tables from the fetched PDF data, and stores the processed data.
    The table of satin use is Appendix 11. on p.42 in Supplementary Figures in the following paper
    Marcus ME, Manne-Goehler J, Theilmann M, Farzadfar F, Moghaddam SS, Keykhaei M, Hajebi A, Tschida S, Lemp JM, Aryal KK, Dunn M, Houehanou C, Bahendeka S, Rohloff P, Atun R, Bärnighausen TW, Geldsetzer P, Ramirez-Zea M, Chopra V, Heisler M, Davies JI, Huffman MD, Vollmer S, Flood D. Use of statins for the prevention of cardiovascular disease in 41 low-income and middle-income countries: a cross-sectional study of nationally representative, individual-level data. Lancet Glob Health. 2022 Mar;10(3):e369-e379. doi: 10.1016/S2214-109X(21)00551-9. PMID: 35180420; PMCID: PMC8896912.

    """
    # Creating a snapshot instance to manage data versioning and metadata.
    snap = Snapshot(f"statins/{SNAPSHOT_VERSION}/lancet_2022.csv")

    # Headers to emulate a web browser, mitigating the risk of being blocked by the server.
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
    }

    # Attempt to fetch data from the source URL.
    if snap.metadata.origin is not None:
        response = requests.get(snap.metadata.origin.url_download, headers=headers)

        # Proceed only if the request was successful (HTTP Status Code 200).
        if response.status_code == 200:
            # Extracting tables from the PDF response.
            df_statins = extract_statin_use_table(response)

            # Merging extracted tables based on the 'country' column.

            # Saving the merged dataframe to a file and updating the DVC.
            df_to_file(df_statins, file_path=snap.path)
            snap.dvc_add(upload=upload)


def extract_statin_use_table(response):
    """
    Extract statin usage data from a specific table within a PDF from an HTTP response.

    This function navigates to specified pages of a PDF, extracts table data, cleans it,
    and converts it to a DataFrame. It uses pdfplumber's table extraction feature
    with specified settings to ensure accurate data capture.

    Parameters:
    -----------
    response : requests.Response
        The HTTP response object containing PDF data, obtained from a GET request.

    Returns:
    --------
    pd.DataFrame
        A DataFrame containing the cleaned and processed statin usage data with columns:
        - country: country name
        - statin_use_secondary: Data related to secondary use of statins
        - statin_use_primary: Data related to primary use of statins

    """
    # Initialize an empty list to hold the extracted and cleaned row data.
    all_rows = []

    # Specify the pages to extract data from (1-indexed for human readability).
    page_start = 42
    page_end = 43

    # Define settings for table extraction using pdfplumber to ensure accurate data capture.
    table_settings = {"vertical_strategy": "text", "horizontal_strategy": "text"}

    # Open the PDF contained in the response and iterate through the specified pages.
    with pdfplumber.open(BytesIO(response.content)) as pdf:
        for i in [page_start - 1, page_end - 1]:  # Adjusting to 0-indexed pages for iteration.
            # Extract the page.
            page = pdf.pages[i]

            # Extract the table from the page using the predefined settings.
            table = page.extract_table(table_settings)
            if table is not None:
                # Append the cleaned data to the master list (exclude headers)
                all_rows.extend(table[4:])

    # Convert the cleaned and processed data into a Pandas DataFrame.
    df = pd.DataFrame(all_rows, columns=["country", "statin_use_secondary", "statin_use_primary"])
    return df


if __name__ == "__main__":
    main()
