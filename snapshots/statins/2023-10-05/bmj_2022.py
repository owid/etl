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
    Main function that extracts the two tables in Supplementary Figures from the following paper
    Guadamuz JS, Shooshtari A, Qato DM. Global, regional and national trends in statin utilisation in high-income and low/middle-income countries, 2015-2020. BMJ Open. 2022 Sep 8;12(9):e061350. doi: 10.1136/bmjopen-2022-061350. PMID: 36691204; PMCID: PMC9462115..

    The function:
    1. Retrieves a PDF with two supplementary files document from a specified URL.
    2. Extracts tables related to statin utilization and economic health indicators from the PDF.
    3. Merges and saves the extracted data to a CSV file.
    4. Uploads the data snapshot.

    """
    # Creating a snapshot instance to manage data versioning and metadata.
    snap = Snapshot(f"statins/{SNAPSHOT_VERSION}/bmj_2022.csv")

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
            df_econ_health = extract_economic_health_indicators(response)

            # Merging extracted tables based on the 'country' column.
            df = pd.merge(df_statins, df_econ_health, on="country")

            # Saving the merged dataframe to a file and updating the DVC.
            df_to_file(df, file_path=snap.path)
            snap.dvc_add(upload=upload)


def extract_statin_use_table(response):
    """
    Extract and process statin utilization data from a PDF.

    The function performs the following:
    - Extracts specified pages from the input PDF
    - Identifies and extracts a table of statin utilization from the pages
    - Cleans the extracted data and organizes it into a structured format
    - Converts the structured data into a Pandas DataFrame

    Parameters:
    -----------
    response : requests.Response
        The response object containing the PDF content retrieved from a previous HTTP request.

    Returns:
    --------
    df : pd.DataFrame
        A DataFrame containing the cleaned statin utilization data with the following columns:
        - 'country': country name
        - 'statin_utilization_october_2019': Statin utilization for October 2019
        - 'statin_utilization_september_2020': Statin utilization for September 2020

    Notes:
    ------
    The specific pages and data extraction logic are hardcoded based on the expected PDF structure.
    Adaptations may be necessary if the structure of the source PDF changes.
    """
    # Initialize an empty list to hold the extracted and cleaned row data.
    all_rows = []

    # Specify the pages to extract data from (1-indexed for human readability).
    page_start = 3
    page_end = 4

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
                # Discard header or irrelevant rows and extract the data rows.
                rows = table[10:]

                # Apply specific cleaning and processing logic based on the page number (original formatting is different depending on the page).
                if i == (page_start - 1):
                    # Clean and merge cell data for the first page.
                    cleaned_rows = [merge_and_clean(row) for row in rows]

                    # Retain only the relevant columns containing country name and statin utilization rates.
                    only_rates = [row[:3] for row in cleaned_rows]

                    # Append the cleaned data to the master list.
                    all_rows.extend(only_rates)
                else:
                    # Clean data for subsequent pages without merging cells.
                    cleaned_rows = [
                        [item.replace("·", ".") if isinstance(item, str) else item for item in row] for row in rows
                    ]
                    only_rates = [row[:3] for row in cleaned_rows]
                    all_rows.extend(only_rates)

    # Discard the last 8 rows from the extracted data, as they might be footnotes or unrelated content.
    rows_to_save = all_rows[:-8]

    # Convert the cleaned and processed data into a Pandas DataFrame.
    df = pd.DataFrame(
        rows_to_save, columns=["country", "statin_utilization_october_2019", "statin_utilization_september_2020"]
    )

    return df


def extract_economic_health_indicators(response):
    """
    Extract and process economic and health indicators from a PDF.

    This function performs the following:
    - Extracts specified pages from the input PDF
    - Identifies and extracts a table of economic and health indicators from the pages
    - Cleans the extracted data and organizes it into a structured format
    - Converts the structured data into a Pandas DataFrame

    Parameters:
    -----------
    response : requests.Response
        The response object containing the PDF content retrieved from a previous HTTP request.

    Returns:
    --------
    df : pd.DataFrame
        A DataFrame containing the cleaned economic and health indicators with the following columns:
        - 'country': country name
        - 'health_expenditure_per_capita_2018': Health expenditure per capita in 2018
        - 'statins_essential_medicine': Indicator if statins are considered essential medicine
        - 'ihd_mortality_rate_2019': Ischemic heart disease (IHD) mortality rate in 2019

    Notes:
    ------
    The specific pages and data extraction logic are hardcoded based on the expected PDF structure.
    Adaptations may be necessary if the structure of the source PDF changes.
    """
    # Initialize an empty list to accumulate the extracted and cleaned row data.
    all_rows = []

    # Specify the pages to extract data from (1-indexed for human readability).
    page_start = 1
    page_end = 2

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
                # Discard header or irrelevant rows and extract the data rows.
                rows = table[11:]
                # Replace non-standard decimal point notation in numbers with a standard period.
                cleaned_rows = [
                    [item.replace("·", ".") if isinstance(item, str) else item for item in row] for row in rows
                ]
                # Apply specific cleaning and processing logic based on the page number (original formatting is different depending on the page).
                if i == (page_start - 1):
                    # Specify the columns of interest for the first page.
                    index_columns = [0, 9, 12, 13]
                    columns_of_interest = [[row[i] for i in index_columns] for row in cleaned_rows]

                    # Append the cleaned data to the master list.
                    all_rows.extend(columns_of_interest)
                else:
                    # Specify the columns of interest for the second page.
                    index_columns = [0, 5, 8, 9]
                    columns_of_interest = [[row[i] for i in index_columns] for row in cleaned_rows]

                    # Append the cleaned data to the master list.
                    all_rows.extend(columns_of_interest)

    # Discard the last 11 rows from the extracted data, which might be footnotes or unrelated content.
    rows_to_save = all_rows[:-11]

    # Convert the cleaned and processed data into a Pandas DataFrame.
    df = pd.DataFrame(
        rows_to_save,
        columns=[
            "country",
            "health_expenditure_per_capita_2018",
            "statins_essential_medicine_2017",
            "ihd_mortality_rate_2019",
        ],
    )

    return df


def merge_and_clean(row):
    """
    Clean and optionally merge items within a data row extracted from a PDF table.

    The function performs the following cleaning steps:
    - Replace a specific character used for decimal points with a standard period.
    - Merge two adjacent cells under specified conditions (both cells contain numeric data).
    - Replace empty strings with None for better compatibility with Pandas DataFrames.

    Parameters:
    -----------
    row : list
        A list of items representing a row of extracted data from the PDF table. Items are assumed
        to be either strings or None.

    Returns:
    --------
    list
        A list representing the cleaned and optionally merged row of data. The list may be shorter
        than the input row if merging occurred.

    Notes:
    ------
    - The merging logic is hardcoded based on the expected PDF structure and may need adjustments if
      the structure changes.
    - Assumes the potential merging cells are in positions 3 and 4 in the input row.
    """
    # Replace non-standard decimal point notation in numbers with a standard period.
    row = [item.replace("·", ".") if isinstance(item, str) else item for item in row]

    # Check if the items in position 3 and 4 are numeric strings. If true, concatenate them and
    # store the result back in position 3, removing the original 4th item.
    if row[3] and row[4] and row[3].replace(".", "", 1).isdigit() and row[4].replace(".", "", 1).isdigit():
        row[2] = row[3] + row[4]
        row[3:5] = []

    # Replace any empty strings with None, ensuring better compatibility with Pandas operations
    # and clearer indication of missing data.
    row = [None if item == "" else item for item in row]

    return row


if __name__ == "__main__":
    main()
