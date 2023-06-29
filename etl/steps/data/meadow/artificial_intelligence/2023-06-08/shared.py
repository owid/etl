import pandas as pd
import pdfplumber
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()


def load_data(snap: Snapshot):
    """
    Load the Excel file from the given snapshot.

    Args:
        snap (Snapshot): The snapshot object containing the path to the Excel file.

    Returns:
        pd.ExcelFile: The loaded Excel file as a pandas ExcelFile object, or None if loading failed.
    """

    # Attempt to load the Excel file from the snapshot path.
    try:
        excel_object = pd.ExcelFile(snap.path)
    except FileNotFoundError:
        raise FileNotFoundError(f"Excel file not found at path: {snap.path}")
    except IsADirectoryError:
        raise IsADirectoryError(f"Provided path is a directory, not an Excel file: {snap.path}")
    except Exception as e:
        raise Exception(f"An error occurred while loading the Excel file: {e}")

    # Return the loaded Excel file as a pandas ExcelFile object.
    return excel_object


def process_data(excel_object: pd.ExcelFile):
    """
    Process sheets in the given Excel file and return a combined DataFrame.

    Args:
        excel_object (pd.ExcelFile): The loaded Excel file to process.
    Returns:
        pd.DataFrame: The combined and processed DataFrame from all sheets.
    """
    sheet_names = excel_object.sheet_names
    data_frames = []
    # Iterate through the matched sheet names and process each sheet
    for i, sheet_name in enumerate(sheet_names):
        log.info(f"Processing sheet: {sheet_name}")
        df = pd.read_excel(excel_object, sheet_name=sheet_name)
        question = df.columns[0]  # Extract the question that was asked
        melted_df = df.melt(
            id_vars=question, var_name="Date", value_name="Value"
        )  # Melt date columns into one called 'Date'
        filtered_df = melted_df[~melted_df[question].isin(["Unweighted base", "Base"])]  # Exclude sample sizes
        filtered_df = filtered_df.assign(Value=filtered_df["Value"] * 100, Group=sheet_name)  # Convert to percentages

        data_frames.append(filtered_df)

    # Concatenate all the processed DataFrames
    df_concat = pd.concat(data_frames, axis=0)
    df_concat.reset_index(drop=True, inplace=True)
    return df_concat


def read_table_from_pdf(pdf_path, page_number: int) -> pd.DataFrame:
    """
    Read a table from a PDF file and convert it into a DataFrame.

    Args:
        pdf_path (str): The path to the PDF file.
        page_number (int): The page number containing the table (1-indexed).

    Returns:
        pd.DataFrame: The extracted table as a DataFrame.
    """
    table_settings = {"vertical_strategy": "text", "horizontal_strategy": "text"}
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[page_number - 1]  # Adjust page number to 0-indexed
        table = page.extract_tables(table_settings=table_settings)[0]  # Extract the first table with custom settings

        # Convert the table data into a DataFrame
        df = pd.DataFrame(table[1::], columns=table[0])

        return df
