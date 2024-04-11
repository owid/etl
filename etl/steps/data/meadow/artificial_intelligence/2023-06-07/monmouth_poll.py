"""Load a snapshot and create a meadow dataset."""

import re
from typing import Optional, cast

import numpy as np
import pandas as pd
import pdfplumber
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("monmouth_poll.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("monmouth_poll.pdf"))

    # Load data from snapshot.
    start_page = 3  # tables of interest start on page 3
    end_page = 6  # and end on page 6
    texts, dataframes = extract_text_and_tables_from_pdf(str(snap.path), start_page, end_page)
    #
    # Process data.
    #
    # Remove unnecessary strings after extracting survey questions from the pdf (e.g., numbers)
    texts = remove_strings(texts)

    #  Preprocesses a list of dataframes ('None' values with NaNs, dropping rows with NaN values, dropping rows with '(n)', adding the 'question' column, and getting rid off the % sign
    preprocessed_dataframes = preprocess_data(dataframes, texts)

    # Combine all dataframes into a single dataframe
    combined_df = pd.concat(preprocessed_dataframes, ignore_index=True)

    # Reset the index
    combined_df.reset_index(drop=True, inplace=True)

    # Pivot to have a column for each question
    pivot_df = combined_df.pivot(index=["answer", "year"], columns="question", values="percentage").reset_index()

    # Create a new table and ensure all columns are snake-case.
    tb = Table(pivot_df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("monmouth_poll.end")


def extract_text_and_tables_from_pdf(pdf_path: str, start_page: Optional[int] = None, end_page: Optional[int] = None):
    """
    Extract survey questions and tables from a PDF file within the specified page range.

    Args:
        pdf_path (str): The path to the PDF file.
        start_page (int): The starting page (inclusive). Default is None, which starts from the first page.
        end_page (int): The ending page (inclusive). Default is None, which extracts until the last page.

    Returns:
        tuple: A tuple containing a list of extracted texts (survey questions) and a list of dataframes.

    """
    with pdfplumber.open(pdf_path) as pdf:
        tables = []
        texts = []

        for i, page in enumerate(pdf.pages):
            # Check if the current page is within the specified range
            if start_page is not None and i < start_page:
                continue
            if end_page is not None and i > end_page:
                break

            # Extract text from the pages of interest
            text = page.extract_text()
            # Extract tables from the pages of interest
            extracted_tables = page.extract_tables()
            for table in extracted_tables:
                tables.append(table)

            # Extract lines starting with specific numbers or phrases that make sure we extract the full question that was asked
            lines = text.split("\n")
            for i in range(len(lines)):
                line = lines[i]

                if re.search(
                    r"^(21\.|22\.|23\.|24\.|25\.|26\.|27\.|27A\.|27B\.|28\.|30\.|31\.|32\.|33\.|Armed military search drones|Robotic nurses for bedridden patients t|Self-driven local delivery|Machines that perform risky jobs|Machines that monitor and make|Facial recognition technology)",
                    line,
                ):
                    if (
                        "?" in line or "TREND" in line or "Jan." in line
                    ):  # Trick to extract the full question in case it takes up more than one line
                        texts.append(line)
                    else:
                        i += 1
                        line += " " + lines[i] if i < len(lines) else ""
                        while "?" not in line and "TREND" not in line and "Jan." not in line:
                            i += 1
                            line += " " + lines[i] if i < len(lines) else ""
                        texts.append(line)

    dataframes = [pd.DataFrame(table[1:], columns=table[0]) for table in tables]
    return texts, dataframes


def preprocess_data(dataframes, texts):
    """
    Preprocess a list of dataframes by replacing 'None' values with NaNs, extract column names from the row with '2023',
    drop rows with NaN values, drop rows with '(n)' in the index, adding the 'question' and 'country' columns,
    convert the 'percentage' column to float, and melt the dataframe.

    Args:
        dataframes (list): A list of dataframes to preprocess.
        texts (list): A list of corresponding texts for each dataframe.

    Returns:
        list: A list of preprocessed dataframes (melted).

    """
    preprocessed_dataframes = []

    for index, dataframe in enumerate(dataframes):
        # Replace 'None' with NaNs
        dataframe = dataframe.replace("None", np.nan)

        # Find the index of the row that contains '2023' (all questions have 2023 answers)
        index_2023 = dataframe[dataframe.eq("2023").any(axis=1)].index[0]

        # Set the column names from the row with '2023'
        column_names = dataframe.iloc[index_2023]
        dataframe.columns = column_names

        # Drop the rows used to set the column names and any rows that contain NaN values
        dataframe = dataframe.iloc[index_2023 + 1 :].dropna(how="any")

        # Drop the rows where the index contains "(n)" - sample size so don't really need this here
        dataframe = dataframe[~dataframe.astype(str).apply(lambda row: row.str.contains(r"\(n\)")).any(axis=1)]

        # Add 'question' name the answer column as a 'country' columns
        question = texts[index]
        dataframe["question"] = question
        dataframe.rename(columns={column_names.iloc[0]: "answer"}, inplace=True)

        # Reset the index
        dataframe.reset_index(drop=True, inplace=True)

        # Melt the dataframe to have a column for each question
        melted_dataframe = pd.melt(dataframe, id_vars=["question", "answer"], var_name="year", value_name="percentage")

        # Remove the % sign
        melted_dataframe["percentage"] = melted_dataframe["percentage"].str.replace("%", "").astype(float)

        # Append the preprocessed dataframe to the list
        preprocessed_dataframes.append(melted_dataframe)

    return preprocessed_dataframes


def remove_strings(texts):
    """
    Removes specific strings from a list of texts.

    Args:
        texts (list): A list of texts to be cleaned.

    Returns:
        list: A list of cleaned texts with specific strings removed.

    """
    for i in range(len(texts)):
        text = texts[i]

        # Define the strings to be removed
        strings_to_remove = [
            "21.",
            "22.",
            "23.",
            "24.",
            "25.",
            "26.",
            "27.",
            "27A.",
            "27B.",
            "28.",
            "30.",
            "31.",
            "32.",
            "33.",
            "Jan.",
            "April",
        ]

        # Remove the specific strings
        for string in strings_to_remove:
            text = re.sub(re.escape(string), "", text)

        # Assign cleaned text back to the list
        texts[i] = text.strip()

    return texts
