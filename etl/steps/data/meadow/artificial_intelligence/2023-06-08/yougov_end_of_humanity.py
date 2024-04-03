"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
import pdfplumber
import shared
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("yougov_end_of_humanity.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("yougov_end_of_humanity.pdf"))

    # Define column names for questions 2 and 3 (possible causes of the end of the human race)
    column_names = [
        "options",
        "Artificial Intelligence",
        "Nuclear weapons",
        "A pandemic",
        "Climate change",
        "World war",
        "Asteroid impact",
        "Alien invasion",
        "An act of God",
        "Global inability to have children",
    ]

    # Define questions asked
    questions = [
        "How concerned, if at all, are you about the possibility of the end of the human race on Earth?",
        "How likely, if at all, do you think it is that the following would cause the end of the human race on Earth?",
        "How concerned, if at all, are you about the possibility that the following will cause the end of the human race on Earth?",
    ]
    #
    # Process data.
    #
    # Extract survey results for questions listed above (df_q1, df_q2, df_q3 and for the 3rd question exctract survey results split by age group)
    df_q1 = question_1_df(snap, questions[0])
    df_q2 = question_2_df(snap, column_names, questions[1])
    df_q3 = question3_df(snap, column_names, questions[2])
    df_q3_age = question3_split_by_age(snap, column_names[1:])

    # Merge the DataFrames on 'options' column
    merged_df = pd.merge(df_q1, df_q2, on="options", how="outer")
    merged_df = pd.merge(merged_df, df_q3, on="options", how="outer")
    merged_df = pd.merge(merged_df, df_q3_age, on="options", how="outer")

    # Reset the index
    merged_df.reset_index(drop=True, inplace=True)

    # Deterministic sorting
    merged_df = merged_df.sort_values("options")

    # Create a new table and ensure all columns are snake-case.
    tb = Table(merged_df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("yougov_end_of_humanity.end")


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


def question_1_df(snap: Snapshot, question: str) -> pd.DataFrame:
    """
    Extract the data for the first question -
    "How concerned, if at all, are you about the possibility of the end of the human race on Earth?",
    from the first page of a PDF and convert it into a DataFrame.

    Args:
        pdf_path (str): The path to the PDF file.
        question (str): The question to extract.

    Returns:
        pd.DataFrame: The extracted data as a DataFrame.
    """
    table = shared.read_table_from_pdf(snap.path, 1)
    # Select relevant entries (pdf file reads all lines into 1 messy dataframe; lines 19-28 are the relevant entries for the first question)
    question1 = table.iloc[19:28]

    # Create a DataFrame with 'options' (first column) and responses in the second last (name it with the question that was asked)
    concern_df = pd.DataFrame({"options": question1.iloc[:, 1], question: question1.iloc[:, -2]})

    # Clean up the data in the question column (remove % sign and dots and convert to numeric)
    concern_df[question] = concern_df[question].str.replace("%", "", regex=True)
    concern_df[question] = concern_df[question].str.replace(".", "", regex=True)
    concern_df[question] = pd.to_numeric(concern_df[question])

    # Drop completely empty rows
    concern_df = concern_df.dropna(axis=0)
    # Reset the index
    concern_df.reset_index(drop=True, inplace=True)

    # Rename the values in the question column for readability
    concern_df = concern_df.replace(
        {
            "Veryconc": "Very concerned",
            "Somewha": "Somewhat",
            "Notveryc": "Not very concerned",
            "Notatall": "Not at all",
        }
    )

    # Reset the index
    concern_df.reset_index(drop=True, inplace=True)

    return concern_df


def question_2_df(snap: Snapshot, column_names: list, question: str) -> pd.DataFrame:
    """
    Extract the data for the second question -
    "How likely, if at all, do you think it is that the following would cause the end of the human race on Earth?",
    from the first page of a PDF, clean it up and convert it into a DataFrame.

    Args:
        pdf_path (str): The path to the PDF file.
        column_names (list): The list of column names for the resulting DataFrame.
        question (str): The question to extract.

    Returns:
        pd.DataFrame: The extracted data as a DataFrame.
    """
    table = shared.read_table_from_pdf(snap.path, 1)

    # Select relevant entries (pdf file reads all lines into 1 messy dataframe; lines starting from 34 are the relevant entries)
    question2 = table.iloc[34:]

    # Remove the '%' symbol from the selected columns
    question2 = question2.replace("%", "", regex=True)

    # Select the relevant columns (firs column with options and last five are survey responses) for the DataFrame and convert them to numeric
    selected_columns = pd.DataFrame(question2.iloc[:, [0] + list(range(-5, 0))])
    selected_columns.loc[:, selected_columns.columns[-5:]] = selected_columns.loc[
        :, selected_columns.columns[-5:]
    ].apply(pd.to_numeric)

    # Drop completely empty rows
    selected_columns = selected_columns.dropna(axis=0)

    # Reset the index
    selected_columns.reset_index(drop=True, inplace=True)

    # Rename the columns with more descriptive names
    selected_columns.columns = [
        "options",
        "Very likely",
        "Somewhat likely",
        "Not very likely",
        "Impossible",
        "Not sure",
    ]

    # Transpose the DataFrame to have 'options' as the index
    transposed_df = selected_columns.T

    # Remove the first row (previous column names) and reset the index
    transposed_df = transposed_df.iloc[1:]
    transposed_df.reset_index(inplace=True)

    # Rename the columns using the provided column names and question
    transposed_df.columns = column_names
    for col in transposed_df.columns[1:]:
        new_col = question + " " + col
        transposed_df.rename(columns={col: new_col}, inplace=True)

    return transposed_df


def question3_df(snap: Snapshot, column_names: list, question: str) -> pd.DataFrame:
    """
    Extract the data for the third question -
    "How concerned, if at all, are you about the possibility that the following will cause the end of the human race on Earth?",
    from the second page of a PDF, clean it up and convert it into a DataFrame.

    Args:
        pdf_path (str): The path to the PDF file.
        column_names (list): The list of column names for the resulting DataFrame.
        question (str): The question to extract.

    Returns:
        pd.DataFrame: The extracted data as a DataFrame.
    """
    table = shared.read_table_from_pdf(snap.path, 2)
    # Skip the first row
    question3 = table.iloc[1:]

    # Set the column names from the 18th row as the header
    question3.columns = question3.iloc[0]
    question3.columns = [
        "options",
        "Very concerned",
        "Somewhat concerned",
        "Not very concerned",
        "Not at all concerned",
        "Not sure",
        "Considers impossible",
    ]
    question3 = question3[14:33]

    # Remove letters and % sign from columns starting from column 2
    question3.iloc[:, 1:] = question3.iloc[:, 1:].replace("[a-zA-Z%]", "", regex=True)

    # Reset the index
    question3.reset_index(drop=True, inplace=True)
    question3.loc[:, question3.columns[1:]] = question3.loc[:, question3.columns[1:]].apply(pd.to_numeric)
    question3 = question3.dropna(axis=0)
    question3["options"] = column_names[1:]
    question3_t = question3.T
    question3_t.columns = question3_t.iloc[0]
    question3_t = question3_t[1:]
    question3_t.reset_index(inplace=True)
    question3_t.rename(columns={question3_t.columns[0]: "options"}, inplace=True)
    for col in question3_t.columns[1:]:
        new_col = question + " " + col
        question3_t.rename(columns={col: new_col}, inplace=True)

    return question3_t


def question3_split_by_age(snap: Snapshot, risks: list) -> pd.DataFrame:
    """
    Extract the data for the question -
    "How concerned, if at all, are you about the possibility that the following will cause the end of the human race on Earth?"
    split by age groups from multiple pages of a PDF and merge them into a single DataFrame.

    Args:
        pdf_path (str): The path to the PDF file.
        risks (list): The list of risk names.

    Returns:
        pd.DataFrame: The merged data as a DataFrame.
    """
    df_age = []
    for p, page in enumerate([18, 20, 22, 24, 26, 28, 30, 32, 34]):
        table = shared.read_table_from_pdf(snap.path, page)
        df = table.iloc[7:23]
        df = pd.DataFrame(df[df.columns[0]])
        df = df.drop(index=range(18, 22))
        df[["Answer", "Total", "Male", "Female", "18-29", "30-44", "45-64", "65+"]] = df[
            "YouGov Survey: AI and the End of Humanity"
        ].str.split(expand=True)
        df.drop(columns="YouGov Survey: AI and the End of Humanity", inplace=True)
        df_age_only = df.iloc[:, -4:]
        df_age_only = df_age_only.replace("%", "", regex=True)
        df_age_only = df_age_only.dropna(axis=0)
        df_age_only.columns = [col + " " + risks[p] for col in df_age_only.columns]

        df_age_only["options"] = [
            "Very concerned",
            "Somewhat concerned",
            "Not very concerned",
            "Not at all concerned",
            "Not sure",
            "Considers impossible",
        ]
        df_age_only.reset_index(inplace=True, drop=True)
        df_age.append(df_age_only)

    combined_df = df_age[0]

    # Merge the remaining DataFrames in df_age on 'options' column
    for i in range(1, len(df_age)):
        combined_df = pd.merge(combined_df, df_age[i], on="options", how="outer")

    # Reset the index
    combined_df.reset_index(drop=True, inplace=True)

    return combined_df
