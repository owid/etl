"""Load a snapshot and create a meadow dataset."""

from typing import cast

import pandas as pd
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
    log.info("yougov_jobs.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("yougov_jobs.pdf"))

    #
    # Process data.
    #
    questions = [
        "Do you think that advances in artificial intelligence (AI) will increase or decrease the number of jobs available in the U.S. for the following people?",
        "Thinking about the effects artificial intelligence (AI) will have on businesses and their employees, do you believe it will increase or decrease each of the following. . . ?",
    ]

    # Extract survey results for questions listed above

    df1 = process_job_title_data(snap.path, questions[0])
    df2 = process_activity_data(snap.path, questions[1])
    merged_df = pd.merge(df1, df2, how="outer", validate="1:1")

    # Create a new table and ensure all columns are snake-case.
    tb = Table(merged_df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("yougov_jobs.end")


def process_job_title_data(pdf_path, question):
    # Read table from PDF file
    df1 = shared.read_table_from_pdf(pdf_path, 1)

    # Select the desired rows
    df1 = df1[19:61]

    # Set the column names from the 6th row
    df1.columns = df1.iloc[5]

    # Remove unnecessary rows
    df1 = df1[3:]

    # Drop the 2nd and 3rd columns
    df1 = df1.drop(df1.columns[[1, 2]], axis=1)

    # Rename the columns
    df1.columns = [question, "Increase", "Have no effect", "Decrease", "Not sure"]

    # Remove "%" from the values
    df1 = df1.replace("%", "", regex=True)

    # Convert columns to numeric values
    df1[df1.columns[1:]] = df1[df1.columns[1:]].apply(pd.to_numeric)

    # Drop rows with missing values
    df1 = df1.dropna(axis=0)

    # Reset the index
    df1.reset_index(inplace=True, drop=True)

    # List of occupations
    df1occupations = [
        "Accountants",
        "Artists",
        "Child care workers",
        "Computer programmers",
        "Customer service agents",
        "Data scientists",
        "Medical doctors",
        "Engineers",
        "Graphic designers",
        "Journalists",
        "Judges",
        "Lawyers",
        "Librarians",
        "Market research analysts",
        "Manufacturing workers",
        "Real estate agents",
        "Retail sales workers",
        "Social workers",
        "Teachers",
        "Truck drivers",
    ]

    # Add occupations as a new column
    df1[question] = df1occupations

    return df1


def process_activity_data(pdf_path, question):
    """
    Process activity data from a PDF file.

    Args:
        pdf_path (str): The path to the PDF file.

    Returns:
        pd.DataFrame: Processed activity data as a DataFrame.
    """
    # Read table from PDF file
    df2 = shared.read_table_from_pdf(pdf_path, 2)

    # Set the column names
    df2.columns = [question, "Increase", "Have no effect", "Decrease", "Not sure"]

    # Select the desired rows
    df2 = df2[4:]

    # Remove "%" from the values
    df2 = df2.replace("%", "", regex=True)

    # Convert columns to numeric values
    df2[df2.columns[1:]] = df2[df2.columns[1:]].apply(pd.to_numeric)

    # Drop rows with missing values
    df2 = df2.dropna(axis=0)

    # Reset the index
    df2.reset_index(inplace=True, drop=True)

    # List of items
    items = [
        "Automation of routine tasks",
        "Customer costs",
        "Customer satisfaction",
        "Innovation",
        "Job opportunities",
        "Operating costs",
        "Productivity",
        "Remote-work capabilities",
        "The quality of work",
        "Worker satisfaction",
        "Work-life balance",
        "Workplace monitoring of employees",
    ]

    # Add items as a new column
    df2[question] = items

    return df2
