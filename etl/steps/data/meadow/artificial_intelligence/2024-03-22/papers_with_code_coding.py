"""Load a snapshot and create a meadow dataset."""

import re

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("papers_with_code_coding.html")

    # Load data from snapshot.
    with open(snap.path, "r") as file:
        html_content = file.read()

    df_code = code_extract(html_content)

    #
    # Process data.
    #
    tb = Table(df_code, short_name=paths.short_name, metadata=snap.to_table_metadata())

    # Ensure metadata is correctly associated.
    for column in tb.columns:
        tb[column].metadata.origins = [snap.metadata.origin]

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["name", "date"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def code_extract(html_content):
    """
    Extracts table data from the HTML content of the language evaluation page on Papers with Code.

    Args:
        html_content (str): The HTML content of the language evaluation page.

    Returns:
        pandas.DataFrame: A DataFrame containing the extracted table data.

    Raises:
        AssertionError: If the evaluation script or script content is not found, or if any required fields are missing in an entry.

    """
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # Find the script with id="evaluation-table-data"
    evaluation_script = soup.find("script", id="evaluation-table-data")

    assert evaluation_script is not None, "Evaluation script not found in HTML content."

    # Extract the contents of the script tag
    script_content = str(evaluation_script)

    assert script_content is not None, "Script content is empty."
    script_bytes = script_content.encode("utf-8")

    # Extract the entries using regex pattern
    pattern = rb'{"table_id": 10864.*?(?=\{"table_id": 10864|$)'

    entries = re.findall(pattern, script_bytes)

    assert entries, "No entries found in script content."

    # Create a list to store the extracted data
    table_data = []

    # Iterate through each entry
    for entry in entries:
        entry_str = entry.decode("utf-8")

        # Extract the desired fields using regex
        method_short_match = re.search(r'"method":\s*"([^"]*)"', entry_str)
        competition_match = re.search(r'"Competition Pass@any":\s*"([^"]*)"', entry_str)
        interview_match = re.search(r'"Interview Pass@any":\s*"([^"]*)"', entry_str)
        evaluation_date_match = re.search(r'"evaluation_date":\s*"([^"]*)"', entry_str)

        # Extract the values from the match objects if available
        method_short = (
            method_short_match.group(1) if method_short_match and method_short_match.group(1) != "null" else np.nan
        )
        competition = (
            competition_match.group(1) if competition_match and competition_match.group(1) != "null" else np.nan
        )
        interview = interview_match.group(1) if interview_match and interview_match.group(1) != "null" else np.nan
        evaluation_date = (
            evaluation_date_match.group(1)
            if evaluation_date_match and evaluation_date_match.group(1) != "null"
            else np.nan
        )

        # Add the extracted data to the list
        table_data.append((method_short, competition, interview, evaluation_date))

    # Convert the table data to a DataFrame
    df = pd.DataFrame(
        table_data,
        columns=[
            "name",
            "performance_code_any_competition",
            "performance_code_any_interview",
            "date",
        ],
    )

    df = df.replace('"', "", regex=True)
    df = df.replace("%", "", regex=True)

    return df
