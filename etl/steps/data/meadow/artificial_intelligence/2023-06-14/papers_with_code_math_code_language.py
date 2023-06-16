"""Load a snapshot and create a meadow dataset."""

import re
from typing import cast

import numpy as np
import pandas as pd
import shared
from bs4 import BeautifulSoup
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("papers_with_code_math_code_language.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.

    # Loop through each extracted file
    snap_language = cast(Snapshot, paths.load_dependency("papers_with_code_language.html"))

    with open(snap_language.path, "r") as file:
        html_content = file.read()
    lang_df = language_extract(html_content)

    snap_math = cast(Snapshot, paths.load_dependency("papers_with_code_math.html"))
    snap_coding = cast(Snapshot, paths.load_dependency("papers_with_code_coding.html"))

    # Math and Coding
    df_list = []
    # Load data from snapshot.
    # Read the HTML file
    column_suffix = ["math", "coding"]
    for p, path in enumerate([snap_math, snap_coding]):
        with open(path.path, "r") as file:
            html_content = file.read()
        df = shared.extract_data_papers_with_code(html_content, column_suffix[p])
        df.drop("additional_data", axis=1, inplace=True)
        df_list.append(df)

    merged_df = pd.concat(df_list)

    all_dfs = pd.merge(lang_df, merged_df, on=["date", "name"], how="outer")
    all_dfs.reset_index(inplace=True, drop=True)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(all_dfs, short_name="papers_with_code_math_code_language", underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=None)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("papers_with_code_math_code_language.end")


def language_extract(html_content):
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

    # Find the script with id="evaluation_data"
    evaluation_script = soup.find("script", id="evaluation-table-data")

    assert evaluation_script is not None, "Evaluation script not found in HTML content."

    # Extract the contents of the script tag
    script_content = str(evaluation_script)

    assert script_content is not None, "Script content is empty."
    script_bytes = script_content.encode("utf-8")

    # Extract the entries using regex pattern
    pattern = rb'{"table_id": 15219.*?(?=\{"table_id": 15219|$)'

    entries = re.findall(pattern, script_bytes)

    assert entries, "No entries found in script content."

    # Create a list to store the extracted data
    table_data = []

    # Iterate through each entry
    for entry in entries:
        entry_str = entry.decode("utf-8")

        # Extract the desired fields using regex
        method_short_match = re.search(r'"method_short":\s*"([^"]*)"', entry_str)
        average_match = re.search(r'"Average \(%\)":\s*"([^"]*)"', entry_str)
        humanities_match = re.search(r'"Humanities":\s*([^,]*)', entry_str)
        stem_match = re.search(r'"STEM":\s*([^,]*)', entry_str)
        social_sciences_match = re.search(r'"Social Sciences":\s*([^,]*)', entry_str)
        other_match = re.search(r'"Other":\s*([^,]*)', entry_str)
        evaluation_date_match = re.search(r'"evaluation_date":\s*"([^"]*)"', entry_str)

        # Extract the values from the match objects if available
        method_short = (
            method_short_match.group(1) if method_short_match and method_short_match.group(1) != "null" else np.nan
        )
        average = average_match.group(1) if average_match and average_match.group(1) != "null" else np.nan
        humanities = humanities_match.group(1) if humanities_match and humanities_match.group(1) != "null" else np.nan
        stem = stem_match.group(1) if stem_match and stem_match.group(1) != "null" else np.nan
        social_sciences = (
            social_sciences_match.group(1)
            if social_sciences_match and social_sciences_match.group(1) != "null"
            else np.nan
        )
        other = other_match.group(1) if other_match and other_match.group(1) != "null" else np.nan
        evaluation_date = (
            evaluation_date_match.group(1)
            if evaluation_date_match and evaluation_date_match.group(1) != "null"
            else np.nan
        )

        # Add the extracted data to the list
        table_data.append((method_short, average, humanities, stem, social_sciences, other, evaluation_date))

    # Convert the table data to a DataFrame
    df = pd.DataFrame(
        table_data,
        columns=[
            "name",
            "performance_language_average",
            "performance_humanities",
            "performance_stem",
            "performance_social_sciences",
            "performance_other",
            "date",
        ],
    )
    df = df.replace('"', "", regex=True)

    return df
