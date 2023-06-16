"""Load a snapshot and create a meadow dataset."""

import re
from typing import cast

import numpy as np
import pandas as pd
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
    log.info("papers_with_code_imagenet_top1.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("papers_with_code_imagenet.html"))

    # Load data from snapshot.
    # Read the HTML file
    #
    # Process data.
    #

    # Create a new table and ensure all columns are snake-case.

    with open(snap.path, "r") as file:
        html_content = file.read()
    df = imagenet_html_extract(html_content)
    tb = Table(df, short_name="papers_with_code_imagenet", underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("papers_with_code_imagenet.end")


def imagenet_html_extract(html_content):
    """
    Extracts table data from the HTML content of the ImageNet SOTA page on Papers with Code.

    Args:
        html_content (str): The HTML content of the ImageNet SOTA page.

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
    script_content = evaluation_script.string

    assert script_content is not None, "Script content is empty."

    # Encode the script content into bytes
    script_bytes = script_content.encode("utf-8")

    # Extract the entries using regex pattern
    entry_pattern = rb'{"table_id": 116.*?(?=\{"table_id": 116|$)'
    entries = re.findall(entry_pattern, script_bytes)

    assert entries, "No entries found in script content."

    # Create a list to store the extracted data
    table_data = []

    # Iterate through each entry
    for entry in entries:
        # Convert the entry bytes back to string
        entry_str = entry.decode("utf-8")

        # Extract the desired fields using regex
        method_short_match = re.search(r'"method_short":\s*"([^"]*)"', entry_str)
        top_1_accuracy_match = re.search(r'"Top 1 Accuracy":\s*"([^"]*)"', entry_str)
        top_5_accuracy_match = re.search(r'"Top 5 Accuracy":\s*"([^"]*)"', entry_str)
        number_of_params_match = re.search(r'"Number of params":\s*"([^"]*)"', entry_str)
        uses_additional_data_match = re.search(r'"uses_additional_data":\s*(true|false)', entry_str)
        evaluation_date_match = re.search(r'"evaluation_date":\s*"([^"]*)"', entry_str)

        # Extract the values from the match objects if available
        method_short = method_short_match.group(1) if method_short_match else np.nan
        top_1_accuracy = top_1_accuracy_match.group(1) if top_1_accuracy_match else np.nan
        top_5_accuracy = top_5_accuracy_match.group(1) if top_5_accuracy_match else np.nan
        number_of_params = number_of_params_match.group(1) if number_of_params_match else np.nan
        uses_additional_data = uses_additional_data_match.group(1).lower() if uses_additional_data_match else np.nan
        evaluation_date = evaluation_date_match.group(1) if evaluation_date_match else np.nan

        table_data.append(
            (method_short, top_1_accuracy, top_5_accuracy, number_of_params, uses_additional_data, evaluation_date)
        )

    # Convert the table data to a DataFrame
    df = pd.DataFrame(
        table_data,
        columns=[
            "Method Short",
            "Top 1 Accuracy",
            "Top 5 Accuracy",
            "Number of Params",
            "Uses Additional Data",
            "Evaluation Date",
        ],
    )
    return df
