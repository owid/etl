"""Load a snapshot and create a meadow dataset."""

import re
from typing import cast

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
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # Find the script with id="evaluation_data"
    evaluation_script = soup.find("script", id="evaluation-table-data")

    if evaluation_script:
        # Extract the contents of the script tag
        script_content = evaluation_script.string

        # Extract the entries using regex pattern
        entry_pattern = r'\{"table_id": 116.*?(?=\{"table_id": 116|$)'
        entries = re.findall(entry_pattern, script_content)

        # Create a list to store the extracted data
        table_data = []

        # Iterate through each entry
        for entry in entries:
            # Extract the desired fields using regex
            method_short_match = re.search(r'"method_short":\s*"([^"]*)"', entry)
            top_1_accuracy_match = re.search(r'"Top 1 Accuracy":\s*"([^"]*)"', entry)
            top_5_accuracy_match = re.search(r'"Top 5 Accuracy":\s*"([^"]*)"', entry)
            number_of_params_match = re.search(r'"Number of params":\s*"([^"]*)"', entry)
            uses_additional_data_match = re.search(r'"uses_additional_data":\s*(true|false)', entry)
            evaluation_date_match = re.search(r'"evaluation_date":\s*"([^"]*)"', entry)

            # Extract the values from the match objects if available
            method_short = method_short_match.group(1) if method_short_match else ""
            top_1_accuracy = top_1_accuracy_match.group(1) if top_1_accuracy_match else ""
            top_5_accuracy = top_5_accuracy_match.group(1) if top_5_accuracy_match else "Not available"
            number_of_params = number_of_params_match.group(1) if number_of_params_match else ""
            uses_additional_data = uses_additional_data_match.group(1).lower() if uses_additional_data_match else ""
            evaluation_date = evaluation_date_match.group(1) if evaluation_date_match else ""

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
