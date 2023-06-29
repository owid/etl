"""Load a snapshot and create a meadow dataset."""

import re
from typing import cast

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("papers_with_code_math_code_atari.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.

    # Loop through each extracted file
    snap = cast(Snapshot, paths.load_dependency("papers_with_code_atari.html"))

    with open(snap.path, "r") as file:
        html_content = file.read()
    df = extract_data_papers_with_code_atari(html_content, "atari")
    df.drop("additional_data", axis=1, inplace=True)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name="papers_with_code_atari", underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=None)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("papers_with_code_atari.end")


def extract_data_papers_with_code_atari(html_content, metric):
    # Define the regex pattern to match the table information
    pattern = r'\{"x": "(\d{4}-\d{2}-\d{2})", "y": ([\d.]+), "name": "([^"]+)", "nameShort": "([^"]+)", "nameDetails": (null|\w+), "paperSlug": "([^"]+)", "usesAdditionalData": (true|false)\}'

    # Find all matches of the pattern
    matches = re.findall(pattern, html_content)

    # Process the matches
    data = []
    for match in matches:
        x = match[0]
        y = match[1]
        name = match[2]
        uses_additional_data = match[6]

        # Append the extracted information to the data list
        data.append({"date": x, "performance_" + metric: y, "name": name, "additional_data": uses_additional_data})

    # Create the DataFrame from the data list
    df = pd.DataFrame(data)

    return df
