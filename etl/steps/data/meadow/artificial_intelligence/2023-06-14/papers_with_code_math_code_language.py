"""Load a snapshot and create a meadow dataset."""

import os
import zipfile
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
    log.info("papers_with_code_math_code_language.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.

    # Loop through each extracted file
    snap_language = cast(Snapshot, paths.load_dependency("papers_with_code_language.html"))

    # Extract the directory path
    directory_path = os.path.dirname(snap_language.path)

    # Find the position of the dash after the date
    dash_index = directory_path.index("-", directory_path.index("/") + 1)

    # Extract the path until the dash after the date, including the full date
    zip_file_path = directory_path[: dash_index + 7] + "/papers_with_code_language.zip"
    output_folder = directory_path[: dash_index + 7] + "/unzipped_language_files/"

    # Extract the zip file
    with zipfile.ZipFile(zip_file_path, "r") as zipf:
        # Extract all files to the output folder
        zipf.extractall(output_folder)

    df_list_lang = []
    url_suffixes = ["Humanities", "STEM", "Social Sciences", "Other"]

    # Loop through each extracted file
    for f, file_name in enumerate(os.listdir(output_folder)):
        file_path = os.path.join(output_folder, file_name)

        # Read the content of the file
        with open(file_path, "r") as file:
            content = file.read()
            df = shared.extract_data_papers_with_code(content, url_suffixes[f])
            df.drop("additional_data", axis=1, inplace=True)
            df_list_lang.append(df)
    merged_df_language = pd.concat(df_list_lang)

    snap_coding = cast(Snapshot, paths.load_dependency("papers_with_code_coding.html"))
    snap_math = cast(Snapshot, paths.load_dependency("papers_with_code_math.html"))
    snap_language = cast(Snapshot, paths.load_dependency("papers_with_code_language.html"))

    # Math and Coding
    df_list = []
    # Load data from snapshot.
    # Read the HTML file
    column_suffix = ["math", "coding", "language_average"]
    for p, path in enumerate([snap_math, snap_coding, snap_language]):
        with open(path.path, "r") as file:
            html_content = file.read()
        df = shared.extract_data_papers_with_code(html_content, column_suffix[p])
        df.drop("additional_data", axis=1, inplace=True)
        df_list.append(df)

    merged_df = pd.concat(df_list)
    all_dfs = pd.concat([merged_df_language, merged_df])
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
