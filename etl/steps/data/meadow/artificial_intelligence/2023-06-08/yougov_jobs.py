"""Load a snapshot and create a meadow dataset."""

from typing import cast

import camelot
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
    log.info("yougov_jobs.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("yougov_jobs.pdf"))
    pdf_path_str = str(snap.path)

    # Read the tables from the PDF
    tables = camelot.read_pdf(pdf_path_str, flavor="stream", pages="1-2")

    # Convert each table to a DataFrame and make the first row the column names
    # Convert each table to a DataFrame and make column name changes
    dfs = []
    question_1 = "Do you think that advances in artificial intelligence (AI) will increase or decrease the number of jobs available in the U.S. for the following people?"
    question_2 = "Thinking about the effects artificial intelligence (AI) will have on businesses and their employees, do you believe it will increase or decrease each of the following?"

    for table in tables:
        df = table.df.copy()  # Create a copy of the DataFrame
        new_header = df.iloc[1]  # Select the first row as the new header
        df = df[2:]  # Remove the first row from the DataFrame
        df.columns = new_header  # Set the new header as the column names
        if table == tables[0]:
            df.rename(columns={"": question_1, df.columns[2]: "Have no effect"}, inplace=True)
        elif table == tables[1]:
            df.rename(columns={"": question_2, df.columns[2]: "Have no effect"}, inplace=True)
        dfs.append(df)

    concatenated_df = pd.concat(dfs, ignore_index=True)
    concatenated_df = concatenated_df.apply(lambda x: x.str.replace("%", "") if x.dtype == "object" else x)

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(concatenated_df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("yougov_jobs.end")
