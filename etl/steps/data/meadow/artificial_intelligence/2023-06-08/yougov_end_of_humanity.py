"""Load a snapshot and create a meadow dataset."""

from typing import cast

import camelot
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
    pdf_path_str = str(snap.path)
    tables = camelot.read_pdf(pdf_path_str, flavor="stream", pages="2")

    #
    # Process data.
    #
    question_1 = "How concerned, if at all, are you about the possibility that the following will cause the end of the human race on Earth?"
    df = tables[0].df.copy()
    # Processing the table (start from the 13th row because that's where the data is)
    df = df[13:]
    # Rename to the be consistent with the survey questions
    df.rename(
        columns={
            df.columns[0]: question_1,
            1: "Very",
            2: "Somewhat",
            3: "Not very",
            4: "Not at all",
            df.columns[5]: "Not Sure",
            df.columns[6]: "Considers Impossible",
        },
        inplace=True,
    )
    df.reset_index(inplace=True, drop=True)

    # Drop this row because it's empty
    df = df.drop(8)
    # Rename 'children' in this row to the question that was asked
    df.at[9, df.columns[0]] = "Global inability to have children"
    df.reset_index(inplace=True, drop=True)

    df = df.apply(lambda x: x.str.replace("%", "") if x.dtype == "object" else x)
    df_t = df.transpose()
    df_t.columns = df_t.iloc[0]
    df_t = df_t.iloc[1:]

    # Rename artificial/intelligence entry
    df_t.rename(columns={df_t.columns[0]: "Artificial Intelligence"}, inplace=True)

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df_t, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("yougov_end_of_humanity.end")
