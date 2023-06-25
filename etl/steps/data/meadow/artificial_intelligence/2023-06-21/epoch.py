"""Load a snapshot and create a meadow dataset."""

# import re
# from openpyxl import load_workbook

from typing import cast

import numpy as np
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
    log.info("epoch.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = cast(Snapshot, paths.load_dependency("epoch.xlsx"))

    # Now read the file with pandas
    df = pd.read_excel(snap.path, sheet_name="ALL ML SYSTEMS")
    # index_not_nan = df.dropna(how="all").index

    # # Load your workbook
    # wb = load_workbook(filename=snap.path, data_only=False)
    # # Select your sheet
    # ws = wb["NOTABLE ML SYSTEMS"]  # replace with your sheet name

    # # Initialize an empty list to store the numbers
    # notable_index = []

    # # Find which rows are selected for Notable AI systems spreadsheet
    # for cell in ws["A"]:  # replace 'A' with your column letter if different
    #     value = cell.value
    #     if isinstance(value, str):
    #         # Find all numbers in the string
    #         numbers = re.findall(r"\d+", value)
    #         # Append each number found to the list
    #         for number in numbers:
    #             notable_index.append(int(number))

    # # Intersect notable_index and index_not_nan to get indices present in both lists
    # final_indices = list(set(notable_index) & set(index_not_nan.tolist()))

    # # Select only notable
    # df = df.loc[final_indices].reset_index(drop=True)

    #
    # Process data.
    #
    # Select columns of interest.
    cols = [
        "System",
        "Domain",
        "Organization Categorization",
        "Publication date",
        "Parameters",
        "Training compute (FLOP)",
        "Training dataset size (datapoints)",
        "Training time (hours)",
        "Equivalent training time (hours)",
        "Inclusion criteria",
    ]

    df = df[cols]
    df.replace("#REF!", np.nan, inplace=True)
    df.replace("", np.nan, inplace=True)

    df["Training compute (FLOP)"] = df["Training compute (FLOP)"].astype(float)

    #
    # Create a new table and ensure all columns are snake-case.
    #
    tb = Table(df, short_name=paths.short_name, underscore=True)
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("epoch.end")
