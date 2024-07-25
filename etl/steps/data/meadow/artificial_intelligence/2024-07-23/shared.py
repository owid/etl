import owid.catalog.processing as pr
import pandas as pd
import pdfplumber
from structlog import get_logger

from etl.snapshot import Snapshot

log = get_logger()


def process_data(snap: Snapshot):
    """
    Process sheets in the given Excel file and return a combined DataFrame.
    """
    sheet_names = snap.ExcelFile().sheet_names
    tables = []
    # Iterate through the matched sheet names and process each sheet
    for i, sheet_name in enumerate(sheet_names):
        log.info(f"Processing sheet: {sheet_name}")
        tb = snap.read_excel(sheet_name=sheet_name)
        question = tb.columns[0]  # Extract the question that was asked
        melted_tb = tb.melt(
            id_vars=question, var_name="Date", value_name="Value"
        )  # Melt date columns into one called 'Date'
        filtered_tb = melted_tb[~melted_tb[question].isin(["Unweighted base", "Base"])]  # Exclude sample sizes
        filtered_tb = filtered_tb.assign(Value=filtered_tb["Value"] * 100, Group=sheet_name)  # Convert to percentages

        tables.append(filtered_tb)

    # Concatenate all the processed DataFrames
    tb_concat = pr.concat(tables, axis=0, ignore_index=True)

    return tb_concat
