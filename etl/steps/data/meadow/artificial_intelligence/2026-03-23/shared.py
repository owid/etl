import owid.catalog.processing as pr

from etl.snapshot import Snapshot


def process_data(snap: Snapshot):
    """
    Process sheets in the given Excel file and return a combined DataFrame.

    Each sheet corresponds to a demographic group. Response values are stored as proportions
    rounded to 2 decimal places in the source, which causes sums of 99% or 101% due to
    independent rounding of each response category. Values are normalized per date so that
    all response categories sum to exactly 100%.
    """
    sheet_names = snap.ExcelFile().sheet_names
    tables = []
    # Iterate through the matched sheet names and process each sheet
    for i, sheet_name in enumerate(sheet_names):
        tb = snap.read_excel(sheet_name=sheet_name)
        question = tb.columns[0]  # Extract the question that was asked
        melted_tb = tb.melt(
            id_vars=question, var_name="Date", value_name="Value"
        )  # Melt date columns into one called 'Date'
        filtered_tb = melted_tb[~melted_tb[question].isin(["Unweighted base", "Base"])]  # Exclude sample sizes
        filtered_tb = filtered_tb.assign(Group=sheet_name)
        # Normalize per date to sum to 100% — source data rounds each response independently,
        # causing sums of 99% or 101%. We rescale to fix this rounding artifact.
        filtered_tb["Value"] = filtered_tb.groupby("Date")["Value"].transform(lambda x: x / x.sum() * 100)

        tables.append(filtered_tb)

    # Concatenate all the processed DataFrames
    tb_concat = pr.concat(tables, axis=0, ignore_index=True)

    return tb_concat
