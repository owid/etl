"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Names of columns (expected to result from concatenating the header rows).
COLUMNS = [
    "year",
    "Household final users - Nominal food expenditure percentage share of disposable personal income (DPI) - FAH",
    "Household final users - Nominal food expenditure percentage share of disposable personal income (DPI) - FAFH",
    "Household final users - Nominal food expenditure percentage share of disposable personal income (DPI) - All food",
    "Household final users - Percentage shares of nominal food expenditures - FAH",
    "Household final users - Percentage shares of nominal food expenditures - FAFH",
    "Household final users - Nominal expenditures per household (U.S. dollars) - FAH",
    "Household final users - Nominal expenditures per household (U.S. dollars) - FAFH",
    "Household final users - Nominal expenditures per household (U.S. dollars) - All food",
    "Household final users - Constant U.S. dollar expenditures per household (1988=100) - FAH",
    "Household final users - Constant U.S. dollar expenditures per household (1988=100) - FAFH",
    "Household final users - Constant U.S. dollar expenditures per household (1988=100) - All food",
    "All purchasers - Percentage share of nominal food expenditures - FAH",
    "All purchasers - Percentage share of nominal food expenditures - FAFH",
    "All purchasers - Nominal expenditures per capita (U.S. dollars) - FAH",
    "All purchasers - Nominal expenditures per capita (U.S. dollars) - FAFH",
    "All purchasers - Nominal expenditures per capita (U.S. dollars) - All food",
    "All purchasers - Constant U.S. dollar expenditures per capita (1988=100) - FAH",
    "All purchasers - Constant U.S. dollar expenditures per capita (1988=100) - FAFH",
    "All purchasers - Constant U.S. dollar expenditures per capita (1988=100) - All food",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots and read its data.
    snap = paths.load_snapshot("food_expenditure_in_us.xlsx")
    tb = snap.read_excel(skiprows=4, header=None, names=COLUMNS)

    # Load the snapshot again (including all header rows) just to check if the header is as expected.
    columns_new = snap.read_excel(skiprows=1, header=[0, 1, 2]).columns

    #
    # Process data.
    #
    # Ensure column names are as expected.
    error = "Column names may have changed."
    assert ["year"] + [" - ".join(cols) for cols in columns_new[1:]] == COLUMNS, error

    # Rename columns conveniently.
    tb = tb.rename(columns={tb.columns[i]: column for i, column in enumerate(COLUMNS)}, errors="raise")

    # Remove rows that contain footer notes.
    tb = tb[tb["year"].astype(str).str.isdigit()].reset_index(drop=True)

    # Create an appropriate index and sort conveniently.
    tb = tb.format(keys=["year"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
