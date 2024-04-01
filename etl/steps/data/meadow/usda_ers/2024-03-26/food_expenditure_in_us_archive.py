"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Names of columns (expected to result from concatenating the header rows).
COLUMNS = [
    "year",
    "Household final users - Food expenditure share of disposable personal income (DPI)1 - FAH",
    "Household Final Users - Food Expenditure Share of Disposable Personal Income (DPI) - FAFH",
    "Household Final Users - Food Expenditure Share of Disposable Personal Income (DPI) - All food",
    "Household Final Users - Share of food expenditures2 - FAH",
    "Household Final Users - Shares of Food Expenditures - FAFH",
    "All purchasers - Share of food expenditures2 - FAH",
    "All Purchasers - Share of Food Expenditures - FAFH",
    "All Purchasers - Nominal expenditure per capita3 - FAH",
    "All Purchasers - Nominal Expenditure per Capita - FAFH",
    "All Purchasers - Nominal Expenditure per Capita - All food",
    "All Purchasers - Constant dollar expenditure per capita (1988=100)3 - FAH",
    "All Purchasers - Constant Dollar Expenditure per Capita (1988=100) - FAFH",
    "All Purchasers - Constant Dollar Expenditure per Capita (1988=100) - All food",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots and read its data.
    snap = paths.load_snapshot("food_expenditure_in_us_archive.xlsx")
    tb = snap.read_excel(skiprows=6, header=None, names=COLUMNS, na_values=["--"])

    # Load the snapshot again (including all header rows) just to check if the header is as expected.
    columns_new = snap.read_excel(skiprows=2, header=[0, 1, 2, 3], na_values=["--"]).columns

    #
    # Process data.
    #
    # Ensure column names are as expected.
    error = "Column names may have changed."
    assert ["year"] + [f"{cols[0]} - {cols[1]} - {cols[3]}" for cols in columns_new[1:]] == COLUMNS, error

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
