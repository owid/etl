"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Expected column names in the Excel file.
COLUMNS = [
    "Year",
    "Food-at-home nominal food expenditure share of disposable personal income, household final users (percentage)",
    "Food-away-from-home nominal food expenditure share of disposable personal income, household final users (percentage)",
    "Total nominal food expenditure share of disposable personal income, household final users (percentage)",
    "Food-at-home share of nominal food expenditures, household final users (percentage)",
    "Food-away-from-home share of nominal food expenditures, household final users (percentage)",
    "Food-at-home expenditures per household, household final users (nominal U.S. dollars)",
    "Food-away-from-home expenditures per household, household final users (nominal U.S. dollars)",
    "Total food expenditures per household, household final users (nominal U.S. dollars)",
    "Food-at-home expenditures per household, household final users (constant U.S. dollars (1988=100))",
    "Food-away-from-home expenditures per household, household final users (constant U.S. dollars (1988=100))",
    "Total food expenditures per household, household final users (constant U.S. dollars (1988=100))",
    "Food-at-home share of nominal food expenditures, all purchasers (percentage)",
    "Food-away-from-home share of nominal food expenditures, all purchasers (percentage)",
    "Food-at-home expenditures per capita, all purchasers (nominal U.S. dollars)",
    "Food-away-from-home expenditures per capita, all purchasers (nominal U.S. dollars)",
    "Total food expenditures per capita, all purchasers (nominal U.S. dollars)",
    "Food-at-home expenditures per capita, all purchasers (constant U.S. dollars (1988=100))",
    "Food-away-from-home expenditures per capita, all purchasers (constant U.S. dollars (1988=100))",
    "Total food expenditures per capita, all purchasers (constant U.S. dollars (1988=100))",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its data (skip the title row, use the single header row).
    snap = paths.load_snapshot("food_expenditure_in_us.xlsx")
    tb = snap.read_excel(skiprows=1)

    #
    # Process data.
    #
    # Ensure column names are as expected.
    assert list(tb.columns) == COLUMNS, f"Column names may have changed. Got: {list(tb.columns)}"

    # Rename "Year" to "year" for consistency.
    tb = tb.rename(columns={"Year": "year"})

    # Remove rows that contain footer notes.
    tb = tb[tb["year"].astype(str).str.isdigit()].reset_index(drop=True)

    # Improve table format.
    tb = tb.format(keys=["year"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
