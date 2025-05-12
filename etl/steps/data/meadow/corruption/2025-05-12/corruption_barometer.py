"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
CORRUPT_PARTIES = [
    "The (President)/(Prime Minister) and Officials in his office",
    "Representatives in the Legislature (i.e. Members of the Parliament or Senators)",
    "Government officials",
    "Local government councilors",
    "Police",
    "Tax Officials, like Ministry of Finance officials or Local Government tax collectors",
    "Judges and Magistrates",
    "Religious leaders",
    "Business executives",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("corruption_barometer.xlsx")

    # Load data from snapshot.

    #
    # Process data.
    #
    xls = pd.ExcelFile(snap.path)
    # Get sheet names and exclude "Contents"
    sheet_names = xls.sheet_names
    sheets_to_extract = [name for name in sheet_names if name != "Contents"]

    # Read and clean each sheet (each sheet is a question)
    cleaned_tb = []
    for sheet_name in sheets_to_extract:
        tb = snap.read(sheet_name=sheet_name)
        if sheet_name == "Q3":
            question = "Total bribary rate"
        else:
            question = (
                str(tb.iloc[0, 1]) if pd.notna(tb.iloc[0, 1]) else "Unknown question"
            )  # First row, second column is usually the question except in sheet Q3

        # Assert that the question is not empty
        assert question != "Unknown question", f"Question could not be determined for sheet '{sheet_name}'."

        # Check if the fifth row, first column is not NaN and matches a valid institution + append to question
        if pd.notna(tb.iloc[4, 0]) and str(tb.iloc[4, 0]).strip() in CORRUPT_PARTIES:
            question = question + " " + str(tb.iloc[4, 0]).strip()  # Append the specification to the question
        country_row_idx = None
        for idx, val in enumerate(tb.iloc[:, 0]):  # Iterate over the first column
            if sheet_name in ["Q3", "Q4"]:
                if (
                    pd.notna(val) and str(val).strip().lower() == "albania"
                ):  # Check for non-NaN and match 'albania' as country column isn't named
                    country_row_idx = idx - 2
                    break
            elif pd.notna(val) and str(val).strip().lower() == "country":  # Check for non-NaN and match 'country'
                country_row_idx = idx
                break

        # Assert that the 'Country' row was found

        assert country_row_idx is not None, f"No 'Country' row found in sheet '{sheet_name}'."

        tb.columns = tb.iloc[country_row_idx]

        tb = snap.read(sheet_name=sheet_name, skiprows=country_row_idx + 1)
        tb = tb.dropna(how="all")  # Drop rows where all values are NaN
        tb = tb.dropna(axis=1, how="all")  # Drop columns where all values are NaN

        # Add a column for the question
        tb["question"] = question
        if sheet_name in ["Q3", "Q4"]:
            # Explicitly rename the first column to 'Country'
            tb.rename(columns={tb.columns[0]: "Country"}, inplace=True)

        # Melt to have answers in a single column
        tb = tb.melt(
            id_vars=["Country", "question"],
            var_name="answer",
            value_name="value",
        )
        tb["year"] = 2017

        cleaned_tb.append(tb)

    tb = pr.concat(cleaned_tb, ignore_index=True)
    # Improve tables format.
    tables = [tb.format(["country", "year", "question", "answer"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
