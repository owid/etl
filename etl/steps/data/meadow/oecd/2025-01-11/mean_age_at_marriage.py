"""Load a snapshot and create a meadow dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("mean_age_at_marriage.xls")

    # Load data from snapshot.
    tb = snap.read(safe_types=False, sheet_name="MeanAgeFirstMarriage")

    #
    # Process data.
    #
    # Find the row where the first column contains "Country"
    header_row = tb[tb.iloc[:, 0].str.contains("Country", na=False)].index[0]

    # Set the header row dynamically and drop rows before the header row
    tb.columns = tb.iloc[header_row]
    tb = tb.drop(index=range(header_row + 1)).reset_index(drop=True)
    # Remove the 'note' column
    tb = tb.drop(columns=["Note"], errors="ignore")

    # Remove rows where 'gender' is NaN
    tb = tb.dropna(subset=["Gender"])
    # Fill NaNs in the 'country' column with the previous value
    tb["Country"] = tb["Country"].fillna(method="ffill")
    # Replace "Male" with "men" and "Female" with "women" in the 'gender' column
    tb["Gender"] = tb["Gender"].replace({"Male": "men", "Female": "women"})

    # Melt the DataFrame to create a 'year' column
    tb = tb.melt(id_vars=["Country", "Gender"], var_name="year", value_name="mean_age_at_first_marriage")
    # Convert the 'mean_age_at_first_marriage' column to numeric
    tb["mean_age_at_first_marriage"] = pd.to_numeric(tb["mean_age_at_first_marriage"], errors="coerce").copy_metadata(
        tb["mean_age_at_first_marriage"]
    )

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "gender"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
