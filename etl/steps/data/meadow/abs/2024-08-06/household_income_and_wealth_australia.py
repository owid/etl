"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("household_income_and_wealth_australia.zip")

    # Load data from snapshot.
    tb = snap.read_in_archive(
        "1. Household income and income distribution, Australia.xlsx", sheet_name="Table 1.1", skiprows=4, nrows=43
    )

    # Rename unnamed columns.
    tb = tb.rename(columns={"Unnamed: 0": "indicator", "Unnamed: 1": "unit"})

    # remove rows with missing values in indicator
    tb = tb.dropna(subset=["indicator"])

    # Remove rows where data is missing for all columns except indicator and unit
    tb = tb.dropna(subset=[col for col in tb.columns if col not in ["indicator", "unit"]], how="all")

    # Transform indicator column to indicator + unit
    tb["indicator"] = tb["indicator"] + " - " + tb["unit"]

    # Drop unit column
    tb = tb.drop(columns=["unit"])

    # Make table long
    tb = tb.melt(id_vars=["indicator"], var_name="year", value_name="value")

    # Now make table wide with indicator as columns
    tb = tb.pivot(index="year", columns="indicator", values="value").reset_index()

    # Add country
    tb["country"] = "Australia"

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
