"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

from datetime import datetime
import pandas as pd

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("near_earth_asteroids.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.

    # Load the CSV file
    tb = tb[["Date", "NEA-km", "NEA-140m", "NEA"]]

    # Sort by Date in descending order
    tb["Date"] = pd.to_datetime(tb["Date"])
    tb = tb.sort_values(by="Date", ascending=False)

    # Extract year from Date
    tb["year"] = tb["Date"].dt.year

    # Filter out data from the current year
    current_year = datetime.now().year
    tb = tb[tb["year"] < current_year]

    # Keep only the latest record for each year
    tb = tb.drop_duplicates(subset="year")

    # Calculate additional columns
    tb["larger_than_1km"] = tb["NEA-km"]
    tb["between_140m_and_1km"] = tb["NEA-140m"] - tb["larger_than_1km"]
    tb["smaller_than_140m"] = tb["NEA"] - tb["larger_than_1km"] - tb["between_140m_and_1km"]

    # Add the 'country' column
    tb["country"] = "World"

    # Select the final columns
    tb = tb[["country", "year", "larger_than_1km", "between_140m_and_1km", "smaller_than_140m"]]

    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
