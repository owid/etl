"""Load a snapshot and create a meadow dataset.

Adapted from Ed's original code.
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load snapshot.
    snap = paths.load_snapshot("near_earth_asteroids.csv")

    # Read data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Select columns.
    tb = tb[["Date", "NEA-km", "NEA-140m", "NEA"]]

    # Sort conveniently.
    tb = tb.sort_values(by="Date", ascending=False)

    # Add a year column.
    tb["year"] = tb["Date"].str[0:4].astype(int)

    # Filter out data from the current year, which is incomplete.
    current_year = int(snap.metadata.origin.date_published[0:4])
    tb = tb[tb["year"] < current_year]

    # Keep only the latest record for each year.
    tb = tb.drop_duplicates(subset="year")

    # Create additional indicators.
    tb["larger_than_1km"] = tb["NEA-km"]
    tb["between_140m_and_1km"] = tb["NEA-140m"] - tb["larger_than_1km"]
    tb["smaller_than_140m"] = tb["NEA"] - tb["larger_than_1km"] - tb["between_140m_and_1km"]

    # Add a country column.
    tb["country"] = "World"

    # Select only necessary columns.
    tb = tb[["country", "year", "larger_than_1km", "between_140m_and_1km", "smaller_than_140m"]]

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
