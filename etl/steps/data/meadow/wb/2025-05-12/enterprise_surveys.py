"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("enterprise_surveys.xlsx")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb = tb.drop(columns=["Subgroup", "Top Subgroup Level", "Subgroup Level", "Average/SE/N"])
    tb = tb.rename(columns={"Economy": "country"})

    # List of regions to update to 2017
    regions_to_update = [
        "All",
        "East Asia & Pacific",
        "Europe & Central Asia",
        "Latin America & Caribbean",
        "Middle East & North Africa",
        "North America",
        "South Asia",
        "Sub-Saharan Africa",
    ]

    # Note that the regional and global averages are actually computed by taking the most recent survey results from each country.
    # Count non-NaN Year values for each country in that subset
    valid_counts = tb.groupby("country")["Year"].apply(lambda x: x.notna().sum())

    # Identify countries with more than 10 non-NaN values
    countries_to_update = valid_counts[valid_counts > 10].index

    # Find the max Year across the whole DataFrame
    most_recent_year = tb["Year"].max()
    # Update the Year for only the selected countries
    tb.loc[tb["country"].isin(countries_to_update), "Year"] = most_recent_year
    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
