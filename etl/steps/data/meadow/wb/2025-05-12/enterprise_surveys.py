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

    # No year is provided for regions, so we set it to 2024
    # Note that the regional and global averages are actually computed by taking the most recent survey results from each country.
    tb.loc[tb["country"].isin(regions_to_update), "Year"] = tb.loc[tb["country"].isin(regions_to_update), "Year"] = tb[
        "Year"
    ].max()

    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
