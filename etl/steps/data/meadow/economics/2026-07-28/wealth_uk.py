"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Rename the source columns to descriptive, snake-case indicator names.
# "share_top_10_5" is the share held by the group inside the top 10% but outside the top 5%, and so on.
COLUMNS = {
    "Year": "year",
    "Bottom 90%": "share_bottom_90",
    "Top 10%": "share_top_10",
    "Top 5%": "share_top_5",
    "Top 1%": "share_top_1",
    "Top 0.5%": "share_top_0p5",
    "Top 0.1%": "share_top_0p1",
    "Top 10-5%": "share_top_10_5",
    "Top 5-1%": "share_top_5_1",
    "Top 1-0.5%": "share_top_1_0p5",
    "Top 0.5-0.1%": "share_top_0p5_0p1",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("wealth_uk.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Rename columns to descriptive indicator names.
    tb = tb.rename(columns=COLUMNS, errors="raise")

    # This is a UK-only source; add the country column explicitly.
    tb["country"] = "United Kingdom"
    tb["country"] = tb["country"].astype("category")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
