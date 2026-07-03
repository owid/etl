"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("cropland_projections.zip")

    # Load data from snapshot.
    with snap.extracted() as archive:
        tb = archive.read("FOFA2050CountryData_Crop-production.csv")

    #
    # Process data.
    #
    # The domain column is constant ("Crop Production"), and the region column (FAO's own grouping of countries) and
    # country codes are not needed.
    tb = tb.drop(columns=["Domain", "CountryCode", "Region"], errors="raise")

    # Rename country column (the rest will be underscored by the format() below).
    tb = tb.rename(columns={"CountryName": "country"}, errors="raise")

    # Improve table format.
    tb = tb.format(["indicator", "item", "element", "country", "scenario", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
