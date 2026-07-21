"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("worldwide_bureaucracy_indicators.zip")

    # The archive contains the data file (WWBICSV.csv) plus country and series metadata files.
    with snap.extracted() as archive:
        tb = archive.read("WWBICSV.csv")
        tb_series = archive.read("WWBISeries.csv")

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country_name", "indicator_code"], short_name=paths.short_name)
    tb_series = tb_series.format(["series_code"], short_name="series_metadata")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb, tb_series], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
