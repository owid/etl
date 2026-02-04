"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("census_us_idb.zip")

    # Load data from snapshot.
    with snap.extracted() as archive:
        tb = archive.read("idb5yr.txt", delimiter="|", force_extension="csv")

    #
    # Process data.
    #
    tb = tb.rename(
        columns={
            "#YR": "year",
        }
    )

    # Improve tables format.
    tables = [
        tb.format(["geo_id", "year"]),
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(
        tables=tables,
        default_metadata=snap.metadata,
    )

    # Save meadow dataset.
    ds_meadow.save()
