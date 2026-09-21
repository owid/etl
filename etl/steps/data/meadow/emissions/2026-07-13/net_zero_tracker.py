"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("net_zero_tracker.xlsx")

    # Load data from snapshot.
    # NOTE: The data lives in the "Current Snapshot" sheet. Its real column names are in the second
    #  row (the first row groups columns into thematic categories), hence header=1. The file also
    #  ships "README" and "Metadata Glossary" sheets that we don't use.
    tb = snap.read(underscore=True, sheet_name="Current Snapshot", header=1)

    #
    # Process data.
    #
    # Set an appropriate index and sort conveniently.
    # NOTE: There is one row per tracked entity (country, region, city or company), each with a
    #  unique id_code. The garden step keeps only country-level entities.
    tb = tb.format(["id_code"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
