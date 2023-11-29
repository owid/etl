"""Load a snapshot and create a meadow dataset."""


from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def replace_country_codes(tb: Table) -> Table:
    """The exported file keeps some country codes instead of names. In this function I replace them."""
    country_dict = {
        "17": "Saudi Arabia",
        "22": "Yemen",
        "2": "Palestine",
        "3": "Algeria",
        "4": "Morocco",
        "6": "Lebanon",
    }

    tb["country"] = tb["country"].replace(country_dict)

    # Set indices, verify integrity and sort.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("arab_barometer_trust.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    # Replace country codes with country names.
    tb = replace_country_codes(tb)

    # Create a new table and ensure all columns are snake-case.
    tb = tb.underscore()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()
