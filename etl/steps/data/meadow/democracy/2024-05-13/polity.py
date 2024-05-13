"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("polity.xlsx")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Yugoslavia ccodes: 347, 345
    tb = correct_yugoslavia_data(tb)
    # Ethiopia ccodes: 529, 530
    tb = correct_ethiopia_data(tb)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def correct_yugoslavia_data(tb: Table) -> Table:
    """There is a duplicate entry with name Yugoslavia in 1991.

    Reason 'Serbia and Montenegro' appears with name 'Yugoslavia' between 1991 and 2002.

    Since this entity has ccode = 347 we can easily fix it by filtering the table by this ccode.
    """
    tb.loc[tb["ccode"] == 347, "country"] = "Serbia and Montenegro"
    return tb


def correct_ethiopia_data(tb: Table) -> Table:
    """There is a duplicate entry with name Ethiopia in 1993.

    ccodes:
        530: Ethiopia (former)
        529: Ethiopia
    """
    tb.loc[tb["ccode"] == 530, "country"] = "Ethiopia (former)"
    return tb
