"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("household.xlsx")

    # Load data from snapshot.
    tb = snap.read_excel(sheet_name="countries")
    tb_reg = snap.read_excel(sheet_name="regions")

    tb = tb.format(["country", "year", "residence"], short_name="household_countries")
    tb_reg = tb_reg.format(["region", "region_type", "year", "residence"], short_name="household_regions")

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb, tb_reg]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
