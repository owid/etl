"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("under_five_mortality.xlsx")

    sheet_names = ["data-for-world-by-year", "data-for-regions-by-year", "data-for-countries-etc-by-year"]

    tb = Table()
    # Load data from snapshot.
    for sheet in sheet_names:
        tb_sheet = snap.read(sheet_name=sheet)
        tb = pr.concat([tb, tb_sheet])

    tb.metadata = snap.to_table_metadata()
    tb = tb.rename(columns={"name": "country", "time": "year"})
    tb = tb.drop(columns=["geo"])
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
