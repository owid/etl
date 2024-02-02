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
    snap = paths.load_snapshot("total_fertility_rate.xlsx")

    sheet_names = ["countries_and_territories", "four_regions", "world_total"]

    tb = Table()
    # Load data from snapshot.
    for sheet in sheet_names:
        tb_sheet = snap.read(sheet_name=sheet)
        tb = pr.concat([tb, tb_sheet])

    tb.metadata = snap.to_table_metadata()
    tb = tb.drop(columns=["indicator.name", "geo", "indicator"])
    tb = pr.melt(tb, id_vars=["geo.name"], var_name="Year", value_name="fertility_rate")
    tb = tb.rename(columns={"geo.name": "country", "Year": "year"})
    tb = tb.dropna(subset=["fertility_rate"])

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
