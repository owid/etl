"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("findex.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Data")
    tb_metadata = snap.read(sheet_name="Series table")

    #
    # Process data.
    #

    tb = tb.drop(columns={"Country code", "Adult populaiton", "Region", "Income group"})
    tb = tb.rename(columns={"Country name": "country"})
    tb = tb.melt(
        id_vars=["country", "Year"],
        var_name="Indicator Name",
        value_name="value",
    )
    tb_metadata = tb_metadata[["Indicator Name", "Short definition", "Long definition"]]
    tb = pr.merge(tb, tb_metadata, on="Indicator Name", how="left")
    # Improve tables format.
    tables = [tb.format(["country", "year", "indicator_name"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
