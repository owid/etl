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
    snap = paths.load_snapshot("sea_ice_index.xlsx")

    # Read data from snapshot.
    data = snap.ExcelFile()

    #
    # Process data.
    #
    # Load sheet of northern hemisphere sea ice extent.
    tb_nh = data.parse("NH-Extent").assign(**{"location": "Northern Hemisphere"})
    tb_sh = data.parse("SH-Extent").assign(**{"location": "Southern Hemisphere"})

    # Sanity check.
    assert tb_nh.iloc[0, 0] == 1978, "First cell in NH spreadsheet was expected to be 1978. Data has changed."
    assert tb_sh.iloc[0, 0] == 1978, "First cell in SH spreadsheet was expected to be 1978. Data has changed."

    # Concatenate both tables.
    tb = pr.concat([tb_sh, tb_nh], ignore_index=True, short_name=paths.short_name)

    # Fix column names.
    tb = tb.rename(columns={tb.columns[0]: "year"})

    # Drop empty rows and columns.
    tb = tb.dropna(how="all").dropna(axis=1, how="all").reset_index(drop=True)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["location", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
