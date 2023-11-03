"""Load a snapshot and create a meadow dataset."""

import numpy as np
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def read_data(snap: Snapshot) -> Table:
    temp = snap.read_excel(header=None)
    line_number_start = [i for i, line in enumerate(temp[temp.columns[0]]) if str(line).strip().lower() == "country"][0]
    line_number_end = [i for i, line in enumerate(temp[temp.columns[0]]) if str(line).strip().lower() == "total"][0]
    tb = snap.read_excel(skiprows=line_number_start, nrows=line_number_end - line_number_start)

    return tb


def process_data(tb: Table) -> Table:
    tb = tb.copy()

    # Remove empty rows, if any.
    tb = tb.dropna(how="all")

    # Check that year range is as expected.
    assert set(tb["Year"].values[:-1]) == {"2007-2016"}

    # Assign a year column (use the latest year in the range).
    tb["Year"] = 2016

    # Last row contains totals. We want to keep them, but remove those columns whose value is "Total".
    tb = tb.replace({"Total": np.nan})
    tb.loc[len(tb) - 1, "Country"] = "World"

    # Ensure all columns are underscore.
    tb = tb.underscore()

    return tb


def run(dest_dir: str) -> None:
    #
    # Load and process inputs.
    #
    # Load snapshot.
    snap = paths.load_snapshot("number_of_wild_fish_killed_for_food.xlsx")
    tb = read_data(snap=snap)

    #
    # Process data.
    #
    # Process yearly data.
    tb = process_data(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = (
        tb.set_index(["country", "year", "fao_species_category"], verify_integrity=True).sort_index().sort_index(axis=1)
    )

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
