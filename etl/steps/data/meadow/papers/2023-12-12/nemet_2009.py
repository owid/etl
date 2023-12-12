"""Load snapshot of Nemet (2009) data and create a table.

"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)

# Columns to select from snapshot, and how to rename them.
COLUMNS = {
    "Cost (2004 USD/Watt)": "cost",
    "Time (Year)": "year",
    "Yearly Capacity (MW)": "yearly_capacity",
    "Previous Capacity (MW)": "previous_capacity",
}


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load snapshot.
    snap = paths.load_snapshot("nemet_2009.csv")
    tb = snap.read_csv()

    #
    # Process data.
    #
    tb = tb.rename(columns=COLUMNS, errors="raise")[COLUMNS.values()]

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset and reuse snapshot metadata.
    ds = create_dataset(dest_dir=dest_dir, tables=[tb], default_metadata=snap.metadata)
    ds.save()
