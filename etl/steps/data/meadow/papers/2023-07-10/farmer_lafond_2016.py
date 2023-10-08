"""Load snapshot of Farmer & Lafond (2016) data and create a table.

"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load snapshot.
    snap = paths.load_snapshot("farmer_lafond_2016.csv")
    tb = snap.read_csv()

    #
    # Prepare data.
    #
    # Store the unit of each technology cost from the zeroth row.
    units = dict(zip(tb.columns.tolist()[1:], tb.loc[0][1:]))

    # The zeroth row will be added as metadata, and the first row is not useful, so drop both.
    tb = tb.drop(index=[0, 1]).reset_index(drop=True)

    # Rename year column and make it integer.
    tb = tb.rename(columns={"YEAR": "year"}).astype({"year": int})

    # Add title, units and description to metadata.
    for column in tb.drop(columns=["year"]).columns:
        tb[column].metadata.title = column
        tb[column].metadata.unit = units[column]
        tb[column].metadata.description = f"Cost for {column}, measured in {units[column]}."

    # Ensure all columns are snake-case.
    tb = tb.underscore()

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset and reuse snapshot metadata.
    ds = create_dataset(dest_dir=dest_dir, tables=[tb], default_metadata=snap.metadata, check_variables_metadata=True)
    ds.save()
