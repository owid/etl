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
    snap = paths.load_snapshot("who_mort_db.csv")

    # Load data from snapshot.
    tb = snap.read(skiprows=6)
    origins = tb["Number"].metadata.origins
    tb = tb.reset_index(drop=True)
    # The column names are shifted by one column to the right.
    tb = clean_data(tb)
    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "sex", "age_group"])
    # Set the origins metadata to the columns.
    for col in tb.columns:
        tb[col].metadata.origins = origins

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def clean_data(tb: Table) -> Table:
    # Get the column names.
    columns = tb.columns
    # Drop the last column, which is empty.
    tb = tb.drop(columns=columns[-1])
    # Rename columns to match the expected
    tb.columns = columns[1:]
    tb = tb.rename(
        columns={"Country Name": "country", "Year": "year", "Sex": "sex", "Age group code": "age_group"}, errors="raise"
    ).drop(columns=["Region Name", "Country Code", "Age Group"])
    return tb
