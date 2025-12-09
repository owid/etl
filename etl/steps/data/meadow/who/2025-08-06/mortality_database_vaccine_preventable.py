"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("mortality_database_vaccine_preventable.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    tb = tb.drop(columns=["Region Code", "Country Code", "Age group code", "Region Name"])
    tb = tb.rename(columns={"Country Name": "country"}, errors="raise")
    tables = [tb.format(["country", "year", "sex", "age_group", "cause"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
