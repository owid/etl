from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("penn_world_table_national_accounts.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Data")

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["countrycode", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
