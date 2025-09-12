"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gender_statistics.zip")

    # Load data from snapshot.
    tb = snap.read_in_archive("Gender_StatsCSV.csv", low_memory=False, safe_types=False)

    columns_to_drop = ["Country Code", "Indicator Name"]
    tb = tb.drop(columns=columns_to_drop)
    tb = tb.rename(columns={"Country Name": "country"})

    tb = tb.melt(
        id_vars=["country", "Indicator Code"],
        var_name="year",
        value_name="value",
    )

    #
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country", "year", "indicator_code"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
