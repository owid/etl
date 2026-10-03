"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("literacy_1950.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    tb = tb[["Country", "Estimated % Adult Illiterate"]]
    tb["year"] = (
        1950  # Estimated population and extent of illiteracy around 1950; for more information go to https://unesdoc.unesco.org/ark:/48223/pf0000002930
    )
    tb = tb.rename(columns={"Estimated % Adult Illiterate": "illiteracy_rate"})
    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
