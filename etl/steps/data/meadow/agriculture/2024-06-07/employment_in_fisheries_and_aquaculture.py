"""Load the employment in fisheries and aquaculture snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load snapshot and read its data.
    snap = paths.load_snapshot()
    tb = snap.read()

    #
    # Process data.
    #
    # Use categoricals for the low-cardinality label columns.
    for column in ["subsector", "region", "period"]:
        tb[column] = tb[column].astype("category")

    # Set an index and sort (the raw table is unique by region, subsector and period).
    tb = tb.format(["region", "subsector", "period"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
