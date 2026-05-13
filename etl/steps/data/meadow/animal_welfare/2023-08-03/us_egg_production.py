"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and load data for number of produced eggs.
    snap = paths.load_snapshot("us_egg_production.csv")
    tb = snap.read_csv()

    # Retrieve snapshot and load data for share of produced eggs that are cage-free.
    tb_share = paths.load_snapshot("us_egg_production_share_cage_free.csv").read_csv()

    #
    # Process data.
    #
    # Improve table formats.
    tb = tb.format(keys=["observed_month", "prod_type", "prod_process"])
    tb_share = tb_share.format(keys=["observed_month", "source"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb, tb_share], default_metadata=snap.metadata)
    ds_meadow.save()
