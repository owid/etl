"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
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
    # Set an appropriate index to both tables and sort conveniently.
    tb = tb.set_index(["observed_month", "prod_type", "prod_process"], verify_integrity=True).sort_index()
    tb_share = tb_share.set_index(["observed_month", "source"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb, tb_share], default_metadata=snap.metadata, check_variables_metadata=True
    )
    ds_meadow.save()
