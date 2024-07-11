"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_number = paths.load_snapshot("poverty_projections_number_global.csv")
    snap_share = paths.load_snapshot("poverty_projections_share_regions.csv")

    # Load data from snapshot.
    tb_number = snap_number.read()
    tb_share = snap_share.read()

    #
    # Process data.
    #
    # Rename columns
    tb_number = tb_number.rename(columns={"million_poor": "headcount_215"})
    tb_share = tb_share.rename(
        columns={
            "region": "country",
            "estimate": "headcount_ratio_215_estimated",
            "projection": "headcount_ratio_215_projected",
        }
    )

    # Rename Global to World in tb_share
    tb_share["country"] = tb_share["country"].replace("Global", "World")

    # Add country column to tb_number
    tb_number["country"] = "World"

    # Merge tables
    tb = pr.merge(tb_number, tb_share, on=["country", "year"], how="outer", short_name="poverty_projections")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap_share.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
