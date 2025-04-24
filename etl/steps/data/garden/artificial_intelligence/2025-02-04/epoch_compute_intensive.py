"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    paths.log.info("epoch.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("epoch_compute_intensive")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch_compute_intensive"]
    tb = tb.reset_index()

    #
    # Process data.
    #
    # Convert FLOP to petaFLOP and remove the column with FLOPs (along with training time in hours)
    tb["training_computation_petaflop"] = tb["training_compute__flop"] / 1e15

    # Convert publication date to a datetime objects
    tb["publication_date"] = pd.to_datetime(tb["publication_date"])

    # Calculate 'days_since_1949'
    tb["days_since_1949"] = (tb["publication_date"] - pd.to_datetime("1949-01-01")).dt.days.astype("Int64")
    tb = tb.dropna(subset=["days_since_1949"])

    tb = tb.reset_index(drop=True)

    assert not tb[["model", "days_since_1949"]].isnull().any().any(), "Index columns should not have NaN values"

    # Drop columns that are not needed
    tb = tb.drop(
        ["training_compute__flop", "organization", "authors", "country__from_organization"],
        axis=1,
    )
    tb = tb.format(["days_since_1949", "model"])

    # Add metadata to the publication date column
    tb["publication_date"].metadata.origins = tb["domain"].metadata.origins

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("epoch_compute_intensive.end")
