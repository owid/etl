"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("epoch_compute_cost")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch_compute_cost"].reset_index()

    #
    # Process data.
    #
    # Convert publication date to a datetime objects
    tb["publication_date"] = pd.to_datetime(tb["publication_date"])

    # Calculate 'days_since_1949'
    tb["days_since_1949"] = (tb["publication_date"] - pd.to_datetime("1949-01-01")).dt.days
    tb["days_since_1949"] = tb["days_since_1949"].astype(int)

    assert not tb[["system", "days_since_1949"]].isnull().any().any(), "Index columns should not have NaN values"

    # Add metadata to the publication date column
    tb["publication_date"].metadata.origins = tb["cost__inflation_adjusted"].metadata.origins
    tb = tb.format(["days_since_1949", "system"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
