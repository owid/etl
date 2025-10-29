"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read table.
    ds_meadow = paths.load_dataset("epoch_compute_cost")
    tb = ds_meadow.read("epoch_compute_cost")

    #
    # Process data.
    #
    # Convert publication date to datetime objects.
    tb["publication_date"] = pd.to_datetime(tb["publication_date"])

    # Calculate days since 1949-01-01.
    tb["days_since_1949"] = (tb["publication_date"] - pd.to_datetime("1949-01-01")).dt.days
    tb["days_since_1949"] = tb["days_since_1949"].astype(int)

    # Validate index columns have no NaN values.
    assert not tb[["model", "days_since_1949"]].isnull().any().any(), "Index columns should not have NaN values"

    # Standardize domain names.
    tb["domain"] = tb["domain"].astype(str)
    # Convert any domain containing "Multimodal" to just "Multimodal".
    tb.loc[tb["domain"].str.contains("Multimodal", na=False), "domain"] = "Multimodal"
    # Convert "Language,Image generation" to "Language and image generation".
    tb.loc[tb["domain"] == "Language,Image generation", "domain"] = "Multimodal"
    tb["publication_date"].metadata.origins = tb["cost__inflation_adjusted"].metadata.origins

    # Format table with index columns.
    tb = tb.format(["days_since_1949", "model"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
