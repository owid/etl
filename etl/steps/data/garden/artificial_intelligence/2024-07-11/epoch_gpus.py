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
    ds_meadow = paths.load_dataset("epoch_gpus")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch_gpus"]
    tb = tb.reset_index()

    #
    # Process data.
    #

    # Convert publication date to a datetime objects
    tb["release_date"] = pd.to_datetime(tb["release_date"], format="mixed")

    # Calculate 'days_since_1949'
    tb["days_since_2000"] = (tb["release_date"] - pd.to_datetime("01/01/2000")).dt.days.astype("Int64")
    tb = tb.dropna(subset=["days_since_2000"])

    tb = tb.reset_index(drop=True)

    assert (
        not tb[["name_of_the_hardware", "days_since_2000"]].isnull().any().any()
    ), "Index columns should not have NaN values"
    tb["release_price__usd"] = tb["release_price__usd"].astype(str)
    tb["release_price__usd"] = tb["release_price__usd"].replace({"\$": "", ",": ""}, regex=True).astype(float)

    tb["comp_performance_per_dollar"] = tb["fp32_performance__flop_s"] / tb["release_price__usd"]
    tb = tb.format(["days_since_2000", "name_of_the_hardware"])
    tb = tb.drop(columns=["release_date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
