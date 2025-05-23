"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
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

    # Read US consumer prices table from garden dataset.
    ds_us_cpi = paths.load_dataset("us_consumer_prices")

    tb_us_cpi = ds_us_cpi["us_consumer_prices"]
    tb_us_cpi = tb_us_cpi.reset_index()

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

    # Extract year from 'release_date' and create a new 'year' column
    tb["year"] = tb["release_date"].dt.year

    # Adjust CPI values so that 2023 is the reference year (2023 = 100)
    cpi_2023 = tb_us_cpi.loc[tb_us_cpi["year"] == 2023, "all_items"].values[0]

    # Adjust 'all_items' column by the 2023 CPI
    tb_us_cpi["cpi_adj_2023"] = tb_us_cpi["all_items"] / cpi_2023
    tb_us_cpi_2023 = tb_us_cpi[["cpi_adj_2023", "year"]].copy()
    tb_cpi = pr.merge(tb, tb_us_cpi_2023, on="year", how="left")

    tb_cpi["release_price__usd"] = round(tb_cpi["release_price__usd"] / tb_cpi["cpi_adj_2023"])
    tb_cpi = tb_cpi.drop("cpi_adj_2023", axis=1)

    tb_cpi["comp_performance_per_dollar"] = (
        tb_cpi["fp32__single_precision__performance__flop_s"] / tb["release_price__usd"]
    )
    tb_cpi = tb_cpi.drop(columns=["release_date", "year"])

    tb_cpi = tb_cpi.format(["days_since_2000", "name_of_the_hardware"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_cpi], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
