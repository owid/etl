"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("mobile_money.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("mobile_money")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["mobile_money"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #

    # Select top-level regions
    df = df[
        df.region.isin(
            [
                "East Asia and Pacific",
                "Europe and Central Asia",
                "Latin America and the Caribbean",
                "South Asia",
                "Middle East and North Africa",
                "Sub-Saharan Africa",
            ]
        )
    ]

    # Exclude NAs and 0s
    df = df[df.active_accounts_90d > 0].dropna()

    # For each region, keep the latest data point in each year
    df = df.sort_values("year")
    df["year"] = df.year.dt.year
    df = df.groupby(["region", "year"], as_index=False).tail(1).reset_index(drop=True)

    # Create a new table with the processed data.
    tb_garden = Table(df, like=tb_meadow)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("mobile_money.end")
