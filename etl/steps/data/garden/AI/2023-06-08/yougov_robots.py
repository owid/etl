"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("yougov_robots.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("yougov_robots"))

    # Read table from meadow dataset.
    tb = ds_meadow["yougov_robots"]
    df = pd.DataFrame(tb)

    # Create a date column (counting days since 2021-01-01)
    df["days_since_2021"] = (
        pd.to_datetime(df["year"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2021-01-01")
    ).dt.days
    df = df.drop("year", axis=1)
    df.rename(columns={"days_since_2021": "year"}, inplace=True)

    # Create a pivot table for each demographic group
    pivot_df = df.pivot_table(index=[df.columns[0], "year"], columns="group", values="value").reset_index()
    pivot_df = pivot_df.rename_axis(None, axis=1)
    pivot_df.rename(columns={pivot_df.columns[0]: "country"}, inplace=True)

    tb = Table(pivot_df, short_name="yougov_robots", underscore=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("yougov_robots.end")
