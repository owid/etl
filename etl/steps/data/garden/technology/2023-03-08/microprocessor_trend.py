"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("microprocessor_trend.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("microprocessor_trend")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["microprocessor_trend"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    # Transistors are counted in thousands
    df["transistors"] = df.transistors * 1000

    # Sort rows by chronological order, and use cummax() to keep the highest-ever number of
    # transistors for each date.
    df = df.sort_values("year")
    df["transistors"] = df.transistors.cummax()

    # Trim dates to years, and keep the maximum number of transistors achieved each year
    df["year"] = df.year.astype(int)
    df = df.groupby(["year", "region"], as_index=False).max().reset_index(drop=True)

    # Create a new table with the processed data.
    tb_garden = Table(df, like=tb_meadow)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("microprocessor_trend.end")
