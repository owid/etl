"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("ai_robots.start")

    #
    # Load inputs.
    #
    # Load Snapshot
    ds_meadow = cast(Dataset, paths.load_dependency("ai_robots"))

    # Read table from meadow dataset.
    tb = ds_meadow["ai_robots"]
    # Iterate over the columns
    for column in tb.columns:
        # Check if the column includes "in_thousands"
        if "__in_thousands" in column:
            # Multiply the values by 1000
            tb[column] = tb[column].apply(lambda x: x * 1000)
            new_column_name = column.replace("in_thousands", "")

            # Rename the column
            tb.rename(columns={column: new_column_name}, inplace=True)

            # Convert the column values to numeric
            tb[new_column_name] = pd.to_numeric(tb[new_column_name], errors="coerce")
    #
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_bills.end")
