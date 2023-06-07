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
    log.info("monmoth_poll.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("monmoth_poll"))

    # Read table from meadow dataset.
    tb = ds_meadow["monmoth_poll"]
    df = pd.DataFrame(tb)
    df["answer"] = df["answer"].str.replace(r"\(VOL\) ", "", regex=True)
    columns_to_rename = df.columns.difference(["year", "answer"])
    new_column_names = [f"Q{i+1}" for i in range(len(columns_to_rename))]
    column_rename_dict = dict(zip(columns_to_rename, new_column_names))

    df.rename(columns=column_rename_dict, inplace=True)
    df.rename(columns={"answer": "country"}, inplace=True)
    tb_garden = Table(df, short_name="monmoth_poll")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("monmoth_poll.end")
