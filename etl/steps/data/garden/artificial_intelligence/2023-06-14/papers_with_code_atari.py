"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
import shared
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("papers_with_code_atari.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("papers_with_code_atari"))
    tb = ds_meadow["papers_with_code_atari"]

    df = pd.DataFrame(tb)

    df["year"] = pd.to_datetime(df["date"]).dt.year
    df = df.drop("date", axis=1)
    df["performance_atari"] = df["performance_atari"] * 100

    pivot_df = shared.select_best_on_date(df, "year")
    tb = Table(pivot_df, short_name="papers_with_code_atari")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("papers_with_code_atari.end")
