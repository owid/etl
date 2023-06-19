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
    log.info("papers_with_code_math_code_language.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("papers_with_code_math_code_language"))

    # Read table from meadow dataset.
    tb = ds_meadow["papers_with_code_math_code_language"]
    df = pd.DataFrame(tb)

    df["days_since"] = (
        pd.to_datetime(df["date"].astype(str), format="%Y-%m-%d") - pd.to_datetime("2019-01-01")
    ).dt.days
    df = df.drop("date", axis=1)

    pivot_df = shared.select_best_on_date(df, "days_since")
    #
    # Process data.
    #
    tb = Table(pivot_df, short_name="papers_with_code_math_code_language")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=None)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("papers_with_code_math_code_language.end")
