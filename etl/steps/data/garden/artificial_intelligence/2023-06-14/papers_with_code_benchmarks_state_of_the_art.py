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
    """
    Combine all Paperswithcode benchmarks in meadow into one garden dataset.

    """

    log.info("papers_with_code_benchmarks_state_of_the_art.start")

    #
    # Load inputs - Atari
    #
    # Load meadow dataset for Atari.
    ds_garden = cast(Dataset, paths.load_dependency("papers_with_code_benchmarks"))
    tb = ds_garden["papers_with_code_benchmarks"]
    # Assuming your DataFrame is called "df"
    corresponding_columns = []
    selected_data = {}
    for col in tb.columns:
        if col.endswith("_improved"):
            corresponding_col = col[:-9]
            corresponding_columns.append(corresponding_col)
            state_of_the_art_rows = tb[tb[col].str.contains("State of the art", na=False)]
            selected_values = state_of_the_art_rows[corresponding_col]
            selected_data[corresponding_col] = selected_values

    # Create a new DataFrame from the selected data
    new_df = pd.DataFrame(selected_data)
    max_values = new_df.groupby("days_since").max()
    max_values.columns = max_values.columns + "_state_of_the_art"

    tb = Table(max_values, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("papers_with_code_benchmarks_state_of_the_art.end")
