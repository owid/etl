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
    log.info("ai_conference_type.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow_ai_small = cast(Dataset, paths.load_dependency("ai_small_conferences"))
    ds_meadow_ai_large = cast(Dataset, paths.load_dependency("ai_large_conferences"))
    ds_meadow_ai_total = cast(Dataset, paths.load_dependency("ai_conferences_total"))

    # Read table from meadow dataset.
    tb_small = ds_meadow_ai_small["ai_small_conferences"]
    tb_large = ds_meadow_ai_large["ai_large_conferences"]
    tb_total = ds_meadow_ai_total["ai_conferences_total"]

    df_small = pd.DataFrame(tb_small)
    df_large = pd.DataFrame(tb_large)
    df_total = pd.DataFrame(tb_total)

    df_total["conference"] = "Total"
    df_small_tot = pd.concat([df_small, df_total], axis=0, join="inner").reset_index(drop=True)
    df_all = pd.concat([df_small_tot, df_large], axis=0, join="inner").reset_index(drop=True)

    df_all["number_of_attendees__in_thousands"] = df_all["number_of_attendees__in_thousands"].apply(
        lambda x: round(x * 1000)
    )
    tb = Table(df_all, short_name="ai_all_conferences")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow_ai_large.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("ai_conference_type.end")
