"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Year constants
YEAR_WPP_START = 1950
YEAR_WPP_PROJ_START = 2023
# Table names
TABLE_NAME_WPP = "mean_age_childbearing"
TABLE_NAME_HFD = "period"
TABLE_NAME_NEW = "mean_age_childbearing"
# Metric names
COLUMN_NAME_WPP = "mean_age_childbearing"
COLUMN_NAME_HFD = "mab"
COLUMN_NEW_NAME = "mean_age_childbearing"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_hfd = paths.load_dataset("hfd")
    ds_un = paths.load_dataset("un_wpp")

    # Read table from meadow dataset.
    tb_hfd = ds_hfd.read(TABLE_NAME_HFD)
    tb_un = ds_un.read(TABLE_NAME_WPP)

    # UN: estimates + medium,
    tb_un = tb_un.loc[
        (tb_un["sex"] == "all") & (tb_un["variant"].isin(["medium", "estimates"]) & (tb_un["age"] == "all")),
        ["country", "year", COLUMN_NAME_WPP],
    ].rename(columns={COLUMN_NAME_WPP: COLUMN_NEW_NAME})

    # HFD: tfr, birth_order=total,
    tb_hfd = tb_hfd.loc[
        ((tb_hfd["birth_order"] == "total") & (tb_hfd["year"] < YEAR_WPP_START)), ["country", "year", COLUMN_NAME_HFD]
    ].rename(columns={COLUMN_NAME_HFD: COLUMN_NEW_NAME})

    # Concatenate
    tb = pr.concat([tb_hfd, tb_un], ignore_index=True, short_name=TABLE_NAME_NEW)

    # Add historical variant
    tb[f"{COLUMN_NEW_NAME}_hist"] = tb[COLUMN_NEW_NAME].copy()
    tb.loc[tb["year"] > YEAR_WPP_PROJ_START, f"{COLUMN_NEW_NAME}_hist"] = pd.NA

    # Format
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
