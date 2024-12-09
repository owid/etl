"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Year constants
YEAR_WPP_START = 1950
YEAR_WPP_PROJ_START = 2023


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_hfd = paths.load_dataset("hfd")
    ds_un = paths.load_dataset("un_wpp")

    # Read table from meadow dataset.
    tb_hfd = ds_hfd.read("period")
    tb_un = ds_un.read("fertility_rate")

    # UN: estimates + medium,
    tb_un = tb_un.loc[
        (tb_un["sex"] == "all") & (tb_un["variant"].isin(["medium", "estimates"]) & (tb_un["age"] == "all")),
        ["country", "year", "fertility_rate"],
    ]

    # HFD: tfr, birth_order=total,
    tb_hfd = tb_hfd.loc[
        ((tb_hfd["birth_order"] == "total") & (tb_hfd["year"] < YEAR_WPP_START)), ["country", "year", "tfr"]
    ].rename(columns={"tfr": "fertility_rate"})

    #
    # Process data.
    #
    tb = pr.concat([tb_hfd, tb_un], ignore_index=True, short_name="fertility_rate")

    # Add historical variant
    tb["fertility_rate_hist"] = tb["fertility_rate"].copy()
    tb.loc[tb["year"] > YEAR_WPP_PROJ_START, "fertility_rate_hist"] = pd.NA

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
