"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEAR_WPP_PROJ_START = 2024
YEAR_WPP_START = 1950


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_hmd = paths.load_dataset("hmd")
    ds_un = paths.load_dataset("un_wpp")

    # Read table from meadow dataset.
    tb_hmd = ds_hmd.read("births")
    tb_un = ds_un.read("births")

    #
    # Process data.
    #
    # UN
    tb_un = tb_un.loc[
        (tb_un["age"] == "all") & (tb_un["variant"].isin(["medium", "estimates"])),
        ["country", "year", "birth_rate"],
    ]
    # HMD
    tb_hmd = tb_hmd.loc[
        (tb_hmd["year"] < YEAR_WPP_START) & (tb_hmd["sex"] == "total"), ["country", "year", "birth_rate"]
    ]

    # Combine
    tb = pr.concat([tb_hmd, tb_un], ignore_index=True, short_name="birth_rate")
    tb = tb.dropna(subset=["birth_rate"])

    # Add historical variant
    tb["birth_rate_hist"] = tb["birth_rate"].copy()
    tb.loc[tb["year"] > YEAR_WPP_PROJ_START, "birth_rate_hist"] = pd.NA

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
