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

    #
    # 1/ Combine fertility rate from HFD and UN WPP datasets.
    #

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

    # Sanity check: UN starts at 1950
    assert tb_un["year"].min() == YEAR_WPP_START, f"UN WPP data does not start at {YEAR_WPP_START}."

    # Combine
    tb = pr.concat([tb_hfd, tb_un], ignore_index=True, short_name="fertility_rate")

    # Add historical variant
    tb["fertility_rate_hist"] = tb["fertility_rate"].copy()
    tb.loc[tb["year"] > YEAR_WPP_PROJ_START, "fertility_rate_hist"] = pd.NA

    # Format
    tb = tb.format(["country", "year"])

    #
    # 2/ Combine fertility rate by age (distribution)
    #

    # Read table from meadow dataset.
    tb_hfd = ds_hfd.read("period_ages_years")
    tb_un = ds_un.read("fertility_single")

    # Rename columns
    tb_un = tb_un.rename(columns={"fertility_rate": "asfr"})
    tb_hfd = tb_hfd.rename(columns={"asfr_period": "asfr"})

    # # Adjust metrics
    # tb_un["asfr"] /= 1_000

    # Filter: Keep HFD for >1950, but include countries not in UN
    countries_hfd = set(tb_hfd["country"].unique())
    countries_un = set(tb_un["country"].unique())
    countries_not_in_un = countries_hfd - countries_un
    tb_hfd = tb_hfd.loc[(tb_hfd["year_as_dimension"] < YEAR_WPP_START) | (tb_hfd["country"].isin(countries_not_in_un))]

    # Ensure min year in UN is 1950
    assert tb_un["year_as_dimension"].min() == YEAR_WPP_START, f"UN WPP data does not start at {YEAR_WPP_START}."

    # Combine
    tb_by_age = pr.concat([tb_hfd, tb_un], ignore_index=True)

    # Format
    tb_by_age = tb_by_age.format(["country", "age", "year_as_dimension"], short_name="fertility_rate_by_age")

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_by_age,
    ]

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
