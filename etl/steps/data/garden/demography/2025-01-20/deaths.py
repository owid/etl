"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# UN WPP relevant years
YEAR_UN_START = 1950
YEAR_WPP_PROJ_START = 2024


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_hmd = paths.load_dataset("hmd")
    tb_hmd = ds_hmd.read("deaths_agg")
    ds_un = paths.load_dataset("un_wpp")
    tb_un = estimate_death_rate_from_un(ds_un)

    #
    # Process data.
    #
    # Keep relevant columns
    columns = ["country", "year", "sex", "death_rate", "deaths"]
    tb_un = tb_un.loc[:, columns]
    tb_hmd = tb_hmd.loc[:, columns]

    # Harmonize sex dimension
    tb_un["sex"] = tb_un["sex"].replace({"all": "total"})
    assert set(tb_un["sex"].unique()) == {"female", "male", "total"}
    assert set(tb_hmd["sex"].unique()) == {"female", "male", "total"}

    # Concatenate tables
    tb_hmd = tb_hmd.loc[tb_hmd["year"] < YEAR_UN_START]
    tb = pr.concat([tb_hmd, tb_un])

    # Add historical variant
    tb["death_rate_hist"] = tb["death_rate"].copy()
    tb.loc[tb["year"] >= YEAR_WPP_PROJ_START, "death_rate_hist"] = pd.NA
    tb["deaths_hist"] = tb["deaths"].copy()
    tb.loc[tb["year"] >= YEAR_WPP_PROJ_START, "deaths_hist"] = pd.NA

    # Format
    tb = tb.format(["country", "year", "sex"], short_name="deaths")

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


def estimate_death_rate_from_un(ds_un):
    """We re-estimate the death rate bc the UN WPP does not provide it broken down by sex."""
    tb_pop = ds_un.read("population")
    tb_deaths = ds_un.read("deaths")

    # Keep past + medium projection
    tb_pop = tb_pop[tb_pop["variant"].isin(["estimates", "medium"])]
    tb_deaths = tb_deaths[tb_deaths["variant"].isin(["estimates", "medium"])]

    # Keep only age='all'
    tb_pop = tb_pop[tb_pop["age"] == "all"]
    tb_deaths = tb_deaths[tb_deaths["age"] == "all"]

    # Drop columns
    tb_pop = tb_pop.drop(columns=["age", "variant"])
    tb_deaths = tb_deaths.drop(columns=["age", "variant"])

    # Merge
    tb = tb_deaths.merge(tb_pop, on=["country", "year", "sex"], validate="1:1")

    # Death rate
    tb["death_rate"] = tb["deaths"] / tb["population"] * 1000
    return tb
