"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMNS_UN = ["location", "year", "age", "sex", "probability_of_survival"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_un_lt = paths.load_dataset("un_wpp_lt")
    ds_un_wpp = paths.load_dataset("un_wpp")

    ds_hmd = paths.load_dataset("hmd")
    ds_hfd = paths.load_dataset("hfd")

    # Load tables
    tb_un = ds_un_lt.read("un_wpp_lt")
    tb_un_proj = ds_un_lt.read("un_wpp_lt_proj")

    # Concatenate
    tb_un = pr.concat([tb_un, tb_un_proj], ignore_index=True)

    # Filter 'total' and 'female'
    tb_un = tb_un.loc[tb_un["sex"].isin(["total", "female"]), COLUMNS_UN]

    # Dtypes
    tb_un["age"] = tb_un["age"].str.replace("100+", "100").astype("UInt16")

    # Scale
    tb_un["probability_of_survival"] /= 100

    # Cumulative product
    # We estimate the cumulative survival probability. This is the probability to survive from birth to a given age.
    # The source provides the probability to survive from one age to the next (pn = probability to survive age n to n+1).
    # To estimate this for people born in 1950, we need the data of p0 in 1950, p1 in 1951, etc. That's why we create year_born.
    # After that, we just do the cumulative product for each year_born.
    # Note that for the cumulative product to make sense, we need to first sort table by age!
    # Step 1: Create year_born
    tb_un["year_born"] = tb_un["year"] - tb_un["age"]
    # Step 2: We only estimate the cumulative survival probability for people born between 1950 and 2023 (reduction of 50% rows)
    tb_un = tb_un.loc[(tb_un["year_born"] >= 1950) & (tb_un["year_born"] <= 2023)]
    # Step 3: Sort by age
    tb_un = tb_un.sort_values(["location", "year_born", "sex", "age"])
    # Step 4: Estimate cumulative survival probability
    tb_un["cumulative_survival"] = tb_un.groupby(["location", "sex", "year_born"])["probability_of_survival"].cumprod()
    # Step 5: Keep only years of interest (15-65), further reduction of 50% rows
    tb_un = tb_un.loc[(tb_un["age"] >= 15) & (tb_un["age"] <= 65)]
    # Step 6: Drop columns
    tb_un = tb_un.drop(columns=["year_born"])

    # Read table from meadow dataset.
    tb = ds_un.read("efr_malani_jacob")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
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
