"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("chupilkin_koczan")
    # Read table from meadow dataset.
    tb = ds_meadow["chupilkin_koczan"].reset_index()

    # State system data
    ds_meadow = paths.load_dataset(short_name="cow_ssm")
    # Read table from meadow dataset.
    tb_system = ds_meadow["cow_ssm_system"].reset_index()

    #
    # Process data.
    #
    # Clean tb_system
    tb_system = tb_system[["statenme", "year", "ccode"]].rename(
        columns={
            "statenme": "country",
        }
    )

    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, country_col="statename")
    # Add an entry for year in period
    tb = expand_observations(tb)
    # Group by country and year
    tb = tb.groupby(["statename", "year"], as_index=False, observed=True).agg(
        {"on_territory": lambda x: min(1, sum(x))}
    )
    # Rename columns
    tb = tb.rename(
        columns={
            "statename": "country",
            "on_territory": "is_location_of_conflict",
        }
    )
    # Quick fixes
    tb["country"] = tb["country"].replace(
        {
            "Ottoman Empire": "Turkey",
            "USSR": "Russia",
            "Sardinia/Piedmont": "Italy",
            "Prussia": "Germany",
        }
    )

    # Preserve metadata
    table_meta = tb.m
    indicator_meta = tb["is_location_of_conflict"].m

    # Merge with complete country-year list (state system dataset): ssm <- chupilkin
    x = tb.merge(tb_system, on=["country", "year"], how="left")
    assert (
        len(x[x["ccode"].isna()]) == 2
    ), "There were two entries expected to have missing match between Chupilkin et al. and SSM!"
    tb = tb_system.merge(tb, on=["country", "year"], how="left")

    # Bring metadata back
    tb.metadata = table_meta
    tb["is_location_of_conflict"].metadata = indicator_meta

    # Fill missing values with zero
    tb["is_location_of_conflict"] = tb["is_location_of_conflict"].fillna(0)

    # Set index
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def expand_observations(tb: Table) -> Table:
    """Add all years in the period."""
    YEAR_MIN = tb["startyear1"].min()
    YEAR_MAX = tb["endyear"].max()

    tb_all_years = Table(pd.RangeIndex(YEAR_MIN, cast(int, YEAR_MAX) + 1), columns=["year"])
    tb = tb.merge(tb_all_years, how="cross")

    # Filter only entries that actually existed
    tb = tb[(tb["year"] >= tb["startyear1"]) & (tb["year"] < tb["endyear"])]

    return tb
