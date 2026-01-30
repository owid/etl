"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("internal_displacement")
    ds_pop = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow.read("internal_displacement")

    tb = tb.rename(columns={"country_name": "country"}, errors="raise")

    tb = tb.drop(columns=["iso3"], errors="raise")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    tb = geo.add_population_to_table(tb, ds_pop)

    # fill n/a values with 0, as no displacements were recorded
    tb = na_to_zero(tb)

    # add total displacements (conflict + disaster)
    tb["total_displacement"] = tb["conflict_total_displacement"] + tb["disaster_total_displacement"]
    tb["total_displacement_rounded"] = tb["total_displacement"].apply(round_idmc_style)
    tb["total_new_displacement"] = tb["conflict_new_displacement"] + tb["disaster_new_displacement"]
    tb["total_new_displacement_rounded"] = tb["total_new_displacement"].apply(round_idmc_style)

    columns_to_calculate = [col for col in tb.columns if col not in ["country", "year", "population"]]

    tb = calculate_shares(tb, columns_to_calculate)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def calculate_shares(tb, columns_to_calculate):
    for displacement_col in columns_to_calculate:
        share_col_name = f"{displacement_col}_per_thousand"
        tb[share_col_name] = (tb[displacement_col] / tb["population"]) * 1000

    tb = tb.drop(columns=["population"], errors="raise")
    return tb


def round_idmc_style(x):
    """Round numbers according to IDMC style.

    - Numbers <= 100,000 are rounded to 2 significant digits.
    - Numbers > 100,000 are rounded to the nearest 1,000.
    Does not work for negative or NaN values.
    """
    if x <= 100000:
        return round(x, -len(str(int(x))) + 2)
    else:
        return round(x, -3)


def na_to_zero(tb):
    """Fill all NaN values with 0. If a country year combination has no row (and no NaN value), create one with 0 values."""
    tb_countries = tb["country"].unique()
    tb_years = tb["year"].unique()
    all_combinations = [(country, year) for country in tb_countries for year in tb_years]

    # fill n/a values with 0, as no displacements were recorded
    displacement_columns = [
        "conflict_total_displacement_rounded",
        "conflict_new_displacement_rounded",
        "disaster_total_displacement_rounded",
        "disaster_new_displacement_rounded",
    ]

    for col in displacement_columns:
        tb[col] = tb[col].fillna(0)

    new_rows = []
    for country, year in all_combinations:
        if not ((tb["country"] == country) & (tb["year"] == year)).any():
            if year == 2008:
                # 2008 only has natural disaster displacements
                new_row = {
                    "country": country,
                    "year": year,
                    "disaster_total_displacement_rounded": 0,
                    "disaster_new_displacement_rounded": 0,
                }
            elif year > 2008:
                new_row = {
                    "country": country,
                    "year": year,
                    "conflict_total_displacement_rounded": 0,
                    "conflict_new_displacement_rounded": 0,
                    "disaster_total_displacement_rounded": 0,
                    "disaster_new_displacement_rounded": 0,
                }
            else:
                raise ValueError(f"Year {year} is before 2008, which is not expected in the dataset.")
            new_rows.append(new_row)
    if new_rows:
        tb_new = Table(pd.DataFrame(new_rows)).copy_metadata(tb)
        tb = pr.concat([tb, tb_new], ignore_index=True)
    return tb
