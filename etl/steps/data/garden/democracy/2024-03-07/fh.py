"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from shared import add_imputes, add_regions_and_global_aggregates, from_wide_to_long, make_table_with_dummies

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
PATH_IMPUTE = paths.directory / "fh.countries_impute.yml"
# Minimum dataset year expected
YEAR_MIN = 1972

# Regions
REGIONS = {
    "Africa": {},
    "Asia": {
        "additional_members": [
            "Chechnya",
            "West Bank and Gaza Strip",
            "West Bank",
            "Israeli-Occupied Territories",
            "Palestinian Authority-Administered Territories",
            "Gaza strip",
            "Indian Kashmir",
            "Pakistani Kashmir",
            "Kurdistan",
            "North Vietnam",
            "Tibet",
        ]
    },
    "North America": {},
    "South America": {},
    "Europe": {
        "additional_members": [
            "Crimea",
            "Eastern Donbas",
            "Northern Ireland",
        ]
    },
    "Oceania": {},
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("fh")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb_ratings = ds_meadow["fh_ratings"].reset_index()
    tb_scores = ds_meadow["fh_scores"].reset_index()

    #
    # Process data.
    #
    tb_ratings = geo.harmonize_countries(
        df=tb_ratings,
        countries_file=paths.country_mapping_path,
    )
    tb_scores = geo.harmonize_countries(
        df=tb_scores,
        countries_file=paths.country_mapping_path,
    )

    # Create indicator for electoral democracy
    tb_scores = add_electdem(tb_scores)

    # Merge
    tb = tb_ratings.merge(tb_scores, on=["country", "year"], how="outer")

    # Drop rows without values
    columns_excluded = ["country", "year", "country_fh"]
    tb = tb.dropna(subset=[col for col in tb.columns if col not in columns_excluded], how="all")
    tb = cast(Table, tb)

    # Impute values
    col_flag_imputed = "values_imputed"
    assert (
        tb["year"].min() == YEAR_MIN
    ), f"Minimum year is not as expected (should be {YEAR_MIN}! Imputing might behave unexpectedly."
    tb = add_imputes(tb=tb, path=PATH_IMPUTE, col_flag_imputed=col_flag_imputed)

    # Get region data
    tb_regions = tb.loc[~tb[col_flag_imputed]].drop(columns=[col_flag_imputed]).copy()
    tb_regions = get_region_aggregates(tb_regions, ds_regions, ds_population)

    # Clear variables before they are included
    tb_regions.loc[tb_regions["year"] < 2005, "num_regime_fh"] = pd.NA
    tb_regions.loc[tb_regions["year"] < 2005, "num_electdem_fh"] = pd.NA

    # Remove imputed flag
    tb = tb.drop(columns=[col_flag_imputed])

    # Table list
    tables = [
        tb.format(["country", "year"], short_name=paths.short_name),
        tb_regions.format(["country", "year", "category"], short_name=paths.short_name + "_regions"),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_electdem(tb: Table) -> Table:
    """Add electoral democracy indicator."""
    mask = (tb["electprocess_fh"] >= 7) & (tb["polrights_score_fh"] >= 20) & (tb["civlibs_score_fh"] >= 30)
    tb.loc[mask, "electdem_fh"] = 1
    tb.loc[
        ~mask & (tb[["electprocess_fh", "polrights_score_fh", "civlibs_score_fh"]].notna().all(axis=1)),
        "electdem_fh",
    ] = 0

    tb["electdem_fh"] = tb["electdem_fh"].copy_metadata(tb["electprocess_fh"])
    return tb


def get_region_aggregates(
    tb: Table,
    ds_regions: Dataset,
    ds_population: Dataset,
) -> Table:
    """Create table with region aggregates.

    Includes counts of countries and counts of people living in countries"""
    tb_ = tb.copy()

    # Set INTs
    tb_ = tb_.astype(
        {
            "regime_fh": "Int64",
            "electdem_fh": "Int64",
        }
    )
    tb_ = cast(Table, tb_)

    # Define columns on which we will estimate (i) "number of countries" and (ii) "number of people living in ..."
    indicators = [
        {
            "name": "regime_fh",
            "values_expected": {
                "0": "Not free",
                "1": "Partly free",
                "2": "Free",
            },
            "has_na": True,
        },
        {
            "name": "electdem_fh",
            "values_expected": {
                "0": "Non-electoral democracy",
                "1": "Electoral democracy",
            },
            "has_na": True,
        },
    ]
    indicator_names = [indicator["name"] for indicator in indicators]

    # 1) numbers
    ## Make dummies
    tb_num = make_table_with_dummies(tb_, indicators)

    ## Count
    tb_num = add_regions_and_global_aggregates(tb_num, ds_regions, regions=REGIONS)
    tb_num = from_wide_to_long(tb_num)
    tb_num = tb_num.rename(columns=dict(zip(indicator_names, [f"num_{i}" for i in indicator_names])))

    return tb_num
