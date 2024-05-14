"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from shared import (
    add_population_in_dummies,
    add_regions_and_global_aggregates,
    expand_observations,
    from_wide_to_long,
    make_table_with_dummies,
)

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


REGIONS = {
    "Africa": {
        "additional_members": [
            "Somaliland",
            "Zanzibar",
        ]
    },
    "Asia": {
        "additional_members": [
            "Palestine/Gaza",
            "Palestine/West Bank",
            "Republic of Vietnam",
        ]
    },
    "North America": {},
    "South America": {},
    "Europe": {},
    "Oceania": {},
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ert")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["ert"].reset_index()

    #
    # Process data.
    #
    # Rename columns
    tb = tb.rename(
        columns={
            "country_name": "country",
        }
    )

    # Add regime_ert
    tb = add_regime_indicators(tb)

    # Drop columns
    tb = tb.drop(
        columns=[
            "aut_ep_end_year",
            "dem_ep_end_year",
            "dem_ep_outcome",
            "aut_ep_outcome",
            "dem_ep",
            "aut_ep",
        ]
    )

    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    # Corrections: Germany -> West Germany, Yemen -> Yemen Arab Republic, Vietnam -> Democratic Republic of Vietnam
    tb = correct_country_names(tb)

    # Get regions table
    tb_regions = get_region_aggregates(tb, ds_regions, ds_population)

    # Index
    tb = tb.format(["country", "year"])
    tb_regions = tb_regions.format(["country", "year", "category"], short_name="region_aggregates")

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_regions,
    ]
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_regime_indicators(tb: Table) -> Table:
    """Estimate all regime-categorisation-related indicators.

    The source provides one indicator: `reg_type`. We derive four indicators in total:

    - `regime_ert`: a 6-category indicator of the regime type.
    - `regime_dich_ert`: a 2-category indicator of the regime type. Is equivalent to `reg_type`.
    - `regime_trich_ert`: a 3-category indicator of the regime type.
    - `regime_trep_outcome_ert`: a 12-category indicator of the regime type.
    """
    tb = tb.rename(
        columns={
            "reg_type": "regime_dich_ert",
        }
    )

    # Add regime_ert
    column = "regime_ert"
    assert set(tb["dem_ep"]) == {0, 1}, "`dem_ep` must only contain values {0,1}"
    assert set(tb["aut_ep"]) == {0, 1}, "`aut_ep` must only contain values {0,1}"
    tb.loc[(tb["regime_dich_ert"] == 0) & (tb["aut_ep"] == 1) & (tb["dem_ep"] == 0), column] = 0
    tb.loc[(tb["regime_dich_ert"] == 0) & (tb["aut_ep"] == 0) & (tb["dem_ep"] == 0), column] = 1
    tb.loc[(tb["regime_dich_ert"] == 0) & (tb["aut_ep"] == 0) & (tb["dem_ep"] == 1), column] = 2
    tb.loc[(tb["regime_dich_ert"] == 1) & (tb["aut_ep"] == 1) & (tb["dem_ep"] == 0), column] = 3
    tb.loc[(tb["regime_dich_ert"] == 1) & (tb["aut_ep"] == 0) & (tb["dem_ep"] == 0), column] = 4
    tb.loc[(tb["regime_dich_ert"] == 1) & (tb["aut_ep"] == 0) & (tb["dem_ep"] == 1), column] = 5
    tb.loc[(tb["regime_dich_ert"] == 0) & (tb["aut_ep"] == 1) & (tb["dem_ep"] == 1), column] = pd.NA
    tb.loc[(tb["regime_dich_ert"] == 1) & (tb["aut_ep"] == 1) & (tb["dem_ep"] == 1), column] = pd.NA

    # Add regime_trich_ert
    column = "regime_trich_ert"
    tb.loc[(tb["regime_ert"] == 0) | (tb["regime_ert"] == 3), column] = 0
    tb.loc[(tb["regime_ert"] == 1) | (tb["regime_ert"] == 4), column] = 1
    tb.loc[(tb["regime_ert"] == 2) | (tb["regime_ert"] == 5), column] = 2

    # Add regime_trep_outcome_ert
    # TODO: needed?
    conditions = [
        (tb["year"] == tb["aut_ep_end_year"]) & (tb["aut_ep_outcome"] == 5),
        (tb["year"] == tb["dem_ep_end_year"]) & (tb["dem_ep_outcome"] == 4),
        (tb["year"] == tb["dem_ep_end_year"]) & (tb["dem_ep_outcome"] == 3),
        (tb["year"] == tb["dem_ep_end_year"]) & (tb["dem_ep_outcome"] == 2),
        (tb["year"] == tb["dem_ep_end_year"]) & (tb["dem_ep_outcome"] == 1),
        (tb["year"] == tb["aut_ep_end_year"]) & (tb["aut_ep_outcome"] == 1),
        (tb["year"] == tb["aut_ep_end_year"]) & (tb["aut_ep_outcome"] == 2),
        (tb["year"] == tb["aut_ep_end_year"]) & (tb["aut_ep_outcome"] == 3),
        (tb["year"] == tb["aut_ep_end_year"]) & (tb["aut_ep_outcome"] == 4),
        (tb["year"] == tb["dem_ep_end_year"]) & (tb["dem_ep_outcome"] == 5),
        (tb["year"] == tb["aut_ep_end_year"]) & (tb["aut_ep_outcome"] == 6),
        (tb["year"] == tb["dem_ep_end_year"]) & (tb["dem_ep_outcome"] == 6),
    ]

    choices = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    tb["regime_trep_outcome_ert"] = np.select(conditions, choices, default=np.nan)

    # Copy metadata from original indicator `regime_dich_ert`
    for column in ["regime_ert", "regime_trich_ert", "regime_trep_outcome_ert"]:
        tb[column] = tb[column].copy_metadata(tb["regime_dich_ert"])
        tb[column] = tb[column].astype("Int64")
    return tb


def correct_country_names(tb: Table) -> Table:
    tb["country"] = tb["country"].astype("string")
    tb.loc[(tb["country"] == "Germany") & (tb["year"] <= 1990) & (tb["year"] >= 1949), "country"] = "West Germany"
    tb.loc[(tb["country"] == "Yemen") & (tb["year"] <= 1990) & (tb["year"] >= 1918), "country"] = "Yemen Arab Republic"
    tb.loc[
        (tb["country"] == "Vietnam") & (tb["year"] <= 1975) & (tb["year"] >= 1945), "country"
    ] = "Democratic Republic of Vietnam"
    tb.loc[(tb["country"] == "Republic of Vietnam") & (tb["year"] < 1945), "country"] = "Vietnam"
    return tb


def get_region_aggregates(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> Table:
    """Get region aggregate values.

    This includes:
        - Number of countries under certain regime
        - People living under certain regime
    """
    tb_ = tb.copy()

    # Expand observations
    tb_ = expand_observations_without_duplicates(tb_)

    # Get aggregates
    indicators = [
        {
            "name": "regime_ert",
            "values_expected": {
                "0": "hardening_autocracy",
                "1": "stable_autocracy",
                "2": "liberalizing_autocracy",
                "3": "eroding_democracy",
                "4": "stable_democracy",
                "5": "deepening_democracy",
            },
            "has_na": True,
        },
        {
            "name": "regime_trich_ert",
            "values_expected": {
                "0": "autocratizing_regime",
                "1": "stable_regime",
                "2": "democratizing_regime",
            },
            "has_na": True,
        },
    ]
    indicator_names = [indicator["name"] for indicator in indicators]
    # Make dummies
    tb_ = make_table_with_dummies(tb_, indicators)

    # 1) numbers
    tb_num = add_regions_and_global_aggregates(tb_, ds_regions, regions=REGIONS)
    tb_num = from_wide_to_long(tb_num)
    tb_num = tb_num.rename(columns=dict(zip(indicator_names, [f"num_{i}" for i in indicator_names])))

    # 2) Get people
    tb_pop = add_population_in_dummies(tb_, ds_population)
    tb_pop = add_regions_and_global_aggregates(tb_pop, ds_regions, regions=REGIONS)
    tb_pop = from_wide_to_long(tb_pop)
    tb_pop = tb_pop.rename(columns={"regime_ert": "pop_regime_ert"})
    tb_pop = tb_pop.rename(columns=dict(zip(indicator_names, [f"pop_{i}" for i in indicator_names])))

    # 3) Merge
    tb_regions = tb_num.merge(tb_pop, on=["country", "year", "category"], how="inner")
    assert (tb_num.shape == tb_pop.shape) and (len(tb_num) == len(tb_regions))
    tb_regions.loc[tb_regions["category"] == "-1", ["num_regime_ert", "num_regime_trich_ert"]] = float("nan")

    return tb_regions


def expand_observations_without_duplicates(tb: Table):
    """Remove duplicate country entries."""
    tb = expand_observations(tb)

    # There are some duplicates in the data. We remove them.
    YEARS_YEMEN = (1918, 1990)
    YEARS_GERMANY = (1945, 1990)
    YEARS_VIETNAM = (1945, 1975)
    tb = tb.loc[
        ~(
            # Yemen
            ((tb["country"] == "Yemen Arab Republic") & ((tb["year"] > YEARS_YEMEN[1]) | (tb["year"] < YEARS_YEMEN[0])))
            | (
                (tb["country"] == "Yemen People's Republic")
                & ((tb["year"] > YEARS_YEMEN[1]) | (tb["year"] < YEARS_YEMEN[0]))
            )
            | ((tb["country"] == "Yemen") & (tb["year"] >= YEARS_YEMEN[0]) & (tb["year"] <= YEARS_YEMEN[1]))
            # Germany
            | ((tb["country"] == "West Germany") & ((tb["year"] > YEARS_GERMANY[1]) | (tb["year"] < YEARS_GERMANY[0])))
            | ((tb["country"] == "East Germany") & ((tb["year"] > YEARS_GERMANY[1]) | (tb["year"] < YEARS_GERMANY[0])))
            | ((tb["country"] == "Germany") & (tb["year"] >= YEARS_GERMANY[0]) & (tb["year"] <= YEARS_GERMANY[1]))
            # Vietnam
            | (
                (tb["country"] == "Republic of Vietnam")
                & ((tb["year"] > YEARS_VIETNAM[1]) | (tb["year"] < YEARS_VIETNAM[0]))
            )
            | (
                (tb["country"] == "Democratic Republic of Vietnam")
                & ((tb["year"] > YEARS_VIETNAM[1]) | (tb["year"] < YEARS_VIETNAM[0]))
            )
            | ((tb["country"] == "Vietnam") & (tb["year"] >= YEARS_VIETNAM[0]) & (tb["year"] <= YEARS_VIETNAM[1]))
        )
    ]
    return tb
