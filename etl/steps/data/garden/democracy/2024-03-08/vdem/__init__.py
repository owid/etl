"""Load a meadow dataset and create a garden dataset."""
from typing import cast

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.processing import concat

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# REGION AGGREGATES
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
        ]
    },
    "North America": {},
    "South America": {},
    "Europe": {
        "additional_members": [
            "Baden",
            "Bavaria",
            "Brunswick",
            "Duchy of Nassau",
            "Hamburg",
            "Hanover",
            "Hesse Electoral",
            "Hesse Grand Ducal",
            "Mecklenburg Schwerin",
            "Modena",
            "Oldenburg",
            "Parma",
            "Piedmont-Sardinia",
            "Saxe-Weimar-Eisenach",
            "Saxony",
            "Tuscany",
            "Two Sicilies",
            "Wurttemberg",
        ]
    },
    "Oceania": {},
}


def run(dest_dir: str) -> None:
    # %% Load data
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vdem")
    ds_regions = paths.load_dataset("regions")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["vdem"].reset_index()

    #
    # Process data.
    #
    # %% Remove imputed countries
    tb_ = tb.loc[~tb["regime_imputed"]].copy()

    tb_["country"] = tb_["country"].astype("string")

    # %% Sanity check: all countries are in the regions
    members_tracked = set()
    for region, region_props in REGIONS.items():
        members_tracked |= set(
            geo.list_members_of_region(region, ds_regions, additional_members=region_props.get("additional_members"))
        )
    assert tb_["country"].isin(members_tracked).all(), "Some countries are not in the regions!"

    # %% Get counts
    # Generate dummy indicators
    tb_sum = make_table_countries_counts(tb_, ds_regions)
    tb_avg = make_table_countries_avg(tb_, ds_regions)

    # %% Set index
    tb = tb.format()

    # %% Save
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_table_countries_counts(tb: Table, ds_regions: Dataset) -> Table:
    """Get region indicators of type "Number of countries"."""
    tb_ = tb.copy()
    # Generate dummy indicators
    tb_ = make_table_with_dummies(tb_)

    tb_ = geo.add_regions_to_table(
        tb_,
        ds_regions,
        regions=REGIONS,
    )
    # Keep only regions
    tb_ = tb_.loc[tb_["country"].isin(REGIONS.keys())]

    # Get value for the World and add to table
    tb_w = tb_.groupby("year", as_index=False).sum().assign(country="World")
    tb_ = concat([tb_, tb_w], ignore_index=True, short_name="region_counts")

    return tb_


def make_table_countries_avg(tb: Table, ds_regions: Dataset) -> Table:
    tb_ = tb.copy()
    return tb_


def make_table_with_dummies(tb: Table) -> Table:
    """Format table to have dummy indicators.

    From a table with categorical indicators, create a new table with dummy indicator for each indicator-category pair.

    Example input:

    | year | country |  regime   | regime_amb |
    |------|---------|-----------|------------|
    | 2000 |   USA   |     1     |      0     |
    | 2000 |   CAN   |     0     |      1     |
    | 2000 |   DEU   |    NaN    |      NaN   |


    Example output:

    | year | country | regime_0 | regime_1 | regime_-1 | regime_amb_0 | regime_amb_0 | regime_amb_-1 |
    |------|---------|----------|----------|-----------|--------------|--------------|---------------|
    | 2000 |   USA   |    0     |    1     |     0     |      1       |      0       |       0       |
    | 2000 |   CAN   |    1     |    0     |     0     |      0       |      1       |       0       |
    | 2000 |   DEU   |    0     |    0     |     1     |      0       |      0       |       1       |

    Note that '-1' denotes NA (missing value) category.

    """
    tb_ = tb.copy()

    # Define indicators for which we will create dummies
    indicators = [
        {
            "name": "regime_row_owid",
            "values_expected": set(map(str, range(4))),
            "has_na": True,
        },
        {
            "name": "regime_amb_row_owid",
            "values_expected": set(map(str, range(10))),
            "has_na": True,
        },
        {
            "name": "num_years_in_electdem_cat",
            "values_expected": {
                "closed autocracy",
                "electoral autocracy",
                "1-18",
                "19-30",
                "31-60",
                "61-90",
                "91+",
            },
            "has_na": True,
        },
        {
            "name": "num_years_in_libdem_cat",
            "values_expected": {
                "closed autocracy",
                "electoral autocracy",
                "electoral democracy",
                "1-18",
                "19-30",
                "31-60",
                "61-90",
                "91+",
            },
            "has_na": True,
        },
        {
            "name": "wom_parl_vdem_cat",
            "values_expected": {
                "0-10% women",
                "10-20% women",
                "20-30% women",
                "30-40% women",
                "40-50% women",
                "50%+ women",
            },
            "has_na": True,
        },
        {
            "name": "wom_hog_vdem",
            "values_expected": {"0", "1"},
            "has_na": True,
        },
        {
            "name": "wom_hos_vdem",
            "values_expected": {"0", "1"},
            "has_na": True,
        },
        {
            "name": "wom_hoe_vdem",
            "values_expected": {"0", "1"},
            "has_na": True,
        },
    ]

    # Convert to string
    indicator_names = [indicator["name"] for indicator in indicators]
    tb_[indicator_names] = tb_[indicator_names].astype("string")

    # Sanity check that the categories for each indicator are as expected
    for indicator in indicators:
        values_expected = indicator["values_expected"]
        # Check and fix NA (convert NAs to -1 category)
        if indicator["has_na"]:
            # Assert that there are actually NaNs
            assert tb_[indicator["name"]].isna().any(), "No NA found!"
            # If NA, we should not have category '-1', otherwise these would get merged!
            assert "-1" not in set(
                tb_[indicator["name"]].unique()
            ), f"Error for indicator `{indicator['name']}`. Found -1, which is not allowed when `has_na=True`!"
            tb_[indicator["name"]] = tb_[indicator["name"]].fillna("-1")
            # Add '-1' as a possible category
            values_expected |= {"-1"}
        else:
            assert not tb_[indicator["name"]].isna().any(), "NA found!"

        values_found = set(tb_[indicator["name"]].unique())
        assert (
            values_found == values_expected
        ), f"Error for indicator `{indicator['name']}`. Expected {indicator['values_expected']} but found {values_found}"

    ## Get dummy indicator table
    tb_ = cast(Table, pd.get_dummies(tb_, dummy_na=True, columns=indicator_names))

    ## Add missing metadata to dummy indicators
    dummy_cols = []
    for indicator in indicators:
        for col in (_dummy_cols := [f"{indicator['name']}_{v}" for v in indicator["values_expected"]]):
            tb_[col].metadata = tb[indicator["name"]].metadata
        dummy_cols.extend(_dummy_cols)

    ### Select subset of columns
    tb_ = tb_.loc[:, ["year", "country"] + dummy_cols]

    return tb_
