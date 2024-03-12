"""Load a meadow dataset and create a garden dataset."""

from typing import Tuple, cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Variable
from owid.catalog.tables import Table, concat

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# IMPUTE: We will infer indicator values for some countries based on their historical equivalences.
COUNTRIES_IMPUTE = [
    {
        "country": "Colombia",
        "country_impute": "Great Colombia",
        "year_min": 1821,
        "year_max": 1830,
    },
    {
        "country": "Costa Rica",
        "country_impute": "Federal Republic of Central America",
        "year_min": 1824,
        "year_max": 1837,
    },
    {
        "country": "Czechia",
        "country_impute": "Czechoslovakia",
        "year_min": 1918,
        "year_max": 1992,
    },
    {
        "country": "Ecuador",
        "country_impute": "Great Colombia",
        "year_min": 1821,
        "year_max": 1829,
    },
    {
        "country": "El Salvador",
        "country_impute": "Federal Republic of Central America",
        "year_min": 1824,
        "year_max": 1838,
    },
    {
        "country": "Guatemala",
        "country_impute": "Federal Republic of Central America",
        "year_min": 1824,
        "year_max": 1838,
    },
    {
        "country": "Honduras",
        "country_impute": "Federal Republic of Central America",
        "year_min": 1824,
        "year_max": 1838,
    },
    {
        "country": "Nicaragua",
        "country_impute": "Federal Republic of Central America",
        "year_min": 1824,
        "year_max": 1837,
    },
    {
        "country": "North Korea",
        "country_impute": "Korea (former)",
        "year_min": 1800,
        "year_max": 1910,
    },
    {
        "country": "Panama",
        "country_impute": "Great Colombia",
        "year_min": 1821,
        "year_max": 1830,
    },
    {
        "country": "Slovakia",
        "country_impute": "Czechoslovakia",
        "year_min": 1918,
        "year_max": 1992,
    },
    {
        "country": "South Korea",
        "country_impute": "Korea (former)",
        "year_min": 1800,
        "year_max": 1910,
    },
    {
        "country": "Venezuela",
        "country_impute": "Great Colombia",
        "year_min": 1821,
        "year_max": 1829,
    },
    {
        "country": "Russia",
        "country_impute": "USSR",
        "year_min": 1922,
        "year_max": 1991,
    },
    {
        "country": "Ethiopia",
        "country_impute": "Ethiopia (former)",
        "year_min": 1952,
        "year_max": 1992,
    },
    {
        "country": "Eritrea",
        "country_impute": "Ethiopia (former)",
        "year_min": 1952,
        "year_max": 1992,
    },
    {
        "country": "Pakistan",
        "country_impute": "Pakistan (former)",
        "year_min": 1947,
        "year_max": 1971,
    },
    {
        "country": "Bangladesh",
        "country_impute": "Pakistan (former)",
        "year_min": 1947,
        "year_max": 1971,
    },
]

# REGION AGGREGATES
REGIONS = {
    "Africa": {},
    "Asia": {},
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
    ds_meadow = paths.load_dataset("bmr")
    ds_regions = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb = ds_meadow["bmr"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Identify duplicate observations
    tb = remove_country_overlaps(tb)

    # Keep relevant columns
    tb = tb[["country", "year", "democracy_omitteddata", "democracy_femalesuffrage"]]
    tb = tb.rename(
        columns={
            "democracy_omitteddata": "regime",
            "democracy_femalesuffrage": "regime_womsuffr",
        }
    )

    # Impute missing values
    tb = add_imputes(tb)

    # Refine
    ## Set NaNs to womsuff
    tb.loc[tb["regime"].isna(), "regime_womsuffr"] = pd.NA
    ## Add country codes
    # tb["ccode"] = tb["country"].astype("category").cat.codes

    ## Add democracy age / experience
    tb = tb.sort_values(["country", "year"])

    # Add years in democracy (consecutive and total)
    tb = add_years_in_democracy(tb)

    # Add age groups
    tb = add_year_counts_groups(tb)

    #### WIP ###############################
    # TODO
    # check coverage of regime and regime_ws
    # Get values for 'World' (until line 62)
    # -x Number of countries in democracy
    # -x Number of countries not in democracy
    # -x Number of countries in democracy with WS
    # -x Number of countries not in democracy with WS
    # -x Number of countries with X years in democracy
    # -x Number of countries with X years in democracy with WS
    #
    # Get equivalents with population counts (kinda ignore former country rows)
    # - Number of people in democracy
    # - Number of people not in democracy
    # - pop_missreg_bmr_owid: countries for which we don't have regime (comes from merging with population)
    # - Number of people in democracy with WS
    # - Number of people not in democracy with WS

    # Sanity checks
    assert tb.loc[tb["regime"].isna(), "regime_womsuffr"].isna().all()
    assert tb.loc[tb["regime_womsuffr"].isna(), "regime"].isna().all()

    # Get country counters
    ## Number of countries in X regime (with or withour WS), number of countries in democracy for Y years (with or without WS)
    tb_num_countries, tb_num_countries_years_consec = make_tables_country_counters(tb, ds_regions)

    ########################################
    # Set index.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    tables = [
        tb,
        tb_num_countries,
        tb_num_countries_years_consec,
    ]
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def remove_country_overlaps(tb: Table) -> Table:
    """Remove country overlaps.

    There are some years for which we have data for both the former and the current country. We should only have one, to avoid double counting.
    """
    ## "Germany" in 1945 and 1990 (those years we have West and East Germany)
    ## "Yugoslavia" in 1991 (the country was dissolved) and "Yugoslavia/Serbia" in 2006 (the country was dissolved)
    ## "Bangladesh" in 1971 (we have data for "Pakistan (former)")
    tb = tb.loc[~((tb["ccode"] == 255) & (tb["year"] == 1945))]
    tb = tb.loc[~((tb["ccode"] == 255) & (tb["year"] == 1990))]
    tb = tb.loc[~((tb["ccode"] == 345) & (tb["year"] == 1991))]
    tb = tb.loc[~((tb["ccode"] == 347) & (tb["year"] == 2006))]
    tb = tb.loc[~((tb["ccode"] == 771) & (tb["year"] == 1971))]

    return tb


def add_imputes(tb: Table) -> Table:
    """Add imputed values to the table."""
    tb_imputed = []
    for impute in COUNTRIES_IMPUTE:
        # Get relevant rows
        tb_ = tb.loc[
            (tb["country"] == impute["country_impute"])
            & (tb["year"] >= impute["year_min"])
            & (tb["year"] <= impute["year_max"])
        ].copy()
        # Sanity checks
        assert tb_.shape[0] > 0, f"No data found for {impute['country_impute']}"
        assert tb_["year"].max() == impute["year_max"], f"Missing years (max check) for {impute['country_impute']}"
        assert tb_["year"].min() == impute["year_min"], f"Missing years (min check) for {impute['country_impute']}"

        # Tweak them
        tb_ = tb_.rename(
            columns={
                "country": "regime_imputed_country",
            }
        ).assign(
            **{
                "country": impute["country"],
                "regime_imputed": True,
            }
        )
        tb_imputed.append(tb_)

    tb = concat(tb_imputed + [tb], ignore_index=True)

    # Set to False by default (for non-imputed countries)
    tb["regime_imputed"] = tb["regime_imputed"].fillna(False)

    # Re-order columns
    cols = [
        "country",
        "year",
        "regime",
        "regime_womsuffr",
        "regime_imputed_country",
        "regime_imputed",
    ]
    tb = cast(Table, tb[cols])

    return tb


def add_years_in_democracy(tb: Table) -> Table:
    """Add years in democracy.

    Two types of counters are generated:
        - Total number of years in democracy.
        - Number of consecutive years in democracy.

    This is applied to two indicators: democracy with and without women's suffrage.

    Consequently, four indicators are created:

        - num_years_in_democracy_consecutive: Number of consecutive years in democracy.
        - num_years_in_democracy: Total number of years in democracy.
        - num_years_in_democracy_ws_consecutive: Number of consecutive years in democracy with women's suffrage.
        - num_years_in_democracy_ws: Total number of years in democracy with women's suffrage.
    """
    ### Count the number of years since the country first became a democracy. Transition NaN -> 1 is considered as 0 -> 1.
    tb["num_years_in_democracy_consecutive"] = tb.groupby(["country", tb["regime"].fillna(0).eq(0).cumsum()])[
        "regime"
    ].cumsum()
    tb["num_years_in_democracy_consecutive"] = tb["num_years_in_democracy_consecutive"].astype(float)
    tb["num_years_in_democracy"] = tb.groupby("country")["regime"].cumsum()
    ## Add democracy age (including women's suffrage) / experience
    ### Count the number of years since the country first became a democracy. Transition NaN -> 1 is considered as 0 -> 1.
    tb["num_years_in_democracy_ws_consecutive"] = tb.groupby(
        ["country", tb["regime_womsuffr"].fillna(0).eq(0).cumsum()]
    )["regime_womsuffr"].cumsum()
    tb["num_years_in_democracy_ws_consecutive"] = tb["num_years_in_democracy_ws_consecutive"].astype(float)
    tb["num_years_in_democracy_ws"] = tb.groupby("country")["regime_womsuffr"].cumsum()

    return tb


def add_year_counts_groups(tb: Table) -> Table:
    """Add age groups to the table."""
    tb = _add_year_counts_groups(tb, "num_years_in_democracy_consecutive")
    tb = _add_year_counts_groups(tb, "num_years_in_democracy_ws_consecutive")

    return tb


def _add_year_counts_groups(tb: Table, column: str) -> Table:
    bins = [
        # -np.inf,
        0,
        18,
        30,
        60,
        90,
        np.inf,
    ]
    labels = [
        "1-18",
        "19-30",
        "31-60",
        "61-90",
        "91+",
    ]
    assert len(bins) == len(labels) + 1, "Length mismatch!"

    tb[f"{column}_group"] = pd.cut(tb[column], bins=bins, labels=labels)
    tb[f"{column}_group"] = Variable(tb[f"{column}_group"]).copy_metadata(tb[column])
    return tb


def from_wide_to_long(tb: Table) -> Table:
    """Format a particular shape of table from wide to long format.

    The expected input table format is:

    | year | country | indicator_a_1 | indicator_a_2 | indicator_b_1 | indicator_b_2 |
    |------|---------|---------------|---------------|---------------|---------------|
    | 2000 |   USA   |       1       |       2       |       3       |       4       |
    | 2000 |   CAN   |       5       |       6       |       7       |       8       |

    The generated output is:

    | year | country |  category  | indicator_a | indicator_b |
    |------|---------|------------|-------------|-------------|
    | 2000 | USA     | category_1 |      1      |       3     |
    | 2000 | USA     | category_2 |      2      |       4     |
    """
    # Melt the DataFrame to long format
    tb = tb.melt(id_vars=["year", "country"], var_name="indicator_type", value_name="value")

    # Extract indicator names and types
    tb["indicator"] = tb["indicator_type"].apply(lambda x: "_".join(x.split("_")[:-1]))
    tb["category"] = tb["indicator_type"].apply(lambda x: x.split("_")[-1])

    # Drop the original 'indicator_type' column as it's no longer needed
    tb.drop("indicator_type", axis=1, inplace=True)

    # Pivot the table to get 'indicator_a' and 'indicator_b' as separate columns
    tb = tb.pivot(index=["year", "country", "category"], columns="indicator", values="value").reset_index()

    # Rename the columns to match your requirements
    tb.columns.name = None  # Remove the hierarchy

    return tb


def make_tables_country_counters(tb: Table, ds_regions: Dataset) -> Tuple[Table, Table]:
    tb_ = tb[~tb["regime_imputed"]].copy()

    ## Sanity check: all countries are in the regions
    members_tracked = set()
    for region, _ in REGIONS.items():
        members_tracked |= set(geo.list_members_of_region(region, ds_regions))
    assert tb_["country"].isin(members_tracked).all(), "Some countries are not in the regions!"

    ## Indicators to create counters for
    indicators = [
        {
            "name": "regime",
            "values_expected": {"0", "1", "-1"},
        },
        {
            "name": "regime_womsuffr",
            "values_expected": {"0", "1", "-1"},
        },
        {
            "name": "num_years_in_democracy_consecutive_group",
            "values_expected": {
                "1-18",
                "19-30",
                "31-60",
                "61-90",
                "91+",
                "-1",
            },
        },
        {
            "name": "num_years_in_democracy_ws_consecutive_group",
            "values_expected": {
                "1-18",
                "19-30",
                "31-60",
                "61-90",
                "91+",
                "-1",
            },
        },
    ]
    indicator_names = [indicator["name"] for indicator in indicators]

    ## Replace NaNs with -1 (easier to process)
    tb_[indicator_names] = tb_[indicator_names].astype("string").fillna("-1")

    ## Sanity checks
    for indicator in indicators:
        assert set(tb_[indicator["name"]]) == indicator["values_expected"]

    ## Get dummy indicator table
    tb_ = pd.get_dummies(tb_, dummy_na=True, columns=indicator_names)

    ## Add missing metadata to dummy indicators
    dummy_cols = []
    for indicator in indicators:
        for col in (_dummy_cols := [f"{indicator['name']}_{v}" for v in indicator["values_expected"]]):
            tb_[col].metadata = tb[indicator["name"]].metadata
        dummy_cols.extend(_dummy_cols)

    ### Select subset of columns
    tb_ = tb_[["year", "country"] + dummy_cols]

    ### Get aggregates
    tb_ = geo.add_regions_to_table(
        tb_,
        ds_regions,
        regions=REGIONS,
    )
    tb_ = tb_.loc[tb_["country"].isin(REGIONS.keys())]

    # Add world
    tb_w = tb_.groupby("year", as_index=False).sum().assign(country="World")
    tb_ = concat([tb_, tb_w], ignore_index=True, short_name="region_counts")

    ## Long format
    tb_ = from_wide_to_long(tb_)

    # Category as INT
    # tb_["category"] = tb_["category"].astype(int)

    # Rename indicators
    tb_ = tb_.rename(
        columns={
            "regime": "num_countries_regime",
            "regime_womsuffr": "num_countries_regime_ws",
            "num_years_in_democracy_consecutive_group": "num_countries_years_in_democracy_consec",
            "num_years_in_democracy_ws_consecutive_group": "num_countries_years_in_democracy_ws_consec",
        }
    )
    ## Get two tables
    tb_num_countries = (
        tb_[["year", "country", "category", "num_countries_regime", "num_countries_regime_ws"]]
        .copy()
        .set_index(["country", "year", "category"], verify_integrity=True)
        .dropna(how="all")
        .sort_index()
    )
    tb_num_countries.metadata.short_name = "num_countries_regime"
    tb_num_countries_years_consec = (
        tb_[
            [
                "year",
                "country",
                "category",
                "num_countries_years_in_democracy_consec",
                "num_countries_years_in_democracy_ws_consec",
            ]
        ]
        .copy()
        .set_index(["country", "year", "category"], verify_integrity=True)
        .dropna(how="all")
        .sort_index()
    )
    tb_num_countries_years_consec.metadata.short_name = "num_countries_regime_years"

    # Remove "unknown regime" for democracy with WS (should be equivalent to without WS, hence the check)
    mask = (slice(None), slice(None), "-1")
    diff = tb_num_countries.loc[mask, "num_countries_regime"] - tb_num_countries.loc[mask, "num_countries_regime_ws"]
    assert (
        diff == 0
    ).all(), "The number of countries with unknown regimes should be the same according to indicators `num_countries_regime` and `num_countries_regime_ws`. Please check!"
    tb_num_countries.loc[mask, "num_countries_regime_ws"] = np.nan

    # Remove the "unknowns" from consecutive year counts
    tb_num_countries_years_consec = tb_num_countries_years_consec.drop(index="-1", level="category")
    return tb_num_countries, tb_num_countries_years_consec
