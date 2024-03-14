"""Load a meadow dataset and create a garden dataset.

This dataset contains X tables. One ('bmr') focusses on country-level data, and the rest on regional aggregates.

Tables:
- 'bmr': Country-level data on democracy. Reports data for countries even if they did not exist. This is basically so that these countries show values on maps.
- 'num_countries_regime': Number of countries in democracy, by region and World.
- 'num_countries_regime_years': Number of countries in democracies aged X years old, by region and World.
- 'population_regime': Number of people in democracy, by region and World.
- 'population_regime_years': Number of people in democracies aged X years old, by region and World.
"""

import json
from typing import Any, Dict, List, Set, Tuple, Union, cast

import numpy as np
import pandas as pd
import yaml
from owid.catalog import Dataset, Variable
from owid.catalog.tables import Table, concat

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# IMPUTE: We will infer indicator values for some countries based on their historical equivalences.
path = paths.directory / "bmr.countries_impute.yml"
COUNTRIES_IMPUTE = yaml.safe_load(path.read_text())

# Overlapping countries as expected when counting countries
COUNTRIES_OVERLAP = json.loads((paths.directory / "bmr.countries_overlap.json").read_text())
COUNTRIES_OVERLAP = [{int(k): set(v) for k, v in overlaps.items()} for overlaps in COUNTRIES_OVERLAP]

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
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["bmr"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

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

    # Drop all-NaN rows
    cols_subset = [col for col in tb.columns if col not in ["country", "year"]]
    tb = tb.dropna(subset=cols_subset, how="all")

    # Impute missing values
    tb = add_imputes(tb)

    # Refine
    ## Set NaNs to womsuff
    tb.loc[tb["regime"].isna(), "regime_womsuffr"] = pd.NA
    # assert tb.loc[tb["regime"].isna(), "regime_womsuffr"].isna().all()
    # assert tb.loc[tb["regime_womsuffr"].isna(), "regime"].isna().all()

    ## Add democracy age / experience
    tb = tb.sort_values(["country", "year"])

    # Add years in democracy (consecutive and total)
    tb = add_years_in_democracy(tb)

    # Add age groups
    tb = add_year_counts_groups(tb)

    # Get number of countries in democracy, by region and World
    tb_num_countries, tb_num_countries_years_consec = make_tables_country_counters(tb, ds_regions)

    # Get number of people in democracy, by region and World
    tb_pop, tb_pop_years_consec = make_tables_population_counters(tb, ds_regions, ds_population)

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
        tb_pop,
        tb_pop_years_consec,
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
    tb = tb.loc[~((tb["ccode"] == 255) & (tb["year"] == 1945))]
    tb = tb.loc[~((tb["ccode"] == 255) & (tb["year"] == 1990))]
    ## "Yugoslavia" in 1991 (the country was dissolved) and "Yugoslavia/Serbia" in 2006 (the country was dissolved)
    tb = tb.loc[~((tb["ccode"] == 345) & (tb["year"] == 1991))]
    tb = tb.loc[~((tb["ccode"] == 347) & (tb["year"] == 2006))]
    ## "Bangladesh" in 1971 (we have data for "Pakistan (former)")
    tb = tb.loc[~((tb["ccode"] == 771) & (tb["year"] == 1971))]

    # Given (a, b, c, d), remove country with ccode `a` in year `c`
    # If `d` is True, only remove if indicators for `a` and `b` are the same for year `c
    drops = [
        # Great Colombia
        ## "Ecuador" in 1830
        (130, 99, 1830, True),
        ## "Venezuela" in 1830
        (101, 99, 1830, True),
        # Central American Union
        ## "Nicaragua" in 1838
        (93, 89, 1838, True),
        ## "Costa Rica" in 1838
        (94, 89, 1838, True),
        # USSR
        ## "Armenia" in 1991
        (371, 364, 1991, True),
        ## "Georgia" in 1991
        (372, 364, 1991, True),
        ## "Azerbaijan" in 1991
        (373, 364, 1991, True),
        ## "Moldova" in 1991
        (359, 364, 1991, False),
        ## "Ukraine" in 1991
        (369, 364, 1991, False),
        ## "Belarus" in 1991
        (370, 364, 1991, False),
        ## "Kazakhstan" in 1991
        (705, 364, 1991, True),
        ## "Uzbekistan" in 1991
        (704, 364, 1991, True),
        ## "Kyrgyzstan" in 1991
        (703, 364, 1991, True),
        ## "Tajikistan" in 1991
        (702, 364, 1991, True),
        ## "Turkmenistan" in 1991
        (701, 364, 1991, True),
        ## Lithuania in 1991
        (368, 364, 1991, True),
        ## Latvia in 1991
        (367, 364, 1991, True),
        ## Estonia in 1991
        (366, 364, 1991, False),
    ]
    cols_indicators = ["democracy_omitteddata", "democracy_femalesuffrage"]
    for drop in drops:
        if drop[3]:
            assert (
                tb.loc[(tb["ccode"] == drop[0]) & (tb["year"] == drop[2]), cols_indicators]
                .reset_index(drop=True)
                .equals(
                    tb.loc[(tb["ccode"] == drop[1]) & (tb["year"] == drop[2]), cols_indicators].reset_index(drop=True)
                )
            ), f"Something off with {drop[0]} and {drop[1]} in {drop[2]}"
        # Remove country
        tb = tb.loc[~((tb["ccode"] == drop[0]) & (tb["year"] == drop[2]))]
    return tb


def add_imputes(tb: Table) -> Table:
    """Add imputed values to the table."""
    # Drop known values that are not correct

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
        )
        tb_["regime_imputed"] = True

        # Different behaviour depending whether we have a list of countries or a single country to impute
        if isinstance(impute["country"], list):
            for country in impute["country"]:
                tb_["country"] = country
                tb_imputed.append(tb_.copy())
        else:
            tb_["country"] = impute["country"]
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

    # Verify that there are no duplicates
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().reset_index()
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


def make_table_with_dummies(
    tb: Table, ds_regions: Dataset, indicators: Union[List[Dict[str, Any]], None] = None
) -> Table:
    """Format table to have dummy indicators.

    From a table with categorical indicators, create a new table with dummy indicator for each indicator-category pair.
    """
    tb_ = tb.copy()

    ## Sanity check: all countries are in the regions
    members_tracked = set()
    for region, _ in REGIONS.items():
        members_tracked |= set(geo.list_members_of_region(region, ds_regions))
    assert tb_["country"].isin(members_tracked).all(), "Some countries are not in the regions!"

    if indicators is None:
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
    """Get tables with number of countries in democracy."""
    tb_ = tb.loc[~tb["regime_imputed"]].copy()

    tb_ = make_table_with_dummies(tb_, ds_regions)

    ### Get aggregates
    tb_ = geo.add_regions_to_table(
        tb_,
        ds_regions,
        regions=REGIONS,
        accepted_overlaps=COUNTRIES_OVERLAP,
    )
    tb_ = tb_.loc[tb_["country"].isin(REGIONS.keys())]

    # Add world
    tb_w = tb_.groupby("year", as_index=False).sum().assign(country="World")
    tb_ = concat([tb_, tb_w], ignore_index=True, short_name="region_counts")

    ## Long format
    tb_ = from_wide_to_long(tb_)

    # Generate two columns (1: in democracy, 2: age of democracy)
    tb_num_countries, tb_num_countries_years_consec = split_into_two_tables(
        tb=tb_,
        column_renames={
            "regime": "num_countries_regime",
            "regime_womsuffr": "num_countries_regime_ws",
            "num_years_in_democracy_consecutive_group": "num_countries_years_in_democracy_consec",
            "num_years_in_democracy_ws_consecutive_group": "num_countries_years_in_democracy_ws_consec",
        },
        table_1_name="num_countries_regime",
        table_2_name="num_countries_regime_years",
    )
    return tb_num_countries, tb_num_countries_years_consec


def make_tables_population_counters(tb: Table, ds_regions: Dataset, ds_population: Dataset) -> Tuple[Table, Table]:
    """Get tables with number of people in democracy."""
    tb_ = tb.copy()

    # Drop historical countries (don't want to double-count population)
    tb_ = expand_observations_without_leading_to_duplicates(tb_, ds_regions)

    # DEBUG
    # tb_.to_csv("temp-working.csv")

    # Get dummy indicators
    tb_ = make_table_with_dummies(tb_, ds_regions)

    # Add population column
    tb_ = geo.add_population_to_table(
        tb_,
        ds_population,
        interpolate_missing_population=True,
        expected_countries_without_population=[
            "Pakistan (former)",
            "Korea (former)",
            "Duchy of Parma and Piacenza",
            "Orange Free State",
            "Federal Republic of Central America",
            "Grand Duchy of Tuscany",
            "Democratic Republic of Vietnam",
            "Kingdom of Saxony",
            "Duchy of Modena and Reggio",
            "Kingdom of the Two Sicilies",
            "Kingdom of Sardinia",
            "Great Colombia",
            "Grand Duchy of Baden",
            "Kingdom of Wurttemberg",
            "Republic of Vietnam",
            "Kingdom of Bavaria",
        ],
    )
    tb_ = cast(Table, tb_.dropna(subset="population"))
    ## Save metadata
    cols = [col for col in tb_.columns if col not in ["year", "country", "population"]]
    meta = {col: tb_[col].metadata for col in cols} | {"population": tb_["population"].metadata}
    ## Encode population in indicators: Population if 1, 0 otherwise
    tb_[cols] = tb_[cols].multiply(tb_["population"], axis=0)
    tb_ = tb_.drop(columns="population")
    ## Add metadata back (combine origins from population)
    for col in cols:
        metadata = meta[col]
        metadata.origins += meta["population"].origins
        tb_[col].metadata = meta[col]

    # Get region aggregates
    tb_ = geo.add_regions_to_table(
        tb_,
        ds_regions,
        regions=REGIONS,
    )
    tb_ = tb_.loc[tb_["country"].isin(REGIONS.keys())]

    # Add world
    tb_w = tb_.groupby("year", as_index=False).sum().assign(country="World")
    tb_ = concat([tb_, tb_w], ignore_index=True, short_name="region_counts")

    # Long format
    tb_ = from_wide_to_long(tb_)

    # Generate two columns (1: in democracy, 2: age of democracy)
    tb_population, tb_population_years_consec = split_into_two_tables(
        tb=tb_,
        column_renames={
            "regime": "population_regime",
            "regime_womsuffr": "population_regime_ws",
            "num_years_in_democracy_consecutive_group": "population_years_in_democracy_consec",
            "num_years_in_democracy_ws_consecutive_group": "population_years_in_democracy_ws_consec",
        },
        table_1_name="population_regime",
        table_2_name="population_regime_years",
    )
    return tb_population, tb_population_years_consec


def expand_observations_without_leading_to_duplicates(tb: Table, ds_regions: Dataset) -> Table:
    """Expand observations (accounting for overlaps between former and current countries).

    If the data has data for "USSR" and "Russia" for the same year, we should drop the "USSR" row.
    """
    # Extend observations to have all country-years
    tb = expand_observations(tb)

    # Drop former and current countries for some periods of years
    ## We've kept countries that were two sides of a current country (need to keep them since each side could have different regime)
    ## West and East Germany, North and South Yemen
    tb = tb.loc[
        ~(
            ((tb["country"] == "Yemen Arab Republic") & ((tb["year"] > 1989) | (tb["year"] < 1918)))
            | ((tb["country"] == "Yemen People's Republic") & ((tb["year"] > 1989) | (tb["year"] < 1967)))
            | ((tb["country"] == "Yemen") & (tb["year"] >= 1918) & (tb["year"] <= 1989))
            | ((tb["country"] == "West Germany") & ((tb["year"] > 1990) | (tb["year"] < 1945)))
            | ((tb["country"] == "East Germany") & ((tb["year"] > 1990) | (tb["year"] < 1945)))
            | ((tb["country"] == "Germany") & (tb["year"] >= 1945) & (tb["year"] <= 1990))
        )
    ]

    # Get list of country names to ignore (always)
    countries_ignore = _get_countries_to_ignore_population(ds_regions)

    # Drop historical countries (don't want to double-count population)
    tb = tb.loc[~tb["country"].isin(countries_ignore)]

    return tb


def expand_observations(tb: Table) -> Table:
    """Expand to have a row per (year, country)."""
    # Add missing years for each triplet ("warcode", "campcode", "ccode")

    # List of countries
    regions = set(tb["country"])

    # List of possible years
    years = np.arange(tb["year"].min(), tb["year"].max() + 1)

    # New index
    new_idx = pd.MultiIndex.from_product([years, regions], names=["year", "country"])

    # Reset index
    tb = tb.set_index(["year", "country"]).reindex(new_idx).reset_index()

    # Type of `year`
    tb["year"] = tb["year"].astype("int")
    return tb


def _get_countries_to_ignore_population(ds_regions: Dataset) -> Set[str]:
    """List of countries to ignore when working with population.

    To avoid double-counting population, former countries are ignored.
    """
    # Get table with regions
    tb_regions = ds_regions["regions"].set_index("name", verify_integrity=True)

    # Get country names from which we are imputing values from (most of them are historical countries)
    countries_ignore = {c["country_impute"] for c in COUNTRIES_IMPUTE}

    # Sanity check (all country names are valid as per tb_regions)
    assert not (countries_ignore - set(tb_regions.index)), "Some countries are not in the regions!"

    # Get only historical countries (we impute from currently existing countries, too!)
    countries_ignore = {c for c in countries_ignore if tb_regions.loc[c, "is_historical"]}
    return countries_ignore


def split_into_two_tables(
    tb: Table, column_renames: Dict[str, str], table_1_name: str, table_2_name: str
) -> tuple[Table, Table]:
    """Make two tables from a single table.

    The original table contains two groups of indicators:

    Group 1: Reports if a country is a democracy or not.
    Group 2: Reports if a country is a democracy with X years of age (indicator for each X years of age group).

    Note that in each group of indicators we have two flavours of democracy: accounting with and without Women's suffrage.
    """
    # Sanity checks
    assert set(column_renames.keys()) == {
        "regime",
        "regime_womsuffr",
        "num_years_in_democracy_consecutive_group",
        "num_years_in_democracy_ws_consecutive_group",
    }, "Missing columns in `column_renames`!"

    # Standardise names
    tb_ = tb.rename(columns=column_renames).copy()

    # Get column names
    col_regime = column_renames["regime"]
    col_regime_ws = column_renames["regime_womsuffr"]
    col_years_consec = column_renames["num_years_in_democracy_consecutive_group"]
    col_years_ws_consec = column_renames["num_years_in_democracy_ws_consecutive_group"]

    def _get_table_subset(table: Table, col: str, col_ws: str, short_name: str) -> Table:
        """Get subset of the table."""
        tb_subset = (
            table.loc[:, ["year", "country", "category", col, col_ws]]
            .copy()
            .set_index(["country", "year", "category"], verify_integrity=True)
            .dropna(how="all")
            .sort_index()
        )
        tb_subset.metadata.short_name = short_name
        return tb_subset

    # TABLE 1: Aggregate regime / regime_ws indicators
    tb_1 = _get_table_subset(tb_, col_regime, col_regime_ws, table_1_name)
    ## Remove "unknown regime" for democracy with WS (should be equivalent to without WS, hence the check)
    mask = (slice(None), slice(None), "-1")
    diff = tb_1.loc[mask, col_regime] - tb_1.loc[mask, col_regime_ws]
    assert (
        diff == 0
    ).all(), f"The number of countries with unknown regimes should be the same according to indicators `{col_regime}` and `{col_regime_ws}`. Please check!"
    tb_1.loc[mask, col_regime_ws] = np.nan

    # TABLE 2: Aggregate years in democracy (with or without WS)
    tb_2 = _get_table_subset(tb_, col_years_consec, col_years_ws_consec, table_2_name)

    ## Remove the "unknowns" from consecutive year counts
    tb_2 = tb_2.drop(index="-1", level="category")

    return tb_1, tb_2
