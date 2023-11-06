from typing import List, Literal, Optional, Type

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from typing_extensions import Self


def expand_observations(
    tb: Table, col_year_start: str, col_year_end: str, cols_scale: Optional[List[str]] = None, rounding: bool = True
) -> Table:
    """Expand to have a row per (year, conflict).

    Example
    -------

        Input:

        | dispnum | year_start | year_end |
        |---------|------------|----------|
        | 1       | 1990       | 1993     |

        Output:

        |  year | warcode |
        |-------|---------|
        |  1990 |    1    |
        |  1991 |    1    |
        |  1992 |    1    |
        |  1993 |    1    |

    Parameters
    ----------
    tb : Table
        Original table, where each row is a conflict with its start and end year.

    Returns
    -------
    Table
        Here, each conflict has as many rows as years of activity. Its deaths have been uniformly distributed among the years of activity.
    """
    # For that we scale the number of deaths proportional to the duration of the conflict.
    if cols_scale:
        for col in cols_scale:
            tb[col] = (tb[col] / (tb[col_year_end] - tb[col_year_start] + 1)).copy_metadata(tb[col])
            if rounding:
                tb[col] = tb[col].round()

    # Add missing years for each triplet ("warcode", "campcode", "ccode")
    YEAR_MIN = tb[col_year_start].min()
    YEAR_MAX = tb[col_year_end].max()
    tb_all_years = Table(pd.RangeIndex(YEAR_MIN, YEAR_MAX + 1), columns=["year"])
    tb = tb.merge(tb_all_years, how="cross")
    # Filter only entries that actually existed
    tb = tb[(tb["year"] >= tb[col_year_start]) & (tb["year"] <= tb[col_year_end])]

    return tb


def add_indicators_extra(
    tb: Table,
    tb_regions: Table,
    columns_conflict_rate: Optional[List[str]] = None,
    columns_conflict_mortality: Optional[List[str]] = None,
) -> Table:
    """Scale original columns to obtain new indicators (conflict rate and conflict mortality indicators).

    CONFLICT RATE:
        Scale columns `columns_conflict_rate` based on the number of countries (and country-pairs) in each region and year.

        For each indicator listed in `columns_to_scale`, two new columns are added to the table:
        - `{indicator}_per_country`: the indicator value divided by the number of countries in the region and year.
        - `{indicator}_per_country_pair`: the indicator value divided by the number of country-pairs in the region and year.

    CONFLICT MORTALITY:
        Scale columns `columns_conflict_mortality` based on the population in each region.

        For each indicator listed in `columns_to_scale`, a new column is added to the table:
        - `{indicator}_per_capita`: the indicator value divided by the number of countries in the region and year.


    tb: Main table
    tb_regions: Table with three columns: "year", "region", "num_countries". Gives the number of countries per region per year.
    columns_to_scale: List with the names of the columns that need scaling. E.g. number_ongiong_conflicts -> number_ongiong_conflicts_per_country
    """
    tb_regions_ = tb_regions.copy()

    # Sanity check 1: columns as expected in tb_regions
    assert set(tb_regions_.columns) == {
        "year",
        "region",
        "number_countries",
        "population",
    }, f"Invalid columns in tb_regions {tb_regions_.columns}"
    # Sanity check 2: regions equivalent in both tables
    regions_main = set(tb["region"])
    regions_aux = set(tb_regions_["region"])
    assert regions_main == regions_aux, f"Regions in main table and tb_regions differ: {regions_main} vs {regions_aux}"

    # Ensure full precision
    tb_regions_["number_countries"] = tb_regions_["number_countries"].astype(float)
    tb_regions_["population"] = tb_regions_["population"]  # .astype(float)
    # Get number of country-pairs
    tb_regions_["number_country_pairs"] = (
        tb_regions_["number_countries"] * (tb_regions_["number_countries"] - 1) / 2
    ).astype(int)

    # Add number of countries and number of country pairs to main table
    tb = tb.merge(tb_regions_, on=["year", "region"], how="left")

    if not columns_conflict_rate and not columns_conflict_mortality:
        raise ValueError(
            "Call to function is useless. Either provide `columns_conflict_rate` or `columns_conflict_mortality`."
        )

    # CONFLICT RATES ###########
    if columns_conflict_rate:
        # Add normalised indicators
        for column_name in columns_conflict_rate:
            # Add per country indicator
            column_name_new = f"{column_name}_per_country"
            tb[column_name_new] = (tb[column_name].astype(float) / tb["number_countries"].astype(float)).replace(
                [np.inf, -np.inf], np.nan
            )
            # Add per country-pair indicator
            column_name_new = f"{column_name}_per_country_pair"
            tb[column_name_new] = (tb[column_name].astype(float) / tb["number_country_pairs"].astype(float)).replace(
                [np.inf, -np.inf], np.nan
            )

    # CONFLICT MORTALITY ###########
    if columns_conflict_mortality:
        # Add normalised indicators
        for column_name in columns_conflict_mortality:
            # Add per country indicator
            column_name_new = f"{column_name}_per_capita"
            tb[column_name_new] = (
                (100000 * tb[column_name].astype(float) / tb["population"])
                .replace([np.inf, -np.inf], np.nan)
                .astype(float)
            )

    # Drop intermediate columns
    tb = tb.drop(columns=["number_countries", "number_country_pairs", "population"])

    return tb


class Normaliser:
    """Normalise indicators."""

    country_column: str

    def code_to_region(self: Self) -> None:
        """Convert code to region name."""
        raise NotImplementedError("Subclasses must implement this method")

    @classmethod
    def add_num_countries_per_year(cls: Type[Self], tb: Table) -> Table:
        """Get number of countries (and country-pairs) per region per year and add it to the table.

        `tb` is expected to be the table cow_ssm_system from the cow_ssm dataset.
        """
        # Get number of country-pairs per region per year
        tb["num_country_pairs"] = (tb["num_countries"] * (tb["num_countries"] - 1) / 2).astype(int)

        return tb

    @classmethod
    def add_indicators(cls: Type[Self], tb: Table, tb_codes: Table, columns_to_scale: List[str]) -> Table:
        """Scale columns `columns_to_scale` based on the number of countries (and country-pairs) in each region and year.

        For each indicator listed in `columns_to_scale`, two new columns are added to the table:
        - `{indicator}_per_country`: the indicator value divided by the number of countries in the region and year.
        - `{indicator}_per_country_pair`: the indicator value divided by the number of country-pairs in the region and year.
        """
        # From raw cow_ssm_system table get number of countryes (and country-pairs) per region per year
        tb_codes = cls.add_num_countries_per_year(tb_codes)
        # Merge with main table
        tb = tb.merge(tb_codes, on=["year", "region"], how="left")

        # Add normalised indicators
        for col in columns_to_scale:
            tb[f"{col}_per_country"] = tb[col] / tb["num_countries"]
            tb[f"{col}_per_country_pair"] = tb[col] / tb["num_country_pairs"]

        # Drop intermediate columns
        tb = tb.drop(columns=["num_countries", "num_country_pairs"])

        return tb


class COWNormaliser(Normaliser):
    """Normalise COW data based on the number of countries (and country-pairs) in each region and year."""

    country_column: str = "statenme"

    @classmethod
    def code_to_region(cls: Type[Self], cow_code: int) -> str:
        """Convert code to region name."""
        match cow_code:
            case c if 2 <= c <= 165:
                return "Americas"
            case c if 200 <= c <= 399:
                return "Europe"
            case c if 402 <= c <= 626:
                return "Africa"
            case c if 630 <= c <= 698:
                return "Middle East"
            case c if 700 <= c <= 999:
                return "Asia and Oceania"
            case _:
                raise ValueError(f"Invalid COW code: {cow_code}")


def add_region_from_code(tb: Table, mode: Literal["gw", "cow", "isd"], col_code: str = "id") -> Table:
    """Add region to table based on code (gw, cow, isd)."""
    tb_ = tb.copy()
    if mode == "gw":
        tb_["region"] = tb_[col_code].apply(_code_to_region_gw)
    elif mode == "cow":
        tb_["region"] = tb_[col_code].apply(_code_to_region_cow)
    elif mode == "isd":
        tb_["region"] = tb_[col_code].apply(_code_to_region_isd)
    else:
        raise ValueError(f"Invalid mode: {mode}")
    return tb_


def _code_to_region_gw(code: int) -> str:
    """Convert code to region name."""
    match code:
        case c if 2 <= c <= 199:
            return "Americas"
        case c if 200 <= c <= 399:
            return "Europe"
        case c if 400 <= c <= 626:
            return "Africa"
        case c if 630 <= c <= 699:
            return "Middle East"
        case c if 700 <= c <= 999:
            return "Asia and Oceania"
        case _:
            raise ValueError(f"Invalid GW code: {code}")


def _code_to_region_cow(code: int) -> str:
    """Convert code to region name."""
    match code:
        case c if 2 <= c <= 165:
            return "Americas"
        case c if 200 <= c <= 399:
            return "Europe"
        case c if 402 <= c <= 626:
            return "Africa"
        case c if 630 <= c <= 698:
            return "Middle East"
        case c if 700 <= c <= 999:
            return "Asia and Oceania"
        case _:
            raise ValueError(f"Invalid COW code: {code}")


def _code_to_region_isd(code: int) -> str:
    """Convert code to region name."""
    match code:
        case c if 2 <= c <= 165:
            return "Americas"
        case c if 200 <= c <= 399:
            return "Europe"
        case c if 402 <= c <= 626:
            return "Africa"
        case c if 630 <= c <= 698:
            return "Middle East"
        case c if 700 <= c <= 999:
            return "Asia and Oceania"
        case _:
            raise ValueError(f"Invalid COW code: {code}")


def fill_gaps_with_zeroes(
    tb: Table, columns: List[str], cols_use_range: Optional[List[str]] = None, use_nan: bool = False
) -> Table:
    """Fill missing values with zeroes.

    Makes sure all combinations of `columns` are present. If not present in the original table, then it is added with zero value.
    """
    # Build grid with all possible values
    values_possible = []
    for col in columns:
        if cols_use_range and col in cols_use_range:
            value_range = np.arange(tb[col].min(), tb[col].max() + 1)
            values_possible.append(value_range)
        else:
            values_possible.append(set(tb[col]))

    # Reindex
    new_idx = pd.MultiIndex.from_product(values_possible, names=columns)
    tb = tb.set_index(columns).reindex(new_idx).reset_index()

    # Fill zeroes
    if not use_nan:
        columns_fill = [col for col in tb.columns if col not in columns]
        tb[columns_fill] = tb[columns_fill].fillna(0)
    return tb


def aggregate_conflict_types(
    tb: Table,
    parent_name: str,
    children_names: Optional[List[str]] = None,
    columns_to_aggregate: List[str] = ["participated_in_conflict"],
    dim_name: str = "conflict_type",
) -> Table:
    """Aggregate metrics in broader conflict types."""
    if children_names is None:
        tb_agg = tb.copy()
    else:
        tb_agg = tb[tb[dim_name].isin(children_names)].copy()
    tb_agg = tb_agg.groupby(["year", "country", "id"], as_index=False).agg(
        {col: lambda x: min(x.sum(), 1) for col in columns_to_aggregate}
    )
    tb_agg[dim_name] = parent_name

    # Combine
    tb = pr.concat([tb, tb_agg], ignore_index=True)
    return tb


def get_number_of_countries_in_conflict_by_region(tb: Table, dimension_name: str, country_system: Literal["gw", "cow", "isd"]) -> Table:
    """Get the number of countries participating in conflicts by region."""
    # Add region
    tb_num_participants = add_region_from_code(tb, country_system)
    tb_num_participants = tb_num_participants.drop(columns=["country"]).rename(columns={"region": "country"})

    # Sanity check
    assert not tb_num_participants["id"].isna().any(), "Some countries with NaNs!"
    tb_num_participants = tb_num_participants.drop(columns=["id"])

    # Groupby sum (regions)
    tb_num_participants = tb_num_participants.groupby(["country", dimension_name, "year"], as_index=False)[
        "participated_in_conflict"
    ].sum()
    # Groupby sum (world)
    tb_num_participants_world = tb_num_participants.groupby([dimension_name, "year"], as_index=False)[
        "participated_in_conflict"
    ].sum()
    tb_num_participants_world["country"] = "World"
    # Combine
    tb_num_participants = pr.concat([tb_num_participants, tb_num_participants_world], ignore_index=True)
    tb_num_participants = tb_num_participants.rename(columns={"participated_in_conflict": "number_participants"})

    # Complement with missing entries
    tb_num_participants = fill_gaps_with_zeroes(
        tb_num_participants, ["country", dimension_name, "year"], cols_use_range=["year"]
    )

    return tb_num_participants
