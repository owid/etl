from typing import List, Optional

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers.misc import expand_time_column


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


def aggregate_conflict_types(
    tb: Table,
    parent_name: str,
    children_names: Optional[List[str]] = None,
    columns_to_aggregate: Optional[List[str]] = None,
    columns_to_aggregate_absolute: Optional[List[str]] = None,
    columns_to_groupby: Optional[List[str]] = None,
    dim_name: str = "conflict_type",
) -> Table:
    """Aggregate metrics in broader conflict types."""
    if columns_to_aggregate is None:
        columns_to_aggregate = ["participated_in_conflict"]
    if columns_to_groupby is None:
        columns_to_groupby = ["year", "country", "id"]
    if columns_to_aggregate_absolute is None:
        columns_to_aggregate_absolute = []
    if children_names is None:
        tb_agg = tb.copy()
    else:
        tb_agg = tb[tb[dim_name].isin(children_names)].copy()
    # Obtain summations
    tb_agg = tb_agg.groupby(columns_to_groupby, as_index=False).agg({col: sum for col in columns_to_aggregate})
    # Threshold to 1 for binary columns
    threshold_upper = 1
    for col in columns_to_aggregate:
        if col not in columns_to_aggregate_absolute:
            tb_agg[col] = tb_agg[col].apply(lambda x: min(x, threshold_upper))
    # Add conflict type
    tb_agg[dim_name] = parent_name

    # Combine
    tb = pr.concat([tb, tb_agg], ignore_index=True)
    return tb


def get_number_of_countries_in_conflict_by_region(tb: Table, dimension_name: str) -> Table:
    """Get the number of countries participating in conflicts by region."""
    # Add region
    tb_num_participants = add_region_from_code(tb)
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
    tb_num_participants = expand_time_column(
        tb_num_participants,
        dimension_col=["country", dimension_name],
        time_col="year",
        method="full_range",
        fillna_method="zero",
    )

    return tb_num_participants


def add_region_from_code(tb: Table, col_code: str = "id") -> Table:
    """Add region to table based on code (gw, cow, isd)."""

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

    tb_ = tb.copy()
    tb_["region"] = tb_[col_code].apply(_code_to_region_gw)
    return tb_
