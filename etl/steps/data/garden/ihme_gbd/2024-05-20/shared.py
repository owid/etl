from typing import Dict, List

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr
from owid.datautils import dataframes

from etl.data_helpers import geo
from etl.data_helpers.population import add_population


def add_regional_aggregates(
    tb: Table, ds_regions: Dataset, index_cols: List[str], regions: List[str], age_group_mapping: Dict[str, List[int]]
) -> Table:
    """
    Adding the regional aggregated data for the OWID continent regions

    For Number and Percent we can sum the values, as the Percent denominator is the same across countries (total number of deaths/cases etc)
    Not all datasets will include Percent as this isn't always that meaningful e.g. in prevalence or incidence data.

    For Rate we need to calculate it for each region by dividing the sum of the 'Number' values by the sum of the population.
    """
    # Split the table into Number, Percent and Rate
    tb_number_percent = tb[tb["metric"].isin(["Number", "Percent"])].copy()
    tb_rate = tb[tb["metric"] == "Rate"].copy()
    # Add population data - some datasets will have data disaggregated by sex
    if "sex" in tb.columns:
        tb_number_percent = add_population(
            df=tb_number_percent,
            country_col="country",
            year_col="year",
            age_col="age",
            age_group_mapping=age_group_mapping,
            sex_col="sex",
            sex_group_all="Both",
            sex_group_female="Female",
            sex_group_male="Male",
        )
    else:
        tb_number_percent = add_population(
            df=tb_number_percent,
            country_col="country",
            year_col="year",
            age_col="age",
            age_group_mapping=age_group_mapping,
        )
    assert tb_number_percent["value"].notna().all(), "Values are missing in the Number table, check configuration"
    # Add region aggregates - for Number and Percent (if present)
    tb_number_percent = geo.add_regions_to_table(
        tb_number_percent,
        index_columns=index_cols,
        regions=regions,
        ds_regions=ds_regions,
        min_num_values_per_year=1,
    )
    # Calculate region aggregates - for Rate
    tb_rate_regions = tb_number_percent[
        (tb_number_percent["country"].isin(regions)) & (tb_number_percent["metric"] == "Number")
    ].copy()

    # Calculate rates per 100,000 for regions
    tb_rate_regions["value"] = (tb_rate_regions["value"] / tb_rate_regions["population"]) * 100000
    tb_rate_regions["metric"] = "Rate"
    tb_rate_regions = tb_rate_regions.astype({"metric": "category"})

    # TODO: maybe we could update pr.concat to work with categoricals if pandas can't handle them and converts
    # them to objects
    tb_rate = Table(dataframes.concatenate([tb_rate, tb_rate_regions], ignore_index=True)).copy_metadata(tb_rate)
    tb_out = Table(dataframes.concatenate([tb_number_percent, tb_rate], ignore_index=True)).copy_metadata(
        tb_number_percent
    )
    tb_out = tb_out.drop(columns="population")
    return tb_out


def add_share_population(tb: Table) -> Table:
    """
    Add a share of the population column to the table.
    The 'Rate' column is the number of cases per 100,000 people, we want the equivalent per 100 people.
    """
    tb_share = tb[tb["metric"] == "Rate"].copy()
    tb_share["metric"] = "Share"
    tb_share["value"] = tb_share["value"] / 1000

    tb = pr.concat([tb, tb_share], ignore_index=True).astype({"metric": "category"})
    return tb
