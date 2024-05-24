from typing import Dict, List

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.data_helpers.population import add_population


def add_regional_aggregates(
    tb: Table, ds_regions: Dataset, index_cols: List[str], regions: List[str], age_group_mapping: Dict[str, List[int]]
) -> Table:
    """
    Adding the regional aggregated data for the OWID continent regions
    """
    tb_number = tb[tb["metric"].isin(["Number", "Percent"])].copy()
    tb_number = add_population(
        df=tb_number, country_col="country", year_col="year", age_col="age", age_group_mapping=age_group_mapping
    )
    tb_rate = tb[tb["metric"] == "Rate"].copy()
    tb_percent = tb[tb["metric"] == "Percent"].copy()
    tb_number_percent = pr.concat([tb_number, tb_percent], ignore_index=True)
    # Add region aggregates - for Number
    tb_number_percent = geo.add_regions_to_table(
        tb_number_percent,
        index_columns=index_cols,
        regions=regions,
        ds_regions=ds_regions,
        min_num_values_per_year=1,
    )
    # Add region aggregates - for Rate - only need population here?

    tb_rate_regions = tb_number[tb_number["country"].isin(regions)].copy()
    tb_rate_regions["value"] = tb_number["value"] / tb_number["population"] * 100_000
    tb_rate_regions["metric"] = "Rate"
    tb_rate = pr.concat([tb_rate, tb_rate_regions], ignore_index=True)
    tb_rate = tb_rate.drop(columns="population")

    tb_out = pr.concat(
        [tb_number_percent, tb_rate],
        ignore_index=True,
    )
    return tb_out
