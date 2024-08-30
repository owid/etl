"""Load a meadow dataset and create a garden dataset."""

from typing import Dict, List

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.data_helpers.population import add_population
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]
AGE_GROUPS_RANGES = {"All ages": [0, None]}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_risk_cancer")

    # Read table from meadow dataset.
    tb = ds_meadow["gbd_risk_cancer"].reset_index()
    ds_regions = paths.load_dataset("regions")
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Add regional aggregates
    tb = add_regional_aggregates(
        tb=tb,
        ds_regions=ds_regions,
        index_cols=["country", "year", "metric", "measure", "rei", "cause", "age", "sex"],
        regions=REGIONS,
        age_group_mapping=AGE_GROUPS_RANGES,
    )

    # Format the tables
    tb = tb.format(["country", "year", "metric", "measure", "rei", "age", "cause", "sex"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_regional_aggregates(
    tb: Table, ds_regions: Dataset, index_cols: List[str], regions: List[str], age_group_mapping: Dict[str, List[int]]
) -> Table:
    """
    Add regional aggregated data for the OWID continent regions.

    For 'Number', values are summed.
    For 'Percent', values are averaged.
    For 'Rate', it is calculated by dividing the sum of 'Number' values by the sum of the population.

    Parameters:
    - tb: Table containing the data.
    - ds_regions: Dataset containing region information.
    - index_cols: List of columns to use as index.
    - regions: List of regions to aggregate.
    - age_group_mapping: Mapping of age groups to age ranges.

    Returns:
    - Table with regional aggregates added.
    """

    def split_table_by_metric(tb: Table):
        tb_number_percent = tb[tb["metric"].isin(["Number", "Percent"])].copy()
        tb_rate = tb[tb["metric"] == "Rate"].copy()
        return tb_number_percent, tb_rate

    def add_population_data(tb: Table) -> Table:
        return add_population(
            df=tb,
            country_col="country",
            year_col="year",
            age_col="age",
            age_group_mapping=age_group_mapping,
            sex_col="sex",
            sex_group_all="Both",
            sex_group_female="Female",
            sex_group_male="Male",
        )

    def add_region_aggregates(
        tb: Table, metric: str, aggregation: str = "sum", weighted_vars={"value": False}, population_col="population"
    ) -> Table:
        return geo.add_regions_to_table(
            tb[tb["metric"] == metric].copy(),
            index_columns=index_cols,
            regions=regions,
            ds_regions=ds_regions,
            aggregations={"value": aggregation},
            min_num_values_per_year=1,
        )

    def calculate_rate(tb: Table) -> Table:
        tb_rate_regions = tb[(tb["country"].isin(regions)) & (tb["metric"] == "Number")].copy()
        tb_rate_regions["value"] = (tb_rate_regions["value"] / tb_rate_regions["population"]) * 100000
        tb_rate_regions["metric"] = "Rate"
        tb_rate_regions = tb_rate_regions.astype({"metric": "category"})
        return tb_rate_regions

    # Split the table into Number, Percent, and Rate
    tb_number_percent, tb_rate = split_table_by_metric(tb)

    # Add population data
    tb_number_percent = add_population_data(tb_number_percent)

    # Ensure no missing values in 'Number' table
    assert tb_number_percent["value"].notna().all(), "Values are missing in the Number table, check configuration"

    # Add region aggregates for Number
    tb_number = add_region_aggregates(tb_number_percent, "Number", aggregation="sum")
    # Add region aggregates for Percent with weighted mean
    tb_percent = add_region_aggregates(tb_number_percent, "Percent", aggregation="mean")

    # Concatenate Number and Percent tables
    tb_number_percent = pr.concat([tb_number, tb_percent], ignore_index=True)  # type: ignore

    # Calculate region aggregates for Rate
    tb_rate_regions = calculate_rate(tb_number)
    tb_rate = pr.concat([tb_rate, tb_rate_regions], ignore_index=True)  # type: ignore

    # Concatenate all tables
    tb_out = pr.concat([tb_number_percent, tb_rate], ignore_index=True)

    # Ensure categorical data types
    for col in ("age", "cause", "metric", "measure", "country"):
        if col in tb_out.columns:
            assert tb_out[col].dtype == "category", f"Column {col} is not of type 'category'"

    # Drop the population column
    tb_out = tb_out.drop(columns="population")
    return tb_out
