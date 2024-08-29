"""Load a meadow dataset and create a garden dataset."""

from shared import add_regional_aggregates, add_share_population

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]
AGE_GROUPS_RANGES = {
    "All ages": [0, None],
    "<5 years": [0, 4],
    "5-14 years": [5, 14],
    "15-19 years": [15, 19],
    "15-49 years": [15, 49],
    "20-24 years": [20, 24],
    "25-29 years": [25, 29],
    "30-34 years": [30, 34],
    "35-39 years": [35, 39],
    "40-44 years": [40, 44],
    "45-49 years": [45, 49],
    "50-54 years": [50, 54],
    "50-69 years": [50, 69],
    "55-59 years": [55, 59],
    "60-64 years": [60, 64],
    "65-69 years": [65, 69],
    "70+ years": [70, None],
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_mental_health")
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    tb = ds_meadow["gbd_mental_health"].reset_index()
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = add_regional_aggregates(
        tb,
        ds_regions,
        index_cols=["country", "year", "metric", "cause", "age", "sex"],
        regions=REGIONS,
        age_group_mapping=AGE_GROUPS_RANGES,
    )
    # Add a share of the population column
    tb = add_share_population(tb)
    #

    # Format the tables
    tb = tb.format(["country", "year", "cause", "metric", "sex", "age"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        # Table has optimal types already and repacking can be time consuming.
        repack=False,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


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

    For Number and Percent we can sum the values, as the Percent denominator is the same across countries (total number of deaths/cases etc)
    Not all datasets will include Percent as this isn't always that meaningful e.g. in prevalence or incidence data.

    For Rate we need to calculate it for each region by dividing the sum of the 'Number' values by the sum of the population.
    """
    # Split the table into Number, Percent and Rate
    tb_percent = tb[tb["metric"].isin(["Percent"])].copy()
    tb_number = tb[tb["metric"].isin(["Number"])].copy()
    tb_rate = tb[tb["metric"] == "Rate"].copy()

    # Calculate region aggregates for Number
    tb_number = add_regions_to_number(tb_number, age_group_mapping, ds_regions, index_cols, regions)
    # Calculate region aggregates for Rate
    tb_rate_regions = add_regions_to_rate(tb_number, regions)
    # Calculate regions aggregates for Percent if it is in the dataset
    if tb_percent.shape[0] > 1:
        tb_percent_regions = add_regions_to_percent(tb_number, regions, index_cols)
    else:
        tb_percent_regions = Table()
    tb_rate = pr.concat([tb_rate, tb_rate_regions], ignore_index=True)  # type: ignore
    # Percent regional aggregates not really working for risk factors due to negative values where some 'risks' reduce deaths
    if "rei" not in index_cols:
        # Check there aren't any values above 100
        tb_percent = pr.concat([tb_percent, tb_percent_regions], ignore_index=True)
        assert tb_percent["value"].max() <= 100 or tb_percent.shape[0] == 0
    # Combine all the metrics back together
    tb_out = pr.concat([tb_number, tb_rate, tb_percent], ignore_index=True)
    for col in ("age", "cause", "metric", "measure", "country"):
        if col in tb_out.columns:
            assert tb_out[col].dtype == "category"
    assert tb_out.age.m.origins
    tb_out = tb_out.drop(columns="population")
    return tb_out


def add_regions_to_percent(tb_number: Table, regions: List[str], index_cols: List[str]) -> Table:
    """
    Calculating the share of deaths using the value of 'All causes' for each dataset as a denominator
    """
    tb_number = tb_number.drop(columns="population")
    tb_percent = tb_number[(tb_number["country"].isin(regions)) & (tb_number["metric"] == "Number")].copy()
    # Grab the all causes data
    all_causes = tb_number[tb_number["cause"] == "All causes"]
    all_causes = all_causes.rename(columns={"value": "all_causes"}).drop(columns=["cause"])
    # Merge it back in to the main dataset
    cols = index_cols.remove("cause")
    tb_percent = tb_percent.merge(all_causes, on=cols)
    # Use the all causes as denominator of all other causes
    tb_percent["share"] = tb_percent["value"] / tb_percent["all_causes"] * 100
    tb_percent = tb_percent.drop(columns=["value", "all_causes"])
    tb_percent = tb_percent.rename(columns={"share": "value"})
    tb_percent["metric"] = "Percent"

    return tb_percent


def add_regions_to_rate(tb_number: Table, regions: List[str]) -> Table:
    tb_rate = tb_number[(tb_number["country"].isin(regions)) & (tb_number["metric"] == "Number")].copy()

    # Calculate rates per 100,000 for regions
    tb_rate["value"] = (tb_rate["value"] / tb_rate["population"]) * 100000
    tb_rate["metric"] = "Rate"
    tb_rate = tb_rate.astype({"metric": "category"})
    return tb_rate


def add_regions_to_number(
    tb_number: Table,
    age_group_mapping: Dict[str, List[int]],
    ds_regions: Dataset,
    index_cols: List[str],
    regions: List[str],
) -> Table:
    # Add population data - some datasets will have data disaggregated by sex
    if "sex" in tb_number.columns:
        tb_number = add_population(
            df=tb_number,
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
        tb_number = add_population(
            df=tb_number,
            country_col="country",
            year_col="year",
            age_col="age",
            age_group_mapping=age_group_mapping,
        )
    assert tb_number["value"].notna().all(), "Values are missing in the Number table, check configuration"
    # Add region aggregates - for Number
    tb_number = geo.add_regions_to_table(
        tb_number,
        index_columns=index_cols,
        regions=regions,
        ds_regions=ds_regions,
        min_num_values_per_year=1,
    )
    return tb_number


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
