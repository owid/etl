from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("un_wpp")
    #
    # Process data.
    #
    tables = [
        # ds_garden["population_january"],
        # ds_garden["population"],
        ds_garden["growth_rate"],
        ds_garden["natural_change_rate"],
        ds_garden["fertility_rate"],
        ds_garden["migration"],
        ds_garden["deaths"],
        ds_garden["births"],
        ds_garden["life_expectancy"],
        ds_garden["mortality_rate"],
    ]
    # Add population change - January
    tb_pop_jan = ds_garden["population_january"].reset_index()
    tb_pop_jan = add_population_change(tb_pop_jan, "population_change_january", "population_january")
    tables = tables + [tb_pop_jan]
    # Add population change - July
    tb_pop_jul = ds_garden["population"].reset_index()
    tb_pop_jul = add_population_change(tb_pop_jul, "population_change_july", "population_july")
    tables = tables + [tb_pop_jul]

    # Reset the index for all tables to prepare for merging
    tables = [tb.reset_index() for tb in tables]

    tb = pr.multi_merge(tables, on=["country", "year", "sex", "age", "variant"], how="outer")
    # This should be the same as population_change
    tb["check_pop_change"] = tb["births"] - tb["deaths"] + tb["net_migration"]
    tb["change_diff_january"] = tb["population_change_january"] - tb["check_pop_change"]
    tb["change_diff_july"] = tb["population_change_july"] - tb["check_pop_change"]

    simple_tb = get_simple_data(tb)
    age_sex_tb = get_age_sex_data(tb)
    full_tb = get_full_data(tb)
    ds_garden = paths.create_dataset(
        tables=[simple_tb, age_sex_tb, full_tb],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_population_change(tb: Table, column_pop_change: str, column_population: str) -> Table:
    """Estimate anual population change."""

    # Sort by year
    tb = tb.sort_values(["country", "year", "sex"])

    # Estimate population change # Fiona: I'm not sure this is done in the same way as WPP do for their population change variable. They have data for 1950 and it shows the difference between 1950 and 1951
    tb[column_pop_change] = tb.groupby(["country", "sex", "age", "variant"])["population"].transform(
        lambda x: x.diff().shift(-1)
    )
    tb = tb.drop(columns=["population_change", "population_density"])  # drop month as we don't need it anymore
    # Rename population column to indicate the month
    tb = tb.rename(columns={"population": column_population})
    tb = tb.format(["country", "year", "sex", "age", "variant"])

    return tb


def get_simple_data(tb: Table) -> Table:
    """
    Getting a simple subset of the data, where only have data for all ages, both sexes and the estimated historical data, with the medium variant.
    """

    tb = tb[(tb["age"] == "all") & (tb["sex"] == "all")]
    tb = tb[tb["variant"].isin(["estimates", "medium"])]
    tb = tb.format(["country", "year", "sex", "age", "variant"], short_name="total_populations_medium")
    return tb


def get_age_sex_data(tb: Table) -> Table:
    """
    Getting a subset of the data, where we have data for all age groups and both sexes, with the medium variant only.
    """

    tb = tb[tb["variant"].isin(["estimates", "medium"])]
    tb = tb[
        tb["age"].isin(
            [
                "0-14",
                "15-64",
                "65+",
                "all",
                "0-4",
                "5-9",
                "10-14",
                "15-19",
                "20-24",
                "25-29",
                "30-34",
                "35-39",
                "40-44",
                "45-49",
                "50-54",
                "55-59",
                "60-64",
                "65-69",
                "70-74",
                "75-79",
                "80-84",
                "85-89",
                "90-94",
                "95-99",
                "100+",
            ]
        )
    ]  # Keeping only standard age groups
    tb = tb.format(["country", "year", "sex", "age", "variant"], short_name="age_sex_populations_medium")
    return tb


def get_full_data(tb: Table) -> Table:
    """
    Getting the full dataset, with all age standard age groups and low and high variants.
    """
    tb = tb[tb["variant"].isin(["estimates", "medium", "low", "high"])]
    tb = tb[
        tb["age"].isin(
            [
                "0-14",
                "15-64",
                "65+",
                "all",
                "0-4",
                "5-9",
                "10-14",
                "15-19",
                "20-24",
                "25-29",
                "30-34",
                "35-39",
                "40-44",
                "45-49",
                "50-54",
                "55-59",
                "60-64",
                "65-69",
                "70-74",
                "75-79",
                "80-84",
                "85-89",
                "90-94",
                "95-99",
                "100+",
            ]
        )
    ]  # Keeping only standard age groups
    tb = tb.format(["country", "year", "sex", "age", "variant"], short_name="all_scenarios")
    return tb
