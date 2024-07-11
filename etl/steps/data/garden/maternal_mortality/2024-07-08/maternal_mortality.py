"""Load a meadow dataset and create a garden dataset."""

from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = [reg for reg in geo.REGIONS.keys() if reg != "European Union (27)"] + ["World"]


def run(dest_dir: str) -> None:
    """Creates long running data set on maternal mortality combining following sources (in brackets - timeframes available for each source):
    - Gapminder maternal mortality data (1751 - 2008)
    - WHO mortality database (1950 - 2020) & UN WPP (1950 - 2020)
    - UN MMEIG maternal mortality data (1985 - 2020)
    We combine them following the hierarchy below, where the most recent data is used when available:
    - UN MMEIG >> WHO/UN >> Gapminder

    For the timeframe ~1950-1984 we calculate maternal mortality ratio and rate out of WHO mortality database and UN WPP by using:
    - death from maternal conditions (all sexes, all ages) from WHO mortality database
    - births and female population aged 14-49 from UN WPP"""

    #
    # Load inputs.
    #
    # Load data sets from Gapminder and UN
    ds_gm = paths.load_dataset("maternal_mortality", namespace="gapminder")
    ds_un = paths.load_dataset("maternal_mortality", namespace="un")
    ds_regions = paths.load_dataset("regions")
    ds_income = paths.load_dataset("income_groups")
    ds_who_mortality = paths.load_dataset("mortality_database")
    ds_wpp = paths.load_dataset("un_wpp")

    # Read table from meadow dataset.
    tb_gm = ds_gm["maternal_mortality"].reset_index()
    tb_un = ds_un["maternal_mortality"].reset_index()
    tb_who_mortality = ds_who_mortality["maternal_conditions__both_sexes__all_ages"].reset_index()
    tb_wpp = ds_wpp["un_wpp"].reset_index()

    # calculate maternal mortality ratio/ rate out of WHO mortality database and UN WPP
    tb_who_mortality = tb_who_mortality.rename(
        columns={"total_deaths_that_are_from_maternal_conditions__in_both_sexes_aged_all_ages": "maternal_deaths"}
    )[["country", "year", "maternal_deaths"]]
    tb_who_mortality = tb_who_mortality[tb_who_mortality["year"] < 1985]

    # we need births from WPP but only until 1985 (when UN MMEIG data starts)
    tb_wpp_births = tb_wpp[(tb_wpp["metric"] == "births") & (tb_wpp["year"] < 1985) & (tb_wpp["age"] == "all")]
    tb_wpp_births = tb_wpp_births.rename(columns={"location": "country", "value": "live_births"})[
        ["country", "year", "live_births"]
    ]
    # we need female population of reproductive age from WPP until 1985
    reproductive_ages = ["15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49"]
    tb_wpp_fm_pop = tb_wpp[
        (tb_wpp["metric"] == "population")
        & (tb_wpp["year"] < 1985)
        & (tb_wpp["age"].isin(reproductive_ages))
        & (tb_wpp["sex"] == "female")
    ]
    tb_wpp_fm_pop = tb_wpp_fm_pop.rename(columns={"location": "country", "value": "female_population"})
    tb_wpp_fm_pop = (
        tb_wpp_fm_pop[["country", "year", "female_population"]].groupby(by=["country", "year"]).sum().reset_index()
    )

    # calculate maternal mortality indicators
    tb_calc_mm = tb_who_mortality.merge(tb_wpp_births, on=["country", "year"], how="left")
    tb_calc_mm = tb_calc_mm.merge(tb_wpp_fm_pop, on=["country", "year"], how="left")
    tb_calc_mm["mmr"] = (tb_calc_mm["maternal_deaths"] / tb_calc_mm["live_births"]) * 100_000
    tb_calc_mm["mm_rate"] = (tb_calc_mm["maternal_deaths"] / tb_calc_mm["female_population"]) * 100_000

    aggr = {"maternal_deaths": "sum", "births": "sum"}

    # regional aggregates (before joining - since Gapminder/ WHO only include subset of countries)
    tb_un = geo.add_regions_to_table(
        tb=tb_un,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income,
        aggregations=aggr,
        num_allowed_nans_per_year=5,
    )

    # calculate aggregated maternal mortality ratio for regions
    tb_un["mmr"] = tb_un.apply(lambda x: calc_mmr(x), axis=1)

    # join the two tables
    # first - rename columns so they have the same names
    tb_un = tb_un.rename(columns={"births": "live_births"})

    # combine dataframes - first UN MMEIG with WHO/UN, then Gapminder with the result
    tb = combine_two_overlapping_dataframes(tb_un, tb_calc_mm, index_columns=["country", "year"])
    tb = combine_two_overlapping_dataframes(tb, tb_gm, index_columns=["country", "year"])

    # since this is the long run dataset, drop all columns not in both datasets
    cols_to_keep = ["country", "year", "maternal_deaths", "mmr", "live_births", "mm_rate"]
    tb = tb[cols_to_keep]

    # index and format columns
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()


def calc_mmr(tb_row):
    """If country is a region, calculate the maternal mortality ratio, else return MMR"""
    if tb_row["country"] in REGIONS:
        return (tb_row["maternal_deaths"] / tb_row["births"]) * 100_000
    return tb_row["mmr"]
