"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Origin, Table
from owid.catalog import processing as pr
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = [reg for reg in geo.REGIONS.keys() if reg not in ["European Union (27)", "North America"]] + ["World"]

LATEST_YEAR = 2020  # latest year for which data is available - to not include predictions from UN WPP

REPRODUCTIVE_AGES = ["15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49"]

# regions where who data on maternal deaths is not reliable (see also https://docs.google.com/spreadsheets/d/1aCNKea-M489pOSLUpurQojEED3ATqnRZeTsSpTNXRA0/edit?gid=0#gid=0)
WHO_REMOVE_REGIONS = [
    "Israel",
    "Suriname",
    "Brazil",
    "Trinidad and Tobago",
    "Guyana",
    "Puerto Rico",
    "Guatemala",
    "Dominican Republic",
    "Belize",
    "Costa Rica",
    "Chile",
    "Jamaica",
    "Russia",
    "Thailand",
    "Ecuador",
    "Argentina",
    "Colombia",
    "Sao Tome and Principe",
    "Armenia",
    "Azerbaijan",
    "Belarus",
    "Cape Verde",
    "Egypt",
    "Estonia",
    "Fiji",
    "French Guiana",
    "Georgia",
    "Guadeloupe",
    "Kazakhstan",
    "Kyrgyzstan",
    "Lithuania",
    "Martinique",
    "Moldova",
    "Philippines",
    "Saint Kitts and Nevis",
    "Saint Vincent and the Grenadines",
    "Tajikistan",
    "Turkmenistan",
    "Ukraine",
    "Uzbekistan",
]


def run() -> None:
    """Creates long running data set on maternal mortality combining following sources (in brackets - timeframes available for each source):
    - Gapminder maternal mortality data (1751 - 2008)
    - WHO mortality database (1950 - 2022) & UN WPP (1950 - 2020)
    - UN MMEIG maternal mortality data (1985 - 2023)
    We combine them following the hierarchy below, where the most recent data is used when available:
    - UN MMEIG >> WHO/UN >> Gapminder

    For the timeframe ~1950-1984 we calculate maternal mortality ratio and rate out of WHO mortality database and UN WPP by using:
    - death from maternal conditions (all sexes, all ages) from WHO mortality database
    - births and female population aged 14-49 from UN WPP

    We also create regional aggregates if a region is more than 90% covered by our data for a given year"""

    #
    # Load inputs.
    #
    # Load data sets from Gapminder and UN
    ds_gm = paths.load_dataset("maternal_mortality", namespace="gapminder")
    ds_un = paths.load_dataset("maternal_mortality", namespace="un")
    # ds_regions = paths.load_dataset("regions")
    # ds_income = paths.load_dataset("income_groups")
    ds_who_mortality = paths.load_dataset("mortality_database")
    ds_wpp = paths.load_dataset("un_wpp")
    ds_pop = paths.load_dataset("population")

    # save origins for later:
    # who_origins = sources_to_origins_who(ds_who_mortality)

    # Read table from meadow dataset.
    tb_gm = ds_gm.read("maternal_mortality", reset_metadata="keep_origins")
    tb_un = ds_un.read("maternal_mortality", reset_metadata="keep_origins")
    tb_who_mortality = ds_who_mortality.read("mortality_database", reset_metadata="keep_origins")
    # Filtering out the data we need from WHO mortality database
    tb_who_mortality = tb_who_mortality[
        (tb_who_mortality["cause"] == "Maternal conditions")
        & (tb_who_mortality["age_group"] == "all ages")
        & (tb_who_mortality["sex"] == "Both sexes")
    ]
    assert tb_who_mortality.shape[0] > 0
    tb_wpp_pop = ds_wpp.read("population", reset_metadata="keep_origins")
    tb_wpp_births = ds_wpp.read("births", reset_metadata="keep_origins")

    # calculate maternal mortality ratio/ rate out of WHO mortality database and UN WPP
    tb_who_mortality = tb_who_mortality.rename(columns={"number": "maternal_deaths"})[
        ["country", "year", "maternal_deaths"]
    ]
    tb_who_mortality = tb_who_mortality[tb_who_mortality["year"] <= LATEST_YEAR]
    # who data has a bunch of zeros, which seem to be missing data, so we replace them with NA
    tb_who_mortality["maternal_deaths"] = tb_who_mortality["maternal_deaths"].replace(0, pd.NA)
    # remove regions with data we don't want to use
    tb_who_mortality = tb_who_mortality[~tb_who_mortality["country"].isin(WHO_REMOVE_REGIONS)]

    # we need births from WPP
    tb_wpp_births = tb_wpp_births[
        (tb_wpp_births["variant"] == "estimates")
        & (tb_wpp_births["year"] <= LATEST_YEAR)
        & (tb_wpp_births["age"] == "all")
    ]
    tb_wpp_births = tb_wpp_births.rename(columns={"births": "live_births"})[["country", "year", "live_births"]]

    # we need female population of reproductive age from WPP
    tb_wpp_fm_pop = tb_wpp_pop[
        (tb_wpp_pop["variant"] == "estimates")
        & (tb_wpp_pop["year"] <= LATEST_YEAR)
        & (tb_wpp_pop["age"].isin(REPRODUCTIVE_AGES))
        & (tb_wpp_pop["sex"] == "female")
    ]
    tb_wpp_fm_pop = tb_wpp_fm_pop.rename(columns={"population": "female_population"})
    tb_wpp_fm_pop = (
        tb_wpp_fm_pop[["country", "year", "female_population"]].groupby(by=["country", "year"]).sum().reset_index()
    )

    # combine births and female population:
    tb_wpp = pr.merge(tb_wpp_births, tb_wpp_fm_pop, on=["country", "year"], how="left")

    # combine with who mortality and calculate maternal mortality ratio and rate
    tb_calc_mm = pr.merge(tb_wpp, tb_who_mortality, on=["country", "year"], how="outer")
    tb_calc_mm["mmr"] = (tb_calc_mm["maternal_deaths"] / tb_calc_mm["live_births"]) * 100_000
    tb_calc_mm["mmr_rate"] = (tb_calc_mm["maternal_deaths"] / tb_calc_mm["female_population"]) * 100_000

    # combine with Gapminder data - using WHO/ UN WPP data where available
    tb_gm["maternal_deaths"] = tb_gm["maternal_deaths"].round().astype("UInt32")
    tb_who_gm = combine_two_overlapping_dataframes(df1 = tb_calc_mm, df2 = tb_gm, index_columns=["country", "year"])

    # calculate mmr and mm rate if NA from Gapminder deaths with UN WPP data
    tb_who_gm["mmr"] = tb_who_gm["mmr"].combine_first(tb_who_gm["maternal_deaths"] / tb_who_gm["live_births"] * 100_000)
    tb_who_gm["mmr_rate"] = tb_who_gm["mmr_rate"].combine_first(
        tb_who_gm["maternal_deaths"] / tb_who_gm["female_population"] * 100_000
    )

    # join the two tables
    # first - rename columns so they have the same names
    tb_un = tb_un.rename(columns={"births": "live_births"})

    # combine UN MMEIG with WHO/UN/GM using UN MMEIG preferentially
    tb = combine_two_overlapping_dataframes(df1 = tb_un, df2 = tb_who_gm, index_columns=["country", "year"])

    # remove all rows that don't have any data on maternal deaths, mmr or mm rate
    tb = tb.dropna(subset=["maternal_deaths", "mmr", "mmr_rate"], how="all")

    # calculate regional aggregates - population is needed for filtering out all regions that are not sufficiently covered by our data
    tb = geo.add_population_to_table(tb, ds_pop)

    # aggr = {"maternal_deaths": "sum", "live_births": "sum", "female_population": "sum", "population": "sum"}

    # tb = geo.add_regions_to_table(
    #    tb=tb,
    #    regions=REGIONS,
    #    ds_regions=ds_regions,
    #    ds_income_groups=ds_income,
    #    aggregations=aggr,
    #    num_allowed_nans_per_year=0,
    # )

    # remove all regions that are less than 90% covered by our data
    # tb = check_region_share_population(tb, REGIONS, ds_pop, 0.9)

    # calculate aggregated maternal mortality ratio and rate for regions
    # tb["mmr"] = tb.apply(lambda x: calc_mmr(x), axis=1)
    # tb["mm_rate"] = tb.apply(lambda x: calc_mmrate(x), axis=1)

    # drop all columns that are not 1) long run or 2) not related to maternal mortality
    cols_to_keep = ["country", "year", "maternal_deaths", "mmr", "live_births", "mmr_rate"]
    tb = tb[cols_to_keep]

    # drop rows where there is no data for maternal mortality indicators (introduced by aggregating regions)
    tb = tb.dropna(subset=["maternal_deaths", "mmr", "mmr_rate"], how="all")

    # fix dtypes (coerce errors since NAs are not accepted otherwise)
    tb["maternal_deaths"] = (
        pd.to_numeric(tb["maternal_deaths"], errors="coerce")
        .round()
        .astype("Int64")
        .copy_metadata(tb["maternal_deaths"])
    )
    tb["live_births"] = (
        pd.to_numeric(tb["live_births"], errors="coerce").round().astype("Int64").copy_metadata(tb["live_births"])
    )
    tb["mmr"] = pd.to_numeric(tb["mmr"], errors="coerce").copy_metadata(tb["mmr"])
    tb["mmr_rate"] = pd.to_numeric(tb["mmr_rate"], errors="coerce").copy_metadata(tb["mmr_rate"])

    # index and format columns
    tb = tb.format(["country", "year"], short_name="maternal_mortality")

    # add who origins to the table
    # tb = add_origins(tb, who_origins, ["maternal_deaths", "mmr", "mm_rate"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()


def calc_mmr(tb_row):
    """If country is a region, calculate the maternal mortality ratio, else return MMR"""
    if tb_row["country"] in REGIONS:
        return (tb_row["maternal_deaths"] / tb_row["live_births"]) * 100_000
    return tb_row["mmr"]


def calc_mmrate(tb_row):
    """If country is a region, calculate the maternal mortality rate, else return MM rate"""
    if tb_row["country"] in REGIONS:
        return (tb_row["maternal_deaths"] / tb_row["female_population"]) * 100_000
    return tb_row["mmr_rate"]


def check_region_share_population(tb: Table, regions: list, ds_population: Dataset, threshold: float) -> Table:
    """
    Check the share of population covered by the regions in the table.
    """
    msk = tb["country"].isin(regions)
    tb_region = tb[msk]
    tb_no_regions = tb[~msk]
    tb_region = geo.add_population_to_table(tb_region, ds_population, population_col="total_population")
    tb_region["share_population"] = tb_region["population"] / tb_region["total_population"]
    tb_region = tb_region[tb_region["share_population"] >= threshold]

    tb_region = tb_region.drop(columns=["total_population", "share_population"])
    tb = pr.concat([tb_region, tb_no_regions])
    return tb


def sources_to_origins_who(ds: Dataset) -> list[Origin]:
    """Create an origin from the sources of the who dataset."""
    origins = []
    for source in ds.metadata.sources:
        origin_ds = Origin(
            producer="WHO Mortality Database",
            description=source.description,
            attribution_short="WHO",
            title=source.name,
            date_published=source.publication_date,
            url_main=source.url,
            citation_full="World Health Organization. 'WHO Mortality Database.' 2022, https://www.who.int/data/data-collection-tools/who-mortality-database.",
            license=ds.licenses[0],
            date_accessed=source.date_accessed,
        )
        origins.append(origin_ds)
    return origins


def add_origins(tb: Table, origins: list[Origin], cols=None):
    if cols is None:
        cols = tb.columns
    for origin in origins:
        for col in cols:
            tb[col].metadata.origins = tb[col].metadata.origins + [origin]
    return tb
