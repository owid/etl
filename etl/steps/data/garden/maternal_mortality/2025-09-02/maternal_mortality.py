"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

LATEST_YEAR = 2023  # latest year for which data is available - to not include predictions from UN WPP

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
    tb_who_gm = combine_two_overlapping_dataframes(df1=tb_calc_mm, df2=tb_gm, index_columns=["country", "year"])

    # calculate mmr and mm rate if NA from Gapminder deaths with UN WPP data
    tb_who_gm["mmr"] = tb_who_gm["mmr"].combine_first(tb_who_gm["maternal_deaths"] / tb_who_gm["live_births"] * 100_000)
    tb_who_gm["mmr_rate"] = tb_who_gm["mmr_rate"].combine_first(
        tb_who_gm["maternal_deaths"] / tb_who_gm["female_population"] * 100_000
    )

    # join the two tables
    # first - rename columns so they have the same names
    tb_un = tb_un.rename(columns={"births": "live_births"})

    # combine UN MMEIG with WHO/UN/GM using UN MMEIG preferentially
    tb_un, tb_who_gm = align_dtypes(tb_un, tb_who_gm)
    tb = combine_two_overlapping_dataframes(df1=tb_un, df2=tb_who_gm, index_columns=["country", "year"])

    # remove all rows that don't have any data on maternal deaths, mmr or mm rate
    tb = tb.dropna(subset=["maternal_deaths", "mmr", "mmr_rate"], how="all")

    # calculate regional aggregates - population is needed for filtering out all regions that are not sufficiently covered by our data
    tb = geo.add_population_to_table(tb, ds_pop)

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
    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()


def align_dtypes(tb1: Table, tb2: Table) -> tuple[Table, Table]:
    shared_cols = tb1.columns.intersection(tb2.columns)

    for col in shared_cols:
        dtype1 = tb1[col].dtype
        dtype2 = tb2[col].dtype

        if dtype1 != dtype2:
            # Coerce both to the "wider" of the two types
            if pd.api.types.is_integer_dtype(dtype1) and pd.api.types.is_float_dtype(dtype2):
                tb1[col] = tb1[col].astype("Float64")
            elif pd.api.types.is_float_dtype(dtype1) and pd.api.types.is_integer_dtype(dtype2):
                tb2[col] = tb2[col].astype("Float64")
            elif dtype1.name.startswith("string") or dtype2.name.startswith("string"):
                tb1[col] = tb1[col].astype("string")
                tb2[col] = tb2[col].astype("string")
            else:
                # fallback to object if uncertain
                tb1[col] = tb1[col].astype("object")
                tb2[col] = tb2[col].astype("object")

    return tb1, tb2
