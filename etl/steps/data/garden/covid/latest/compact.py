"""Load a meadow dataset and create a garden dataset."""

from ast import literal_eval
from typing import cast

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# What to load
INDICATORS = [
    {
        "short_name": "cases_deaths",
        "indicators": [
            "total_cases",
            "new_cases",
            "new_cases_7_day_avg_right",
            "total_cases_per_million",
            "new_cases_per_million",
            "new_cases_per_million_7_day_avg_right",
            "total_deaths",
            "new_deaths",
            "new_deaths_7_day_avg_right",
            "total_deaths_per_million",
            "new_deaths_per_million",
            "new_deaths_per_million_7_day_avg_right",
        ],
        "column_rename": {
            # Cases
            "new_cases_7_day_avg_right": "new_cases_smoothed",
            "new_cases_per_million_7_day_avg_right": "new_cases_smoothed_per_million",
            # Deaths
            "new_deaths_7_day_avg_right": "new_deaths_smoothed",
            "new_deaths_per_million_7_day_avg_right": "new_deaths_smoothed_per_million",
        },
    },
    {
        "short_name": "excess_mortality",
        "indicators": [
            "p_proj_all_ages",
            "cum_p_proj_all_ages",
            "cum_excess_proj_all_ages",
            "cum_excess_per_million_proj_all_ages",
        ],
        "column_rename": {
            "entity": "country",
            "p_proj_all_ages": "excess_mortality",
            "cum_p_proj_all_ages": "excess_mortality_cumulative",
            "cum_excess_proj_all_ages": "excess_mortality_cumulative_absolute",
            "cum_excess_per_million_proj_all_ages": "excess_mortality_cumulative_per_million",
        },
    },
    {
        "short_name": "hospital",
        "indicators": [
            "daily_occupancy_hosp",
            "daily_occupancy_hosp_per_1m",
            "weekly_admissions_hosp",
            "weekly_admissions_hosp_per_1m",
            "daily_occupancy_icu",
            "daily_occupancy_icu_per_1m",
            "weekly_admissions_icu",
            "weekly_admissions_icu_per_1m",
        ],
        "column_rename": {
            "daily_occupancy_hosp": "hosp_patients",
            "daily_occupancy_hosp_per_1m": "hosp_patients_per_million",
            "weekly_admissions_hosp": "weekly_hosp_admissions",
            "weekly_admissions_hosp_per_1m": "weekly_hosp_admissions_per_million",
            "daily_occupancy_icu": "icu_patients",
            "daily_occupancy_icu_per_1m": "icu_patients_per_million",
            "weekly_admissions_icu": "weekly_icu_admissions",
            "weekly_admissions_icu_per_1m": "weekly_icu_admissions_per_million",
        },
    },
    {
        "short_name": "oxcgrt_policy",
        "indicators": [
            "stringency_index",
        ],
    },
    {
        "short_name": "tracking_r",
        "indicators": [
            "r",
        ],
        "column_rename": {
            "r": "reproduction_rate",
        },
        "column_dtypes": {
            "date": "datetime64[ns]",
        },
    },
    {
        "short_name": "testing",
        "indicators": [
            "total_tests",
            "new_tests",
            "total_tests_per_thousand",
            "new_tests_per_thousand",
            "new_tests_7day_smoothed",
            "new_tests_per_thousand_7day_smoothed",
        ],
        "column_rename": {
            "new_tests_7day_smoothed": "new_tests_smoothed",
            "new_tests_per_thousand_7day_smoothed": "new_tests_smoothed_per_thousand",
        },
        "column_dtypes": {
            "date": "datetime64[ns]",
        },
    },
    {
        "short_name": "combined",
        "indicators": [
            "short_term_positivity_rate",
            "short_term_tests_per_case",
        ],
        "column_rename": {
            "short_term_positivity_rate": "positive_rate",
            "short_term_tests_per_case": "tests_per_case",
        },
    },
    {
        "short_name": "vaccinations_global",
        "indicators": [
            "total_vaccinations",
            "people_vaccinated",
            "people_fully_vaccinated",
            "total_boosters",
            "daily_vaccinations",
            "daily_vaccinations_smoothed",
            "total_vaccinations_per_hundred",
            "people_vaccinated_per_hundred",
            "people_fully_vaccinated_per_hundred",
            "total_boosters_per_hundred",
            "daily_vaccinations_smoothed_per_million",
            "daily_people_vaccinated_smoothed",
            "daily_people_vaccinated_smoothed_per_hundred",
        ],
        "column_rename": {
            "daily_vaccinations": "new_vaccinations",
            "daily_vaccinations_smoothed": "new_vaccinations_smoothed",
            "daily_vaccinations_smoothed_per_million": "new_vaccinations_smoothed_per_million",
            "daily_people_vaccinated_smoothed": "new_people_vaccinated_smoothed",
            "daily_people_vaccinated_smoothed_per_hundred": "new_people_vaccinated_smoothed_per_hundred",
        },
    },
]
# Renamings of some columns
INDICATOR_RENAME = {}
for i in INDICATORS:
    if "column_rename" in i:
        INDICATOR_RENAME.update(i["column_rename"])
# Index shared in tables
COLUMNS_INDEX = [
    "country",
    "date",
]
# Non-covid indicators
COLUMNS_EXT = [
    "code",
    "continent",
    "population",
    "population_density",
    "median_age",
    "life_expectancy",
    "gdp_per_capita",
    "extreme_poverty",
    "diabetes_prevalence",
    "handwashing_facilities",
    "hospital_beds_per_thousand",
    "human_development_index",
    # Missing: aged_65_older, aged_70_older, female_smokers, male_smokers
]
# Output columns
COLUMNS_OUTPUT = [pp for p in INDICATORS for pp in p["indicators"]]
COLUMNS_OUTPUT = COLUMNS_INDEX + [INDICATOR_RENAME.get(col, col) for col in COLUMNS_OUTPUT] + COLUMNS_EXT

# Columns of tests (needed for testing units processing)
for i in INDICATORS:
    if i["short_name"] == "testing":
        COLUMNS_TESTING = i["indicators"]
        if "column_rename" in i:
            COLUMNS_TESTING = [i["column_rename"].get(col, col) for col in COLUMNS_TESTING]
        break


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load other datasets
    ds_pop = paths.load_dataset("population")
    ds_le = paths.load_dataset("life_expectancy")
    ds_wpp = paths.load_dataset("un_wpp")
    ds_regions = paths.load_dataset("regions")
    ds_wdi = paths.load_dataset("wdi")
    ds_hdr = paths.load_dataset("undp_hdr")
    ds_pip = paths.load_dataset("world_bank_pip")
    ds_who = paths.load_dataset("who")
    ds_ghe = paths.load_dataset("ghe")

    # Load meadow dataset.
    tbs = []
    for params in INDICATORS:
        ds = paths.load_dataset(short_name=params["short_name"])
        # Read table
        if "table_name" not in params:
            tb = ds[params["short_name"]].reset_index()
        else:
            tb = ds[params["table_name"]].reset_index()
        # Rename columns if applicable
        if "column_rename" in params:
            tb = tb.rename(columns=params["column_rename"])
            columns = [params["column_rename"].get(col, col) for col in params["indicators"]]
        else:
            columns = params["indicators"]
        # Set dtypes if applicable
        if "column_dtypes" in params:
            tb = tb.astype(params["column_dtypes"])

        # Keep relevant columns
        tb = tb[COLUMNS_INDEX + columns]

        # Append to list of tables
        tbs.append(tb)

    # Merge tables
    tb = pr.multi_merge(tbs, on=COLUMNS_INDEX, how="outer")

    # Add tests units
    tb = add_test_units(tb)

    # Demography indicators
    tb = add_demography_indicators(tb, ds_pop, ds_le, ds_wpp)

    # ISO, Continent
    tb = add_region_indicators(tb, ds_regions)

    # Econ / Health
    tb = add_external_indicators(tb, ds_wdi, ds_hdr, ds_pip, ds_who, ds_ghe)

    #
    # Process data.
    #
    # Sanity check
    tb = tb[COLUMNS_OUTPUT]

    # Format
    tb = tb.format(["country", "date"], short_name="compact")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        formats=["csv", "feather"],
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_test_units(tb: Table) -> Table:
    """Add column specifying units of testing values."""
    units = tb["total_tests"].m.display["entityAnnotationsMap"]
    units = units.split("\n")
    tb_units = pr.read_from_records(
        [
            {
                "country": u.split(":")[0].strip(),
                "tests_units": u.split(":")[1].strip(),
            }
            for u in units
        ]
    )
    tb_units.tests_units.metadata.title = "Testing units"
    tb_units.tests_units.metadata.description = "Units used by the location to report its testing data. A country file can't contain mixed units. All metrics concerning testing data use the specified test unit. Valid units are 'people tested' (number of people tested), 'tests performed' (number of tests performed. a single person can be tested more than once in a given day) and 'samples tested' (number of samples tested. In some cases, more than one sample may be required to perform a given test.)"

    # Merge
    tb = tb.merge(tb_units, on="country", how="left")

    # Remove unneeded units
    tb.loc[tb[COLUMNS_TESTING].isna().all(axis=1), "tests_units"] = pd.NA

    # Dtype
    tb["tests_units"] = tb["tests_units"].astype("string")
    return tb


def add_demography_indicators(tb: Table, ds_pop: Dataset, ds_le: Dataset, ds_wpp: Dataset) -> Table:
    # population, pop density

    tb_pop = ds_pop["population"].reset_index()
    tb_pop = tb_pop.loc[tb_pop["year"] == 2022, ["country", "population"]]

    tb_popd = ds_pop["population_density"].reset_index()
    tb_popd = tb_popd.loc[tb_popd["year"] == 2022, ["country", "population_density"]]

    # life exp
    tb_le = ds_le["life_expectancy"].reset_index()
    tb_le = tb_le.loc[
        (tb_le["year"] == 2021) & (tb_le["sex"] == "all") & (tb_le["age"] == 0), ["country", "life_expectancy_0"]
    ]
    tb_le = tb_le.rename(columns={"life_expectancy_0": "life_expectancy"})
    tb_le["country"] = tb_le["country"].replace({"Northern America": "North America"})

    # median age
    tb_age = ds_wpp["median_age"].reset_index()
    tb_age = tb_age.loc[
        (tb_age["year"] == 2022)
        & (tb_age["sex"] == "all")
        & (tb_age["age"] == "all")
        & (tb_age["variant"] == "estimates"),
        ["country", "median_age"],
    ]
    tb_age["country"] = tb_age["country"].str.replace(" (UN)", "")

    # merge
    tb = pr.multi_merge([tb, tb_pop, tb_popd, tb_le, tb_age], on="country", how="left")

    return tb


def add_region_indicators(tb: Table, ds_regions: Dataset) -> Table:
    tb_regions = ds_regions["regions"].reset_index()
    ## continent
    tb_cont = tb_regions.loc[tb_regions["region_type"] == "continent", ["name", "members"]]  # .explode("members")
    tb_cont["members"] = tb_cont["members"].astype("string").apply(lambda x: literal_eval(x))
    tb_cont = tb_cont.explode("members").rename(columns={"name": "continent", "members": "code"})
    ## code
    tb_regions = tb_regions[["code", "name"]].rename(columns={"name": "country"})
    ## internal merge
    tb_regions = tb_regions.merge(tb_cont, on="code", how="left")
    tb_regions = cast(Table, tb_regions)

    # merge
    tb = pr.multi_merge([tb, tb_regions], on="country", how="left")
    return tb


def add_external_indicators(
    tb: Table, ds_wdi: Dataset, ds_hdr: Dataset, ds_pip: Dataset, ds_who: Dataset, ds_ghe: Dataset
) -> Table:
    def _ffill_and_keep_latest(tb_: Table, cols):
        tb_[cols] = tb_.sort_values("year").groupby("country", observed=True)[cols].ffill()
        tb_ = tb_.drop_duplicates(subset=["country"], keep="last")
        tb_ = tb_.drop(columns=["year"])
        return tb_

    # WDI
    ## filter latest years
    tb_wdi = ds_wdi["wdi"].reset_index()
    tb_wdi = tb_wdi.loc[
        tb_wdi["year"] > 2010, ["country", "year", "ny_gdp_pcap_pp_kd", "sh_sta_diab_zs", "sh_med_beds_zs"]
    ]
    ## get most recent data
    cols = ["ny_gdp_pcap_pp_kd", "sh_sta_diab_zs", "sh_med_beds_zs"]
    tb_wdi = _ffill_and_keep_latest(tb_wdi, cols)
    ## rename
    tb_wdi = tb_wdi.rename(
        columns={
            "ny_gdp_pcap_pp_kd": "gdp_per_capita",
            "sh_sta_diab_zs": "diabetes_prevalence",
            "sh_med_beds_zs": "hospital_beds_per_thousand",
        }
    )

    # HDR
    tb_hdr = ds_hdr["undp_hdr"].reset_index()
    tb_hdr = tb_hdr.loc[tb_hdr["year"] == 2022, ["country", "hdi"]]
    tb_hdr = tb_hdr.rename(columns={"hdi": "human_development_index"})

    # PIP
    tb_pip = ds_pip["income_consumption_2017"].reset_index()
    tb_pip = tb_pip.loc[tb_pip["year"] > 2010, ["country", "year", "headcount_ratio_215"]]
    ## get most recent data
    cols = ["headcount_ratio_215"]
    tb_pip = _ffill_and_keep_latest(tb_pip, cols)
    ## rename cols
    tb_pip = tb_pip.rename(columns={"headcount_ratio_215": "extreme_poverty"})

    # WHO
    tb_who = ds_who["who"].reset_index()
    tb_who = tb_who.loc[(tb_who["year"] > 2010) & (tb_who["residence"] == "Total"), ["country", "year", "hyg_bas"]]
    ## get most recent data
    cols = ["hyg_bas"]
    tb_who = _ffill_and_keep_latest(tb_who, cols)
    ## rename cols
    tb_who = tb_who.rename(columns={"hyg_bas": "handwashing_facilities"})

    # GHE
    tb_ghe = ds_ghe["ghe"].reset_index()
    # death_rate100k__age_group_age_standardized__sex_both_sexes__cause_cardiovascular_diseases
    tb_ghe = tb_ghe.loc[
        (tb_ghe["year"] > 2010)
        & (tb_ghe["age_group"] == "Age-standardized")
        & (tb_ghe["sex"] == "Both sexes")
        & (tb_ghe["cause"] == "Cardiovascular diseases"),
        ["country", "year", "death_rate100k"],
    ]
    ## get most recent data
    cols = ["death_rate100k"]
    tb_ghe = _ffill_and_keep_latest(tb_ghe, cols)
    ## rename cols
    tb_ghe = tb_ghe.rename(columns={"death_rate100k": "cardiovasc_death_rate"})

    # merge
    tb = pr.multi_merge([tb, tb_wdi, tb_hdr, tb_pip, tb_who, tb_ghe], on="country", how="left")

    return tb
