"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

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
# Output columns
COLUMNS_OUTPUT = [pp for p in INDICATORS for pp in p["indicators"]]
COLUMNS_OUTPUT = COLUMNS_INDEX + [INDICATOR_RENAME.get(col, col) for col in COLUMNS_OUTPUT]

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

    # Extra indicators

    # population, pop density, life exp, median age, 
    ds_wpp = paths.load_dataset("un_wpp")
    # ISO, Continent
    ds_regions = paths.load_dataset("regions")



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
