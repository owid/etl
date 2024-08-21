"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# What to load
INDICATORS = [
    {
        "dataset": "cases_deaths",
        "table": "cases_deaths",
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
    },
    {
        "dataset": "hospital",
        "table": "hospital",
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
    },
]
# Renamings of some columns
INDICATOR_RENAME = {
    # Cases
    "new_cases_7_day_avg_right": "new_cases_smoothed",
    "new_cases_per_million_7_day_avg_right": "new_cases_smoothed_per_million",
    # Deaths
    "new_deaths_7_day_avg_right": "new_deaths_smoothed",
    "new_deaths_per_million_7_day_avg_right": "new_deaths_smoothed_per_million",
    # Hospital
    "daily_occupancy_hosp": "hosp_patients",
    "daily_occupancy_hosp_per_1m": "hosp_patients_per_million",
    "weekly_admissions_hosp": "weekly_hosp_admissions",
    "weekly_admissions_hosp_per_1m": "weekly_hosp_admissions_per_million",
    "daily_occupancy_icu": "icu_patients",
    "daily_occupancy_icu_per_1m": "icu_patients_per_million",
    "weekly_admissions_icu": "weekly_icu_admissions",
    "weekly_admissions_icu_per_1m": "weekly_icu_admissions_per_million",
}
# Index shared in tables
COLUMNS_INDEX = [
    "country",
    "date",
]
# Output columns
COLUMNS_OUTPUT = [pp for p in INDICATORS for pp in p["indicators"]]
COLUMNS_OUTPUT = COLUMNS_INDEX + [INDICATOR_RENAME.get(col, col) for col in COLUMNS_OUTPUT]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    tbs = []
    for params in INDICATORS:
        tb = paths.load_dataset(short_name=params["dataset"])[params["table"]].reset_index()
        # tb = tb[params["indicators"]]
        tbs.append(tb)

    # Merge tables
    tb = pr.multi_merge(tbs, on=COLUMNS_INDEX, how="outer")

    # Rename
    tb = tb.rename(columns=INDICATOR_RENAME)

    #
    # Process data.
    #
    tb = tb[COLUMNS_OUTPUT]
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
