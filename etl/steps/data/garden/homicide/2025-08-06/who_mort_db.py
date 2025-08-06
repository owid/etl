import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.data_helpers import geo
from etl.data_helpers.population import add_population
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
paths = PathFinder(__file__)

AGE_GROUPS_RANGES = {
    "Age00": [0, 0],
    "Age01_04": [1, 4],
    "Age05_09": [5, 9],
    "Age10_14": [10, 14],
    "Age15_19": [15, 19],
    "Age20_24": [20, 24],
    "Age25_29": [25, 29],
    "Age30_34": [30, 34],
    "Age35_39": [35, 39],
    "Age40_44": [40, 44],
    "Age45_49": [45, 49],
    "Age50_54": [50, 54],
    "Age55_59": [55, 59],
    "Age60_64": [60, 64],
    "Age65_69": [65, 69],
    "Age70_74": [70, 74],
    "Age75_79": [75, 79],
    "Age80_84": [80, 84],
    "Age85_over": [85, None],
    "All ages": [0, None],
}


def run() -> None:
    log.info("who_mort_db.start")

    # read dataset from meadow
    ds_meadow = paths.load_dataset("who_mort_db")
    tb = ds_meadow["who_mort_db"].reset_index()

    log.info("who_mort_db.harmonize_countries")
    tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)
    tb = clean_up_dimensions(tb)
    tb = add_age_groups(tb)

    tb = tb.format(["country", "year", "age_group", "sex"])
    ds_garden = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("who_mort_db.end")


def clean_up_dimensions(tb: Table) -> Table:
    sex_dict = {"All": "Both Sexes", "Male": "Males", "Female": "Females", "Unknown": "Unknown sex"}
    age_dict = {"Age_all": "All ages", "Age_unknown": "Unknown age"}
    tb = tb.astype({"sex": str, "age_group": str}).replace({"sex": sex_dict, "age_group": age_dict})

    return tb


def add_age_groups(tb: Table) -> Table:
    """
    Create custom age-group aggregations, there are a range of different ones that might be useful for this dataset.
    For example, we may want to compare to other causes of death imported via the IHME dataset, so we should include age-groups that match our chosen IHME groups.

    """
    age_groups_ihme = {
        "Age00": "Age 0-4",
        "Age01_04": "Age 0-4",
        "Age05_09": "Age 5-14",
        "Age10_14": "Age 5-14",
        "Age15_19": "Age 15-49",
        "Age20_24": "Age 15-49",
        "Age25_29": "Age 15-49",
        "Age30_34": "Age 15-49",
        "Age35_39": "Age 15-49",
        "Age40_44": "Age 15-49",
        "Age45_49": "Age 15-49",
        "Age50_54": "Age 50-69",
        "Age55_59": "Age 50-69",
        "Age60_64": "Age 50-69",
        "Age65_69": "Age 50-69",
        "Age70_74": "Age 70+",
        "Age75_79": "Age 70+",
        "Age80_84": "Age 70+",
        "Age85_over": "Age 70+",
    }

    age_groups_decadal = {
        "Age00": "Age 0-4",
        "Age01_04": "Age 0-4",
        "Age05_09": "Age 5-14",
        "Age10_14": "Age 5-14",
        "Age15_19": "Age 15-24",
        "Age20_24": "Age 15-24",
        "Age25_29": "Age 25-34",
        "Age30_34": "Age 25-34",
        "Age35_39": "Age 35-44",
        "Age40_44": "Age 35-44",
        "Age45_49": "Age 45-54",
        "Age50_54": "Age 45-54",
        "Age55_59": "Age 55-64",
        "Age60_64": "Age 55-64",
        "Age65_69": "Age 65-74",
        "Age70_74": "Age 65-74",
        "Age75_79": "Age 75-84",
        "Age80_84": "Age 75-84",
        "Age85_over": "Age 85+",
    }

    age_groups_child = {
        "Age00": "Age 0-19",
        "Age01_04": "Age 0-19",
        "Age05_09": "Age 0-19",
        "Age10_14": "Age 0-19",
        "Age15_19": "Age 0-19",
    }

    tb_age_group_ihme = build_custom_age_groups(tb, age_groups=age_groups_ihme)
    tb_age_group_decadal = build_custom_age_groups(tb, age_groups=age_groups_decadal)
    tb_age_group_child = build_custom_age_groups(tb, age_groups=age_groups_child)
    tb_orig = remove_granular_age_groups(tb)
    tb_combined = pr.concat([tb_orig, tb_age_group_ihme, tb_age_group_decadal, tb_age_group_child], axis=0)
    tb_combined = tb_combined.drop_duplicates()
    return tb_combined


def build_custom_age_groups(tb: Table, age_groups: dict) -> Table:
    """
    Estimate metrics for broader age groups. In line with the age-groups we define in the age_groups dict:
    """
    tb_age = tb.copy()
    # Add population values for each dimension
    log.info("who_mort_db.add_population_values")

    tb_age = tb_age[tb_age["age_group"].isin(age_groups.keys())]

    total_deaths = tb_age["number"].sum()

    tb_age = add_population(
        df=tb_age,
        country_col="country",
        year_col="year",
        sex_col="sex",
        sex_group_all="Both Sexes",
        sex_group_female="Females",
        sex_group_male="Males",
        age_col="age_group",
        age_group_mapping=AGE_GROUPS_RANGES,
    )
    # Map age groups to broader age groups. Missing age groups in the list are passed as they are (no need to assign to broad group)
    log.info("who_mort_db.create_broader_age_groups")

    tb_age["age_group"] = tb_age["age_group"].map(age_groups)

    # Sum
    tb_age = tb_age.drop(
        [
            "percentage_of_cause_specific_deaths_out_of_total_deaths",
            "age_standardized_death_rate_per_100_000_standard_population",
            "death_rate_per_100_000_population",
        ],
        axis=1,
    )
    tb_age = tb_age.groupby(["country", "year", "sex", "age_group"], observed=True).sum()
    tb_age["death_rate_per_100_000_population"] = (tb_age["number"] / tb_age["population"].replace(0, pd.NA)) * 100000
    tb_age = tb_age.drop(columns=["population"]).reset_index()

    assert total_deaths == tb_age["number"].sum()

    return tb_age


def remove_granular_age_groups(tb: Table) -> Table:
    """
    Remove the small five-year age-groups that are in the original dataset
    """
    age_groups_to_keep = ["All ages", "Unknown age"]
    tb = tb[tb["age_group"].isin(age_groups_to_keep)]
    assert len(tb["age_group"].drop_duplicates()) == len(age_groups_to_keep)
    return tb
