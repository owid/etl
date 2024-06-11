from typing import List

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.catalog.utils import underscore

from etl.helpers import PathFinder, create_dataset

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load in the cause of death data and hierarchy of causes data
    ds_cause = paths.load_dataset("gbd_cause")
    tb_cause = ds_cause["gbd_cause_deaths"].reset_index()
    ds_hierarchy = paths.load_dataset("cause_hierarchy")
    tb_hierarchy = ds_hierarchy["cause_hierarchy"].reset_index()

    # Underscore the hierarchy cause names to match tb_cause
    tb_hierarchy["cause_name_underscore"] = tb_hierarchy["cause_name"].apply(underscore)
    # Dropping out the rows which are only in the yld_only column (years lost to disease) - typically diseases that don't cause deaths e.g. tooth caries
    tb_hierarchy = tb_hierarchy[tb_hierarchy["yld_only"].isna()]
    tb_cause["cause_name_underscore"] = tb_cause["cause"].apply(underscore)
    tb_cause = tb_cause[tb_cause["metric"] == "Number"]
    tb_hierarchy = add_owid_hierarchy(tb_hierarchy, owid_level_name="owid")
    # We'll iterate through each level of the hierarchy to find the leading cause of death in under-fives in each country-year
    levels = [1, 2, 3, 4, "owid"]
    age_groups = ["All ages"]

    tb_out = []
    for level in levels:
        paths.log.info(f"Processing level {level}")
        for age_group in age_groups:
            paths.log.info(f"Processing age group {age_group}")
            # Get the causes at this level
            level_causes = tb_hierarchy[tb_hierarchy["level"] == level]["cause_name"].to_list()
            # Create table with leading cause of death at this level for each country-year
            tb_level = create_hierarchy_table(
                age_group=age_group,
                tb_cause=tb_cause,
                level_causes=level_causes,
                level=level,
            )
            tb_level = tb_level.format(
                ["country", "year"], short_name=f"leading_cause_level_{level}_in_{age_group.lower().replace(' ', '_')}"
            )
            tb_out.append(tb_level)

    # Removing the tables where the age group doesn't match the hierarchy
    tb_out = [
        tb
        for tb in tb_out
        if tb.metadata.short_name
        not in [
            "leading_cause_level_owid_under_5_in_all_ages",
            "leading_cause_level_owid_all_ages_in_under_5",
        ]
    ]

    # Save outputs.

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tb_out,
        check_variables_metadata=True,
        default_metadata=ds_cause.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def create_hierarchy_table(age_group: str, tb_cause: Table, level_causes: List[str], level: str) -> Table:
    """
    For each level_cause find the relevant table in ds_cause and create a table with the leading cause of death in each country-year

    """
    tb_out = []
    for cause in level_causes:
        tb = tb_cause[(tb_cause["cause"] == cause) & (tb_cause["age"] == age_group)]
        assert tb.shape[0] > 0, f"Table {cause} is empty"
        cols = ["country", "year", "cause", "value"]
        tb = tb[cols]
        # tb = tb.rename(columns={"value": cause}, errors="raise")
        tb_out.append(tb)

    tb_out = pr.concat(tb_out, ignore_index=True, sort=False, verify_integrity=True)
    leading_causes_idx = tb_out.groupby(["country", "year"], observed=True)["value"].idxmax()
    leading_causes_tb = tb_out.loc[leading_causes_idx]
    leading_causes_tb = leading_causes_tb.drop(columns=["value"])
    leading_causes_tb = leading_causes_tb.rename(columns={"cause": f"leading_deaths_level_{level}"})

    return leading_causes_tb


def add_owid_hierarchy(tb_hierarchy: Table, owid_level_name: str) -> Table:
    """
    At OWID we use a mixture of level 2 and 3 GBD causes of death, to limit the number of causes shown on a chart.
    These are the causes of death we show in our causes of death charts e.g. https://ourworldindata.org/grapher/causes-of-death-in-children-under-5
    """

    all_ages = [
        "Cardiovascular diseases",
        "Neoplasms",
        "Chronic respiratory diseases",
        "Digestive diseases",
        "Lower respiratory infections",
        "Neonatal disorders",
        "Alzheimer's disease and other dementias",
        "Diabetes mellitus",
        "Diarrheal diseases",
        "Cirrhosis and other chronic liver diseases",
        "Meningitis",
        "Parkinson's disease",
        "Nutritional deficiencies",
        "Malaria",
        "Drowning",
        "Interpersonal violence",
        "Maternal disorders",
        "HIV/AIDS",
        "Drug use disorders",
        "Tuberculosis",
        "Alcohol use disorders",
        "Self-harm",
        "Exposure to forces of nature",
        "Environmental heat and cold exposure",
        "Conflict and terrorism",
        "Chronic kidney disease",
        "Poisonings",
        "Protein-energy malnutrition",
        "Road injuries",
        "Fire, heat, and hot substances",
        "Acute hepatitis",
        "COVID-19",
    ]
    missing_items = [item for item in all_ages if item not in tb_hierarchy["cause_name"].values]
    assert len(missing_items) == 0, f"{missing_items} not in list, check spelling"
    msk_all_ages = tb_hierarchy["cause_name"].isin(all_ages)
    tb_hierarchy_all_ages = tb_hierarchy[msk_all_ages]
    tb_hierarchy_all_ages = tb_hierarchy_all_ages.copy()
    tb_hierarchy_all_ages["level"] = owid_level_name

    tb_hierarchy = pr.concat([tb_hierarchy_all_ages, tb_hierarchy], ignore_index=True).drop_duplicates()

    return tb_hierarchy
