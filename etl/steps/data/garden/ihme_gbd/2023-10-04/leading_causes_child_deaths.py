from typing import Any, List

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# naming conventions
paths = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    # Load in the cause of death data and hierarchy of causes data
    ds_cause = paths.load_dataset("gbd_cause")
    ds_hierarchy = paths.load_dataset("cause_hierarchy")
    tb_hierarchy = ds_hierarchy["cause_hierarchy"].reset_index()

    # Underscore the hierarchy cause names to match tb_cause
    tb_hierarchy["cause_name_underscore"] = tb_hierarchy["cause_name"].apply(underscore)
    tb_hierarchy = add_owid_hierarchy(tb_hierarchy)
    # We'll iterate through each level of the hierarchy to find the leading cause of death in under-fives in each country-year
    levels = [1, 2, 3, 4, "owid_all_ages", "owid_under_5"]
    age_groups = ["under_5", "all_ages"]

    tb_out = []
    for level in levels:
        log.info(f"Processing level {level}")
        for age_group in age_groups:
            log.info(f"Processing age group {age_group}")
            # Get the causes at this level
            level_causes = tb_hierarchy[tb_hierarchy["level"] == level]["cause_name_underscore"].to_list()
            # Create table with leading cause of death at this level for each country-year
            tb_level = create_hierarchy_table(
                age_group=age_group,
                ds_cause=ds_cause,
                level_causes=level_causes,
                short_name=f"leading_cause_level_{level}_in_{age_group}",
            )
            # Make the disease names more readable
            tb_level = clean_disease_names(tb=tb_level, tb_hierarchy=tb_hierarchy, level=level)
            tb_level = tb_level.set_index(["country", "year"], verify_integrity=True)
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


def create_hierarchy_table(age_group: str, ds_cause: Dataset, level_causes: List[str], short_name: str) -> Table:
    """
    For each level_cause find the relevent table in ds_cause and create a table with the leading cause of death in each country-year

    """
    tb_out = []
    for cause in level_causes:
        cause_table_name = cause + f"__both_sexes__{age_group}"
        tb = ds_cause[cause_table_name].reset_index()
        assert tb.shape[0] > 0, f"Table {cause_table_name} is empty"
        death_col = f"deaths_that_are_from_{cause}__in_both_sexes_aged_{age_group}"
        if death_col in tb.columns:
            cols = ["country", "year", death_col]
            tb = tb[cols]
            tb = tb.rename(columns={death_col: cause}, errors="raise")
            tb_out.append(tb)

    tb_out = pr.concat(tb_out, ignore_index=True)
    # Melt the table from wide to long to make it easier to groupby
    long_tb = pr.melt(
        tb_out, id_vars=["country", "year"], var_name=f"disease_{short_name}", value_name=f"deaths_{short_name}"
    )
    long_tb = long_tb.dropna(how="any")
    # Find the cause of death with the highest number of deaths in each country-year
    long_tb[f"disease_{short_name}"] = long_tb[f"disease_{short_name}"].astype(str)
    leading_causes_idx = long_tb.groupby(["country", "year"], observed=True)[f"deaths_{short_name}"].idxmax()
    leading_causes_tb = long_tb.loc[leading_causes_idx]
    leading_causes_tb = leading_causes_tb.drop(columns=[f"deaths_{short_name}"])
    leading_causes_tb.metadata.short_name = short_name

    return leading_causes_tb


def clean_disease_names(tb: Table, tb_hierarchy: Table, level: Any) -> Table:
    """
    Making the underscored disease names more readable using the original hierarchy table

    """

    tb_hierarchy = tb_hierarchy[tb_hierarchy["level"] == level][["cause_name", "cause_name_underscore"]]
    disease_col = [item for item in tb.columns if "disease_" in item]
    disease_col = disease_col[0]
    tb = tb.merge(tb_hierarchy, how="inner", left_on=disease_col, right_on="cause_name_underscore")
    tb = tb.drop(columns=["cause_name_underscore", disease_col])
    tb = tb.rename(columns={"cause_name": disease_col})

    # Add more succinct disease names

    disease_dict = {
        "Neonatal encephalopathy due to birth asphyxia and trauma": "Asphyxia and trauma",
        "Neonatal preterm birth": "Preterm birth",
        "Exposure to forces of nature": "Natural disasters",
        "Neoplasms": "Cancer",
    }

    tb[disease_col] = tb[disease_col].replace(disease_dict)

    return tb


def add_owid_hierarchy(tb_hierarchy: Table) -> Table:
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
        "Road injury",
        "Fire, heat, and hot substances",
        "Acute hepatitis",
    ]

    under_five = [
        "Lower respiratory infections",
        "Invasive Non-typhoidal Salmonella (iNTS)",
        "Interpersonal violence",
        "Nutritional deficiencies",
        "Acute hepatitis",
        "Neoplasms",
        "Measles",
        "Digestive diseases",
        "Cirrhosis and other chronic liver diseases",
        "Chronic kidney disease",
        "Cardiovascular diseases",
        "Congenital birth defects",
        "Neonatal preterm birth",
        "Environmental heat and cold exposure",
        "Neonatal sepsis and other neonatal infections",
        "Exposure to forces of nature",
        "Diabetes mellitus",
        "Neonatal encephalopathy due to birth asphyxia and trauma",
        "Meningitis",
        "Other neonatal disorders",
        "Whooping cough",
        "Diarrheal diseases",
        "Fire, heat, and hot substances",
        "Road injuries",
        "Tuberculosis",
        "HIV/AIDS",
        "Drowning",
        "Malaria" "Syphilis",
    ]
    msk_all_ages = tb_hierarchy["cause_name"].isin(all_ages)
    tb_hierarchy_all_ages = tb_hierarchy[msk_all_ages]
    tb_hierarchy_all_ages = tb_hierarchy_all_ages.copy()
    tb_hierarchy_all_ages["level"] = "owid_all_ages"

    msk_under_five = tb_hierarchy["cause_name"].isin(under_five)
    tb_hierarchy_under_five = tb_hierarchy[msk_under_five]
    tb_hierarchy_under_five = tb_hierarchy_under_five.copy()
    tb_hierarchy_under_five["level"] = "owid_under_5"

    tb_hierarchy = pr.concat(
        [tb_hierarchy_all_ages, tb_hierarchy_under_five, tb_hierarchy], ignore_index=True
    ).drop_duplicates()

    return tb_hierarchy
