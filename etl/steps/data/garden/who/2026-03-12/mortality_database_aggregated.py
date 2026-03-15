"""
Garden step for WHO Mortality Database aggregated data.

This step:
1. Loads meadow dataset (with ICD-10 codes aggregated into cause categories)
2. Harmonizes country names
3. Saves as garden dataset for comparison with manual mortality_database
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def map_sex(sex_code):
    """
    Map WHO sex codes to labels.
    """
    sex_code = str(sex_code).strip()

    if sex_code == "1":
        return "Males"
    elif sex_code == "2":
        return "Females"
    elif sex_code == "9":
        return "Sex unspecified"
    else:
        return "Unknown"


def tidy_age_dimension(tb: Table) -> Table:
    age_dict = {
        "0": "Under 1 year",
        "1": "1 year",
        "2": "2 years",
        "3": "3 years",
        "4": "4 years",
        "1-4": "1-4 years",
        "5-9": "5-9 years",
        "10-14": "10-14 years",
        "15-19": "15-19 years",
        "20-24": "20-24 years",
        "25-29": "25-29 years",
        "30-34": "30-34 years",
        "35-39": "35-39 years",
        "40-44": "40-44 years",
        "45-49": "45-49 years",
        "50-54": "50-54 years",
        "55-59": "55-59 years",
        "60-64": "60-64 years",
        "65-69": "65-69 years",
        "70-74": "70-74 years",
        "75-79": "75-79 years",
        "80-84": "80-84 years",
        "85-89": "85-89 years",
        "90-94": "90-94 years",
        "75 &+": "75+ years",
        "85 &+": "85+ years",
        "95 &+": "95+ years",
        "15-24": "15-24 years",
        "25-34": "25-34 years",
        "35-44": "35-44 years",
        "All ages": "All ages",
        "Unknown": "Unknown age",
    }

    tb["age_group"] = tb["age_group"].astype(str).str.strip().replace(age_dict)

    print(tb["age_group"].unique())
    return tb


def run() -> None:
    """
    Load meadow dataset, harmonize countries, and save as garden dataset.
    """
    # Load meadow dataset
    ds_meadow = paths.load_dataset("mortality_database_aggregated")
    tb = ds_meadow["mortality_database_aggregated"].reset_index()

    # Harmonize country names using the same mapping as mortality_database
    tb = paths.regions.harmonize_names(tb)
    tb["sex"] = tb["sex"].apply(map_sex)
    # Keep original sex-specific rows
    tb_sex = tb.copy()

    # Create Both sexes by summing male + female only
    group_cols = ["country", "year", "age_group", "cause_category"]

    tb_both = (
        tb.loc[tb["sex"].isin(["Males", "Females"])]
        .groupby(group_cols, as_index=False)
        .agg(
            {
                "deaths": "sum",
                # keep population only if you want it, and only if male/female populations
                # should be added together:
                "population": "sum",
            }
        )
    )

    tb_both["sex"] = "Both sexes"

    # Combine back
    tb = pr.concat([tb_sex, tb_both], ignore_index=True)

    # Optional: drop population if you do not need it
    tb = tb.drop(columns=["population"])
    tb = tidy_age_dimension(tb)

    # Format
    tb = tb.format(["country", "year", "sex", "age_group", "cause_category"])
    # Create garden dataset
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, repack=False)

    # Save
    ds_garden.save()
