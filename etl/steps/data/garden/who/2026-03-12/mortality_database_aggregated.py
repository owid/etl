"""
Garden step for WHO Mortality Database aggregated data.

This step:
1. Loads meadow dataset (with ICD-10 codes aggregated into cause categories)
2. Harmonizes country names
3. Saves as garden dataset for comparison with manual mortality_database
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def map_sex(sex_code):
    """
    Map WHO sex codes to labels.
    """
    sex_code = str(sex_code).strip()

    if sex_code == "1":
        return "Male"
    elif sex_code == "2":
        return "Female"
    elif sex_code == "9":
        return "Sex unspecified"
    else:
        return "Unknown"


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

    # Format
    tb = tb.format(["country", "year", "sex", "age_group", "cause_category"])
    tb = tb.drop(columns=["population"])
    # Create garden dataset
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, repack=False)

    # Save
    ds_garden.save()
