"""
Meadow step: Aggregate ICD-10 codes into cause categories.

Loads the aggregated snapshot (with correct Format code handling) and:
1. Maps ICD-10 codes to cause categories
2. Aggregates deaths within each cause category
3. Recalculates percentages and rates
"""

import pandas as pd

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def map_icd10_to_category(icd_code: str) -> str:
    """
    Map ICD-10 code to cause category.

    Based on WHO cause category definitions from the manual dataset.
    """
    # Special case: AAA = All Causes
    if icd_code == "AAA":
        return "All Causes"

    if not icd_code or len(icd_code) < 1:
        return "Uncategorized"

    letter = icd_code[0].upper()

    # Extract numeric part for range checking
    try:
        if len(icd_code) >= 2:
            num = int(icd_code[1:3])
        else:
            num = 0
    except (ValueError, IndexError):
        num = 0

    # Detailed ICD-10 mapping based on WHO categories

    # A00-B99: Infectious and parasitic diseases (with exceptions)
    if letter in ["A", "B"]:
        return "Infectious and parasitic diseases"

    # C00-C97: Malignant neoplasms
    if letter == "C" and num <= 97:
        return "Malignant neoplasms"

    # D00-D48: Other neoplasms
    if letter == "D" and num <= 48:
        return "Other neoplasms"

    # D50-D64 (minus D64.9), D65-D89: Diabetes mellitus and endocrine disorders
    # D50-D53, D64.9: Nutritional deficiencies
    if letter == "D":
        if 50 <= num <= 53:
            return "Nutritional deficiencies"
        elif num == 64 and icd_code == "D649":
            return "Nutritional deficiencies"
        elif 55 <= num <= 89:
            return "Diabetes mellitus, blood and endocrine disorders"

    # E00-E02, E40-E64: Nutritional deficiencies (with exceptions)
    # E10-E14, E03-E07, E15-E16, E20-E34, E65-E88: Diabetes and endocrine
    if letter == "E":
        if num <= 2 or (40 <= num <= 46) or num == 50 or (51 <= num <= 64):
            return "Nutritional deficiencies"
        else:
            return "Diabetes mellitus, blood and endocrine disorders"

    # F01-F99: Neuropsychiatric conditions
    if letter == "F":
        return "Neuropsychiatric conditions"

    # G00-G04, G14: Infectious and parasitic diseases
    # G06-G98 (minus G14): Neuropsychiatric conditions
    if letter == "G":
        if num <= 4 or num == 14:
            return "Infectious and parasitic diseases"
        else:
            return "Neuropsychiatric conditions"

    # H00-H61, H68-H93: Sense organ diseases
    # H65-H66: Respiratory infections
    if letter == "H":
        if 65 <= num <= 66:
            return "Respiratory infections"
        else:
            return "Sense organ diseases"

    # I00-I99: Cardiovascular diseases
    if letter == "I":
        return "Cardiovascular diseases"

    # J00-J22, P23, U04, U07.1, U07.2, U09.9, U10.9: Respiratory infections
    # J30-J98: Respiratory diseases
    if letter == "J":
        if num <= 22:
            return "Respiratory infections"
        elif 30 <= num <= 98:
            return "Respiratory diseases"

    # K00-K14: Oral conditions
    # K20-K92: Digestive diseases
    if letter == "K":
        if num <= 14:
            return "Oral conditions"
        elif 20 <= num <= 92:
            return "Digestive diseases"

    # L00-L98: Skin diseases
    if letter == "L":
        return "Skin diseases"

    # M00-M99: Musculoskeletal diseases
    if letter == "M":
        return "Musculoskeletal diseases"

    # N00-N64, N75-N98: Genitourinary diseases
    # N70-N73: Infectious and parasitic diseases
    if letter == "N":
        if 70 <= num <= 73:
            return "Infectious and parasitic diseases"
        else:
            return "Genitourinary diseases"

    # O00-O99: Maternal conditions
    if letter == "O":
        return "Maternal conditions"

    # P00-P96 (minus P23, P37.3, P37.4): Perinatal conditions
    # P23, P37.3, P37.4: Special cases
    if letter == "P":
        if num == 23:
            return "Respiratory infections"
        elif icd_code in ["P373", "P374"]:
            return "Infectious and parasitic diseases"
        else:
            return "Perinatal conditions"

    # Q00-Q99: Congenital anomalies
    if letter == "Q":
        return "Congenital anomalies"

    # R95: Sudden infant death syndrome
    # R00-R94, R96-R99: Ill-defined diseases
    if letter == "R":
        if num == 95:
            return "Sudden infant death syndrome"
        else:
            return "Ill-defined diseases"

    # U07.0, X41, X42, X44, X45: Neuropsychiatric conditions
    # U04, U07.1, U07.2, U09.9, U10.9: Respiratory infections
    # U12.9: Unintentional injuries
    if letter == "U":
        if icd_code in ["U04", "U071", "U072", "U099", "U109"]:
            return "Respiratory infections"
        elif icd_code == "U129":
            return "Unintentional injuries"
        elif icd_code == "U070":
            return "Neuropsychiatric conditions"

    # V01-X59 (minus X41-X42, X44-X45): Unintentional injuries
    # X41, X42, X44, X45: Neuropsychiatric conditions
    # X60-Y09: Intentional injuries (X60-X84, Y87.0, Y87.1)
    if letter == "V":
        return "Unintentional injuries"

    if letter == "W":
        return "Unintentional injuries"

    if letter == "X":
        # X41, X42, X44, X45: Neuropsychiatric (drug-related)
        if icd_code in [
            "X41",
            "X410",
            "X411",
            "X412",
            "X413",
            "X414",
            "X415",
            "X416",
            "X417",
            "X418",
            "X419",
            "X42",
            "X420",
            "X421",
            "X422",
            "X423",
            "X424",
            "X425",
            "X426",
            "X427",
            "X428",
            "X429",
            "X44",
            "X440",
            "X441",
            "X442",
            "X443",
            "X444",
            "X445",
            "X446",
            "X447",
            "X448",
            "X449",
            "X45",
            "X450",
            "X451",
            "X452",
            "X453",
            "X454",
            "X455",
            "X456",
            "X457",
            "X458",
            "X459",
        ]:
            return "Neuropsychiatric conditions"
        # X60-X84: Intentional self-harm
        elif 60 <= num <= 84:
            return "Intentional injuries"
        # X01-X59: Unintentional
        else:
            return "Unintentional injuries"

    # Y10-Y34, Y87.2: Ill-defined injuries
    # Y35-Y36, Y87.0, Y87.1: Intentional injuries
    # Y40-Y89 (minus above): Unintentional injuries
    if letter == "Y":
        if 10 <= num <= 34 or icd_code == "Y872":
            return "Ill-defined injuries"
        elif (35 <= num <= 36) or icd_code in ["Y870", "Y871"]:
            return "Intentional injuries"
        elif 40 <= num <= 89:
            return "Unintentional injuries"
        elif num <= 9:
            return "Intentional injuries"

    return "Uncategorized"


def run() -> None:
    """Aggregate ICD-10 codes into cause categories."""

    # Load snapshot
    snap = paths.load_snapshot("mortality_database_aggregated.feather")
    tb = snap.read()

    # Remove UK constituent countries since snapshot already has "United Kingdom" (aggregated)
    # The snapshot contains both "United Kingdom" and its constituents, but we only want the aggregated version
    uk_countries = ["United Kingdom, England and Wales", "United Kingdom, Northern Ireland", "United Kingdom, Scotland"]
    tb = tb[~tb["country"].isin(uk_countries)]

    # Map ICD-10 codes to cause categories
    tb["cause_category"] = tb["cause"].apply(map_icd10_to_category)

    # IMPORTANT: AAA (All Causes) already includes all individual ICD codes
    # So when calculating "All Causes", we should ONLY use AAA, not sum with individual codes
    # For other cause categories, we sum individual ICD codes that belong to that category

    # Split into AAA and non-AAA
    tb_aaa = tb[tb["cause"] == "AAA"].copy()
    tb_other = tb[tb["cause"] != "AAA"].copy()

    # For AAA, it's already "All Causes" - no aggregation needed
    tb_aaa["cause_category"] = "All Causes"
    tb_agg_aaa = tb_aaa.groupby(
        ["country", "year", "sex", "age_group", "cause_category"],
        as_index=False,
        observed=True,
    ).agg(
        {
            "number": "sum",
            "population": "first",
        }
    )

    # For other ICD codes, aggregate by cause category
    # EXCLUDE "All Causes" category from non-AAA codes (they would double-count with AAA)
    tb_other = tb_other[tb_other["cause_category"] != "All Causes"]
    tb_agg_other = tb_other.groupby(
        ["country", "year", "sex", "age_group", "cause_category"],
        as_index=False,
        observed=True,
    ).agg(
        {
            "number": "sum",
            "population": "first",  # Population is same across causes
        }
    )

    # Combine AAA and other causes
    tb_agg = pd.concat([tb_agg_aaa, tb_agg_other], ignore_index=True)

    # Recalculate death rate per 100,000 population
    tb_agg["death_rate_per_100_000_population"] = (tb_agg["number"] / tb_agg["population"]) * 100000

    # Recalculate percentage (total deaths per age group)
    total_deaths = (
        tb_agg.groupby(["country", "year", "sex", "age_group"], as_index=False, observed=True)
        .agg({"number": "sum"})
        .rename(columns={"number": "total_deaths"})
    )

    tb_agg = tb_agg.merge(total_deaths, on=["country", "year", "sex", "age_group"])
    tb_agg["percentage_of_cause_specific_deaths_out_of_total_deaths"] = (
        tb_agg["number"] / tb_agg["total_deaths"]
    ) * 100

    # Calculate age-standardized death rates
    # TODO: Implement proper age-standardization using WHO standard population
    tb_agg["age_standardized_death_rate_per_100_000_standard_population"] = None

    # Add ICD codes column
    tb_agg["icd10_codes"] = ""

    # Rename cause_category to cause
    tb_agg = tb_agg.rename(columns={"cause_category": "cause"})

    # Calculate "Both sexes" from Males + Females
    # The raw WHO data often has incomplete "Both sexes" data
    # Following the manual dataset approach: Both sexes = Males + Females

    # Get Males and Females data
    males_females = tb_agg[tb_agg["sex"].isin(["Males", "Females"])].copy()

    # Group by everything except sex and sum
    both_sexes = males_females.groupby(["country", "year", "age_group", "cause"], as_index=False, observed=True).agg(
        {
            "number": "sum",
            "population": "sum",
        }
    )

    # Add sex column
    both_sexes["sex"] = "Both sexes"

    # Recalculate derived metrics
    both_sexes["death_rate_per_100_000_population"] = (both_sexes["number"] / both_sexes["population"]) * 100000

    # Recalculate percentages
    total_deaths_both = (
        both_sexes.groupby(["country", "year", "age_group"], as_index=False, observed=True)
        .agg({"number": "sum"})
        .rename(columns={"number": "total_deaths"})
    )

    both_sexes = both_sexes.merge(total_deaths_both, on=["country", "year", "age_group"])
    both_sexes["percentage_of_cause_specific_deaths_out_of_total_deaths"] = (
        both_sexes["number"] / both_sexes["total_deaths"]
    ) * 100

    both_sexes["age_standardized_death_rate_per_100_000_standard_population"] = None
    both_sexes["icd10_codes"] = ""

    # Remove original "Both sexes" data (which may be incomplete)
    tb_final = tb_agg[tb_agg["sex"] != "Both sexes"].copy()

    # Add calculated "Both sexes"
    from owid.catalog import Table

    both_sexes_table = Table(both_sexes)
    tb_final = pd.concat([tb_final, both_sexes_table], ignore_index=True)

    # Add "all ages" row (sum across all age groups)
    # This matches the manual dataset structure
    all_ages = tb_final.groupby(["country", "year", "sex", "cause"], as_index=False, observed=True).agg(
        {
            "number": "sum",
            "population": "sum",
        }
    )

    all_ages["age_group"] = "all ages"
    all_ages["death_rate_per_100_000_population"] = (all_ages["number"] / all_ages["population"]) * 100000
    all_ages["percentage_of_cause_specific_deaths_out_of_total_deaths"] = 100.0  # All ages = 100%
    all_ages["age_standardized_death_rate_per_100_000_standard_population"] = None
    all_ages["icd10_codes"] = ""

    # Combine all data
    all_ages_table = Table(all_ages)
    tb_final = pd.concat([tb_final, all_ages_table], ignore_index=True)

    # Convert back to Table to preserve metadata
    tb_final = Table(tb_final, short_name=paths.short_name)

    # Add origins to all indicator columns
    for col in [
        "icd10_codes",
        "number",
        "percentage_of_cause_specific_deaths_out_of_total_deaths",
        "age_standardized_death_rate_per_100_000_standard_population",
        "death_rate_per_100_000_population",
    ]:
        tb_final[col].metadata.origins = [snap.metadata.origin]

    # Select final columns
    tb_final = tb_final[
        [
            "country",
            "year",
            "sex",
            "age_group",
            "cause",
            "icd10_codes",
            "number",
            "percentage_of_cause_specific_deaths_out_of_total_deaths",
            "age_standardized_death_rate_per_100_000_standard_population",
            "death_rate_per_100_000_population",
        ]
    ]

    # Format and save
    tb_final = tb_final.format(["country", "year", "sex", "age_group", "cause"])

    ds_meadow = paths.create_dataset(tables=[tb_final], default_metadata=snap.metadata)
    ds_meadow.save()
