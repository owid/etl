"""Load WHO mortality database snapshot and create meadow dataset.

This step:
1. Loads the snapshot with country, year, sex, cause (ICD-10 codes), age_group, deaths, population
2. Maps ICD-10 codes to cause categories
3. Drops uncategorized causes
4. Cleans and formats the data
"""

import re

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def normalize_icd_code(icd_code: str) -> str:
    if icd_code is None:
        return ""
    # uppercase and keep only letters/numbers
    return re.sub(r"[^A-Z0-9]", "", str(icd_code).upper().strip())


# These are probably WHO mortality list / reference-table codes, not ICD-10 proper.
WHO_NUMERIC_CATEGORY_MAP = {
    # All causes
    "1000": "All Causes",
    # Infectious and parasitic diseases
    "1001": "Infectious and parasitic diseases",
    "1002": "Infectious and parasitic diseases",
    "1003": "Infectious and parasitic diseases",
    "1004": "Infectious and parasitic diseases",
    "1005": "Infectious and parasitic diseases",
    "1006": "Infectious and parasitic diseases",
    "1007": "Infectious and parasitic diseases",
    "1008": "Infectious and parasitic diseases",
    "1009": "Infectious and parasitic diseases",
    "1010": "Infectious and parasitic diseases",
    "1011": "Infectious and parasitic diseases",
    "1012": "Infectious and parasitic diseases",
    "1013": "Infectious and parasitic diseases",
    "1014": "Infectious and parasitic diseases",
    "1015": "Infectious and parasitic diseases",
    "1016": "Infectious and parasitic diseases",
    "1017": "Infectious and parasitic diseases",
    "1018": "Infectious and parasitic diseases",
    "1019": "Infectious and parasitic diseases",
    "1020": "Infectious and parasitic diseases",
    "1021": "Infectious and parasitic diseases",
    "1022": "Infectious and parasitic diseases",
    "1023": "Infectious and parasitic diseases",
    "1024": "Infectious and parasitic diseases",
    "1025": "Infectious and parasitic diseases",
    "1026": "Infectious and parasitic diseases",
    # Neoplasms
    "1027": "Malignant neoplasms",
    "1028": "Malignant neoplasms",
    "1029": "Malignant neoplasms",
    "1030": "Malignant neoplasms",
    "1031": "Malignant neoplasms",
    "1032": "Malignant neoplasms",
    "1033": "Malignant neoplasms",
    "1034": "Malignant neoplasms",
    "1035": "Malignant neoplasms",
    "1036": "Malignant neoplasms",
    "1037": "Malignant neoplasms",
    "1038": "Malignant neoplasms",
    "1039": "Malignant neoplasms",
    "1040": "Malignant neoplasms",
    "1041": "Malignant neoplasms",
    "1042": "Malignant neoplasms",
    "1043": "Malignant neoplasms",
    "1044": "Malignant neoplasms",
    "1045": "Malignant neoplasms",
    "1046": "Malignant neoplasms",
    # Other neoplasms
    "1047": "Other neoplasms",
    # Diabetes mellitus, blood and endocrine disorders
    "1048": "Diabetes mellitus, blood and endocrine disorders",
    "1049": "Diabetes mellitus, blood and endocrine disorders",
    "1050": "Diabetes mellitus, blood and endocrine disorders",
    "1051": "Diabetes mellitus, blood and endocrine disorders",
    "1052": "Diabetes mellitus, blood and endocrine disorders",
    # Neuropsychiatric conditions
    "1053": "Neuropsychiatric conditions",
    "1054": "Neuropsychiatric conditions",
    "1055": "Neuropsychiatric conditions",
    "1056": "Neuropsychiatric conditions",
    "1057": "Neuropsychiatric conditions",
    "1058": "Neuropsychiatric conditions",
    "1059": "Neuropsychiatric conditions",
    # Sense organ diseases
    "1060": "Sense organ diseases",
    "1061": "Sense organ diseases",
    # Cardiovascular diseases
    "1062": "Cardiovascular diseases",
    "1063": "Cardiovascular diseases",
    "1064": "Cardiovascular diseases",
    "1065": "Cardiovascular diseases",
    "1066": "Cardiovascular diseases",
    "1067": "Cardiovascular diseases",
    "1068": "Cardiovascular diseases",
    "1069": "Cardiovascular diseases",
    # Respiratory diseases / infections
    "1070": "Respiratory diseases",
    "1071": "Respiratory infections",
    "1072": "Respiratory infections",
    "1073": "Respiratory infections",
    "1074": "Respiratory diseases",
    "1075": "Respiratory diseases",
    # Digestive diseases
    "1076": "Digestive diseases",
    "1077": "Digestive diseases",
    "1078": "Digestive diseases",
    "1079": "Digestive diseases",
    # Skin diseases
    "1080": "Skin diseases",
    # Musculoskeletal diseases
    "1081": "Musculoskeletal diseases",
    # Genitourinary diseases
    "1082": "Genitourinary diseases",
    "1083": "Genitourinary diseases",
    "1084": "Genitourinary diseases",
    "1085": "Genitourinary diseases",
    "1086": "Genitourinary diseases",
    # Maternal conditions
    "1087": "Maternal conditions",
    "1088": "Maternal conditions",
    "1089": "Maternal conditions",
    "1090": "Maternal conditions",
    "1091": "Maternal conditions",
    # Perinatal conditions
    "1092": "Perinatal conditions",
    # Congenital anomalies
    "1093": "Congenital anomalies",
    # Ill-defined diseases
    "1094": "Ill-defined diseases",
    # Injuries
    "1095": "Injuries",
    "1096": "Unintentional injuries",
    "1097": "Unintentional injuries",
    "1098": "Unintentional injuries",
    "1099": "Unintentional injuries",
    "1100": "Unintentional injuries",
    "1101": "Intentional injuries",
    "1102": "Intentional injuries",
    "1103": "Unintentional injuries",
    # Special U codes
    "1901": "Respiratory infections",  # U049 SARS unspecified
    "1902": "Neuropsychiatric conditions",  # U070 Vaping-related disorder
    "1903": "Respiratory infections",  # U071-U072 COVID-19
}


def map_icd10_to_category(icd_code: str) -> str:
    code = normalize_icd_code(icd_code)

    if code == "AAA":
        return "All Causes"

    if not code:
        return "Uncategorized"

    # Separate handling for WHO numeric list/reference codes
    if code.isdigit():
        return WHO_NUMERIC_CATEGORY_MAP.get(code, "Uncategorized")

    letter = code[0]

    # first 2 digits after the letter, if present
    m = re.match(r"^[A-Z](\d{2})", code)
    num = int(m.group(1)) if m else None

    # A00-B99
    if letter in {"A", "B"}:
        return "Infectious and parasitic diseases"

    # C00-C97
    if letter == "C" and num is not None and num <= 97:
        return "Malignant neoplasms"

    # D00-D48 / D50-D53 / D64.9 / D55-D89
    if letter == "D" and num is not None:
        if num <= 48:
            return "Other neoplasms"
        if 50 <= num <= 53:
            return "Nutritional deficiencies"
        if code == "D649":
            return "Nutritional deficiencies"
        if 55 <= num <= 89 or (num == 64 and code != "D649"):
            return "Diabetes mellitus, blood and endocrine disorders"

    # E-codes
    if letter == "E" and num is not None:
        if num <= 2 or 40 <= num <= 46 or num == 50 or 51 <= num <= 64:
            return "Nutritional deficiencies"
        return "Diabetes mellitus, blood and endocrine disorders"

    if letter == "F":
        return "Neuropsychiatric conditions"

    if letter == "G" and num is not None:
        if num <= 4 or num == 14:
            return "Infectious and parasitic diseases"
        return "Neuropsychiatric conditions"

    if letter == "H" and num is not None:
        if 65 <= num <= 66:
            return "Respiratory infections"
        return "Sense organ diseases"

    if letter == "I":
        return "Cardiovascular diseases"

    if letter == "J" and num is not None:
        if num <= 22:
            return "Respiratory infections"
        if 30 <= num <= 98:
            return "Respiratory diseases"

    if letter == "K" and num is not None:
        if num <= 14:
            return "Oral conditions"
        if 20 <= num <= 92:
            return "Digestive diseases"

    if letter == "L":
        return "Skin diseases"

    if letter == "M":
        return "Musculoskeletal diseases"

    if letter == "N" and num is not None:
        if 70 <= num <= 73:
            return "Infectious and parasitic diseases"
        return "Genitourinary diseases"

    if letter == "O":
        return "Maternal conditions"

    if letter == "P":
        if code.startswith("P23"):
            return "Respiratory infections"
        if code in {"P373", "P374"}:
            return "Infectious and parasitic diseases"
        return "Perinatal conditions"

    if letter == "Q":
        return "Congenital anomalies"

    if letter == "R" and num is not None:
        if code.startswith("R95"):
            return "Sudden infant death syndrome"
        # WHO list: R00-R94, R96-R99
        if (0 <= num <= 94) or (96 <= num <= 99):
            return "Ill-defined diseases"

    if letter == "U":
        # WHO mortality list puts these under respiratory infections / COVID
        if code in {"U04", "U049"} or code.startswith("U04"):
            return "Respiratory infections"
        if code in {"U071", "U072", "U099", "U109"}:
            return "Respiratory infections"
        if code in {"U07", "U09", "U10"}:
            # category-level U07/U09/U10 in your data; map to same broad bucket
            return "Respiratory infections"
        if code in {"U129"} or code == "U12":
            return "Unintentional injuries"
        if code in {"U070"}:
            return "Neuropsychiatric conditions"
        if code.startswith("U85"):
            # U85: Emergency use of COVID-19 vaccines
            return "Unintentional injuries"

    if letter in {"V", "W"}:
        return "Unintentional injuries"

    if letter == "X":
        if code in {
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
        }:
            return "Neuropsychiatric conditions"
        if num is not None and 60 <= num <= 84:
            return "Intentional injuries"
        return "Unintentional injuries"

    if letter == "Y" and num is not None:
        if 10 <= num <= 34 or code == "Y872":
            return "Ill-defined injuries"
        if 35 <= num <= 36 or code in {"Y870", "Y871"} or num <= 9:
            return "Intentional injuries"
        if 40 <= num <= 89:
            return "Unintentional injuries"

    return "Uncategorized"


def run() -> None:
    """Load snapshot and create meadow dataset."""
    # Load snapshot
    snap = paths.load_snapshot("mortality_database_aggregated.feather")
    tb = snap.read()

    tb["cause_category"] = tb["cause"].apply(map_icd10_to_category)
    tb = tb.drop(columns=["cause"])

    group_cols = ["country", "year", "sex", "age_group", "cause_category"]

    tb = tb.groupby(group_cols, as_index=False).agg(
        {
            "deaths": "sum",
            "population": "first",
        }
    )

    tb = tb.format(group_cols)
    # Save to meadow
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
