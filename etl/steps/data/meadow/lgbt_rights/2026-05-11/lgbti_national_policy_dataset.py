"""Load the LGBTI National Policy Dataset snapshot and create a meadow dataset.

The source file is long format with one row per (country, year, law, status).
We rename columns and Law/Status values to snake_case, cast low-cardinality string
columns to categorical, and persist the 4-column index.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Rename source columns to snake_case.
COLUMN_RENAME = {
    "Country": "country",
    "ISO": "iso3",
    "COW": "cow",
    "Year": "year",
    "Law": "law",
    "Status": "status",
    "Proportion": "proportion",
    "Year_of_National_Adoption": "year_of_national_adoption",
    "Year_of_First_Adoption": "year_of_first_adoption",
    "Year_of_National_Repeal": "year_of_national_repeal",
    "Year_Law_Changed": "year_law_changed",
    "Blood_Restriction_Period": "blood_restriction_period",
    "Max_Punishment": "max_punishment",
    "Fine_Amount": "fine_amount",
    "Sexes_Covered": "sexes_covered",
    "Evidence_of_Enforcement": "evidence_of_enforcement",
    "Sub_National_Variation": "sub_national_variation",
    "Sub_National_Coverage": "sub_national_coverage",
    "Gender_Change_Requirement": "gender_change_requirement",
    "Civil_Union_Benefits": "civil_union_benefits",
    "Source_1": "source_1",
    "Source_2": "source_2",
    "Source_3": "source_3",
    "Source_4": "source_4",
    "Source_5": "source_5",
    "Source_6": "source_6",
    "Source_7": "source_7",
    "Source_8": "source_8",
}

# Normalize the 27 Law values to snake_case (explicit for stability across releases).
LAW_RENAME = {
    "Age of Consent": "age_of_consent",
    "Blood Donations": "blood_donations",
    "Civil Unions": "civil_unions",
    "Constitutional Protections - Gender Identity": "constitutional_protections_gender_identity",
    "Constitutional Protections - Sexual Orientation": "constitutional_protections_sexual_orientation",
    "Conversion Therapies": "conversion_therapies",
    "Death Penalty": "death_penalty",
    "Employment Discrimination - Gender Identity": "employment_discrimination_gender_identity",
    "Employment Discrimination - Sexual Orientation": "employment_discrimination_sexual_orientation",
    "Gender Affirming Care - Adults": "gender_affirming_care_adults",
    "Gender Affirming Care - Minors": "gender_affirming_care_minors",
    "Gender Assignment Surgeries on Children": "gender_assignment_surgeries_on_children",
    "Gender Marker Change": "gender_marker_change",
    "Goods/Services Discrimination - Gender Identity": "goods_services_discrimination_gender_identity",
    "Goods/Services Discrimination - Sexual Orientation": "goods_services_discrimination_sexual_orientation",
    "Hate Crime Protections - Gender Identity": "hate_crime_protections_gender_identity",
    "Hate Crime Protections - Sexual Orientation": "hate_crime_protections_sexual_orientation",
    "Incitement to Hatred": "incitement_to_hatred",
    "Joint Adoption": "joint_adoption",
    "LGB Military": "lgb_military",
    "LGBTQ+ Civil Society Restrictions": "lgbtq_civil_society_restrictions",
    "Marriage Equality": "marriage_equality",
    "Morality/Propaganda": "morality_propaganda",
    "Religious Exemption Laws": "religious_exemption_laws",
    "Same-Sex Acts": "same_sex_acts",
    "Third-Gender Recognition": "third_gender_recognition",
    "Transgender Military": "transgender_military",
}

STATUS_RENAME = {
    "Legal": "legal",
    "Illegal": "illegal",
    "Equal": "equal",
    "Unequal": "unequal",
    "Covered": "covered",
    "Restricted": "restricted",
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("lgbti_national_policy_dataset.csv")
    tb = snap.read(safe_types=False, low_memory=False)

    #
    # Process data.
    #
    # Rename source columns to snake_case.
    tb = tb.rename(columns=COLUMN_RENAME, errors="raise")

    # Normalize Law and Status values to snake_case.
    tb["law"] = tb["law"].map(LAW_RENAME)
    tb["status"] = tb["status"].map(STATUS_RENAME)
    assert tb["law"].notna().all(), "Unmapped Law value(s) — extend LAW_RENAME"
    assert tb["status"].notna().all(), "Unmapped Status value(s) — extend STATUS_RENAME"

    # Cast low-cardinality columns to categorical for read-time and feather-size wins.
    for col in ("country", "law", "status"):
        tb[col] = tb[col].astype("category")

    # Cast mixed object columns (string-or-NaN) to pandas string dtype so feather repack works.
    mixed_object_cols = [
        "blood_restriction_period",
        "max_punishment",
        "fine_amount",
        "sexes_covered",
        "sub_national_coverage",
        "year_law_changed",
        "gender_change_requirement",
        *[f"source_{i}" for i in range(1, 9)],
    ]
    for col in mixed_object_cols:
        tb[col] = tb[col].astype("string")

    # 4-component index. The natural key is (country, year, law, status).
    tb = tb.format(["country", "year", "law", "status"], sort_columns=True)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
