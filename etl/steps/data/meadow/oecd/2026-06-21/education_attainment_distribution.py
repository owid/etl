"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Map OECD education level codes to short column names.
EDUCATION_LEVEL_MAP = {
    "ISCED11A_0": "less_than_primary",
    "ISCED11A_0T2": "below_upper_secondary",
    "ISCED11A_3_4": "upper_secondary_post_secondary_non_tertiary",
    "ISCED11A_5T8": "tertiary",
}

SEX_MAP = {
    "_T": "total",
    "F": "female",
    "M": "male",
}


def run() -> None:
    snap = paths.load_snapshot("education_attainment_distribution.csv")
    tb = snap.read()

    # Keep only relevant columns.
    tb = tb[["Reference area", "SEX", "ATTAINMENT_LEV", "TIME_PERIOD", "OBS_VALUE"]]
    tb = tb.rename(
        columns={
            "Reference area": "country",
            "TIME_PERIOD": "year",
            "OBS_VALUE": "value",
            "SEX": "sex",
            "ATTAINMENT_LEV": "education_level",
        },
        errors="raise",
    )

    # Drop rows without data.
    tb = tb.dropna(subset=["value"])

    # Map codes to readable names.
    tb["education_level"] = tb["education_level"].map(EDUCATION_LEVEL_MAP)
    tb["sex"] = tb["sex"].map(SEX_MAP)

    # Pivot education levels into columns.
    tb = tb.pivot(index=["country", "year", "sex"], columns="education_level", values="value").reset_index()

    # Prefix education columns with "share_".
    for col in EDUCATION_LEVEL_MAP.values():
        if col in tb.columns:
            tb = tb.rename(columns={col: f"share_{col}"})

    # Use categoricals for low-cardinality columns.
    tb["country"] = tb["country"].astype("category")
    tb["sex"] = tb["sex"].astype("category")

    tb = tb.format(["country", "year", "sex"])

    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
