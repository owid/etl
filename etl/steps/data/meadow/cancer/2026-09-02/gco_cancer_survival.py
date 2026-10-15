"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns that identify one estimate in the bulk file.
INDEX_COLUMNS = [
    "country",
    "sub_region",
    "cancer_site",
    "sex",
    "age_group",
    "interval",
    "year",
    "measure_type",
    "survival_years",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("gco_cancer_survival.csv")

    # Load data from snapshot (the file uses "NA" for missing values).
    tb = snap.read(safe_types=False, na_values=["NA"])

    #
    # Process data.
    #
    tb = tb.rename(columns={"agegrp": "age_group", "time": "survival_years"})

    # "year" mixes single years (annual estimates, interval=1) and five-year periods like "1995-1999" (interval=5), so
    # it is kept as text here. "survival_years" (years since diagnosis) only applies to survival measures and is
    # empty for incidence and mortality rates; use 0 so it can be part of the index.
    tb["year"] = tb["year"].astype(str)
    tb["survival_years"] = tb["survival_years"].fillna(0).astype(int)

    # Use categoricals for the low-cardinality text columns.
    for column in ["country", "sub_region", "cancer_site", "sex", "age_group", "measure_type"]:
        tb[column] = tb[column].astype("category")

    # Improve table format.
    tb = tb.format(INDEX_COLUMNS)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
