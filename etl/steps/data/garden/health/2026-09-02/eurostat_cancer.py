"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Series (cancer type, sex) reported by Eurostat. Breast and cervical cancer screening only cover women.
EXPECTED_SERIES = {
    ("Breast cancer", "F"),
    ("Cervical cancer", "F"),
    ("Colon and rectum cancer", "F"),
    ("Colon and rectum cancer", "M"),
    ("Colon and rectum cancer", "T"),
}
# Number of countries with at least one value (31 in the 2024-08-23 version, 36 in the 2026-09-02 version; Switzerland
# is listed in the source but has no values).
# NOTE: A lower count on the next update usually means a parsing or harmonization regression, not a real change.
MIN_NUM_COUNTRIES = 36


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("eurostat_cancer")

    # Read table from meadow dataset.
    tb = ds_meadow.read("eurostat_cancer")

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Drop empty observations (Eurostat publishes the full country-year grid, with missing values left blank).
    tb = tb.dropna(subset=["pct_of_population"])

    # Pivot to one column per cancer type and sex (e.g. "colon_and_rectum_cancer_t").
    tb = tb.pivot(
        index=["country", "year"], columns=["icd10", "sex"], values="pct_of_population", join_column_levels_with="_"
    )

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    """Check the structure and value ranges of the meadow table."""
    error = "Unexpected (cancer type, sex) combinations in the source data."
    assert set(zip(tb["icd10"], tb["sex"])) == EXPECTED_SERIES, error
    error = "Duplicate (country, year, cancer type, sex) rows."
    assert not tb.duplicated(subset=["country", "year", "icd10", "sex"]).any(), error
    error = "Screening coverage must be a share between 0% and 100%."
    assert tb["pct_of_population"].dropna().between(0, 100).all(), error


def sanity_check_outputs(tb: Table) -> None:
    """Check the output table."""
    expected_columns = {f"{cancer}_{sex}".lower().replace(" ", "_") for cancer, sex in EXPECTED_SERIES}
    error = f"Unexpected output columns: {set(tb.columns) ^ expected_columns}"
    assert set(tb.columns) == expected_columns, error
    error = "There should be no columns with only NaNs."
    assert tb.columns[tb.isna().all()].empty, error
    error = "Screening coverage must be a share between 0% and 100%."
    assert (tb.min().min() >= 0) and (tb.max().max() <= 100), error
    num_countries = tb.index.get_level_values("country").nunique()
    error = f"Only {num_countries} countries in the output, fewer than the {MIN_NUM_COUNTRIES} of the current version."
    assert num_countries >= MIN_NUM_COUNTRIES, error
