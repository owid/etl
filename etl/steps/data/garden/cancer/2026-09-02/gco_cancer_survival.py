"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Cancer sites as labeled by the source, and the adjective form used in our indicator names.
CANCER_NAMES = {
    "Colon": "Colon",
    "Colorectal": "Colorectal",
    "Liver": "Liver",
    "Lung": "Lung",
    "Oesophagus": "Oesophageal",
    "Ovary": "Ovarian",
    "Pancreas": "Pancreatic",
    "Rectum": "Rectal",
    "Stomach": "Stomach",
}
# Measures kept in this dataset, and their indicator names.
MEASURE_NAMES = {
    "Incidence (ASR)": "incidence__asr",
    "Mortality (ASR)": "mortality__asr",
    "Net Survival": "net_survival",
}
EXPECTED_COUNTRIES = {"Australia", "Canada", "Denmark", "Ireland", "New Zealand", "Norway", "United Kingdom"}
EXPECTED_SEXES = {"All", "Females", "Males"}
EXPECTED_MEASURES = {"Conditional Net Survival", "Incidence (ASR)", "Mortality (ASR)", "Net Survival"}
# Number of (country, year, sex, cancer) rows in the current version of the output.
# NOTE: Fewer rows on the next update usually means a parsing regression rather than a change in the source.
MIN_NUM_ROWS = 3475


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gco_cancer_survival")

    # Read table from meadow dataset.
    tb = ds_meadow.read("gco_cancer_survival", safe_types=False)

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    # Keep national estimates for all ages, as annual series (not five-year diagnosis periods).
    tb = tb[(tb["sub_region"] == "All") & (tb["age_group"] == "All") & (tb["interval"] == 1)]

    # Keep age-standardized incidence and mortality rates, and net survival five years after diagnosis.
    is_rate = tb["measure_type"].isin(["Incidence (ASR)", "Mortality (ASR)"])
    is_five_year_survival = (tb["measure_type"] == "Net Survival") & (tb["survival_years"] == 5)
    tb = tb[is_rate | is_five_year_survival]

    # Net survival is reported as a fraction; express it as a percentage.
    tb["measure"] = tb["measure"].astype(float)
    tb.loc[tb["measure_type"] == "Net Survival", "measure"] *= 100

    # Annual estimates have a single year of diagnosis.
    tb["year"] = tb["year"].astype(str).astype(int)
    tb["measure_type"] = tb["measure_type"].astype(str).map(MEASURE_NAMES)
    tb["cancer"] = tb["cancer_site"].astype(str).map(CANCER_NAMES)
    tb["gender"] = tb["sex"].astype(str)

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # One column per measure. Pivoting on the year of diagnosis keeps each measure aligned with its own year; the
    # online tool's wide export did not, which shifted net survival by one year where a series had gaps.
    tb = tb.pivot(index=["country", "year", "gender", "cancer"], columns="measure_type", values="measure").reset_index()

    # Improve table format.
    tb = tb.format(["country", "year", "gender", "cancer"], short_name=paths.short_name)

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
    error = "Unexpected set of countries in the source data."
    assert set(tb["country"]) == EXPECTED_COUNTRIES, error
    error = "Unexpected set of cancer sites in the source data."
    assert set(tb["cancer_site"]) == set(CANCER_NAMES), error
    error = "Unexpected set of sexes in the source data."
    assert set(tb["sex"]) == EXPECTED_SEXES, error
    error = "Unexpected set of measures in the source data."
    assert set(tb["measure_type"]) == EXPECTED_MEASURES, error
    error = "Rates and survival must be non-negative."
    assert (tb["measure"].dropna() >= 0).all(), error
    # Net survival is reported as a fraction. In small sub-national age strata the estimator can slightly exceed 1
    # (27 values up to 1.06 in the 2019 release), but national all-ages estimates, the ones we publish, must not.
    is_survival = tb["measure_type"].isin(["Net Survival", "Conditional Net Survival"])
    error = "Net survival is expected as a fraction between 0 and (about) 1."
    assert tb[is_survival]["measure"].dropna().between(0, 1.1).all(), error
    is_national = (tb["sub_region"] == "All") & (tb["age_group"] == "All")
    error = "National all-ages net survival must be a fraction between 0 and 1."
    assert tb[is_survival & is_national]["measure"].dropna().between(0, 1).all(), error


def sanity_check_outputs(tb: Table) -> None:
    """Check the output table."""
    error = f"Unexpected output columns: {set(tb.columns) ^ set(MEASURE_NAMES.values())}"
    assert set(tb.columns) == set(MEASURE_NAMES.values()), error
    error = "There should be no columns with only NaNs."
    assert tb.columns[tb.isna().all()].empty, error
    error = "Five-year net survival must be a percentage between 0% and 100%."
    assert tb["net_survival"].dropna().between(0, 100).all(), error
    error = "Incidence and mortality rates must be non-negative."
    assert (tb[["incidence__asr", "mortality__asr"]].min() >= 0).all(), error
    error = f"Only {len(tb)} rows in the output, fewer than the {MIN_NUM_ROWS} of the current version."
    assert len(tb) >= MIN_NUM_ROWS, error
