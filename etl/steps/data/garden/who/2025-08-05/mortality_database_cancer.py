"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    log.info("Starting WHO cancer mortality database processing")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("mortality_database_cancer")
    tb = ds_meadow.read("mortality_database_cancer", safe_types=False)

    # Validate raw input data
    log.info("Validating input data")
    _validate_input_data(tb)
    _validate_cancer_mortality_values(tb)
    _validate_demographic_dimensions(tb)
    _validate_country_coverage(tb)

    #
    # Process data.
    #
    log.info("Processing data")
    tb = tidy_sex_dimension(tb)
    tb = tidy_age_dimension(tb)
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Calculate death rates for  combined age groups.
    # The death rate is per 100,000 population, so we reverse-calculate the population size.
    tb["estimated_population"] = tb["number"] / tb["death_rate_per_100_000_population"] * 100000
    tb = add_age_group_aggregate(tb, ["less than 1 year", "1-4 years"], "< 5 years")
    tb = add_age_group_aggregate(tb, ["less than 1 year", "1-4 years", "5-9 years"], "< 10 years")
    tb = tb.drop(columns=["estimated_population"])

    # Final validation BEFORE formatting
    log.info("Validating processed data")
    _validate_cancer_causes(tb)
    _validate_cancer_age_patterns(tb)
    _validate_temporal_patterns(tb)
    _validate_data_completeness(tb)

    # Format table (sets indexes) - do this AFTER validation
    tb = tb.format(["country", "year", "sex", "age_group", "cause", "icd10_codes"])
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=False, default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()
    log.info("WHO cancer mortality database processing completed successfully")


def add_age_group_aggregate(tb: Table, age_groups: list[str], label: str) -> Table:
    """
    Aggregates death numbers and recalculates death rates for a combined age group.

    Parameters:
    - tb (Table): Original table with disaggregated age group data.
    - age_groups (list of str): List of age group labels to combine (e.g., ["less than 1 year", "1-4 years"]).
    - label (str): New age group label to assign to the aggregated rows (e.g., "< 5 years").

    Returns:
    - Table: Aggregated rows with updated death rate and age group label merged into the original table.
    """
    # Filter relevant age groups
    tb_filtered = tb[tb["age_group"].isin(age_groups)].copy()

    # Group by relevant dimensions and sum values
    tb_filtered = tb_filtered.groupby(["country", "year", "sex", "cause", "icd10_codes"], as_index=False).agg(
        {"number": "sum", "estimated_population": "sum"}
    )

    # Recalculate the death rate for the new age group
    tb_filtered["death_rate_per_100_000_population"] = (
        tb_filtered["number"] / tb_filtered["estimated_population"] * 100000
    )

    # Assign new age group label
    tb_filtered["age_group"] = label

    # Drop the helper column
    tb = pr.concat([tb, tb_filtered])
    return tb


def tidy_sex_dimension(tb: Table) -> Table:
    """
    Improve the labelling of the sex column
    """
    sex_dict = {"All": "Both sexes", "Female": "Females", "Male": "Males", "Unknown": "Unknown sex"}
    tb["sex"] = tb["sex"].replace(sex_dict)
    return tb


def tidy_age_dimension(tb: Table) -> Table:
    age_dict = {
        "[Unknown]": "Unknown age",
        "[85+]": "over 85 years",
        "[80-84]": "80-84 years",
        "[75-79]": "75-79 years",
        "[70-74]": "70-74 years",
        "[65-69]": "65-69 years",
        "[60-64]": "60-64 years",
        "[55-59]": "55-59 years",
        "[50-54]": "50-54 years",
        "[45-49]": "45-49 years",
        "[40-44]": "40-44 years",
        "[35-39]": "35-39 years",
        "[30-34]": "30-34 years",
        "[25-29]": "25-29 years",
        "[20-24]": "20-24 years",
        "[15-19]": "15-19 years",
        "[10-14]": "10-14 years",
        "[5-9]": "5-9 years",
        "[1-4]": "1-4 years",
        "[0]": "less than 1 year",
        "[All]": "all ages",
    }

    tb["age_group"] = tb["age_group"].replace(age_dict)
    return tb


def _validate_input_data(tb: Table) -> None:
    """Basic integrity checks for cancer mortality database."""
    assert len(tb) > 0, "Dataset is empty"

    # Required columns
    required_cols = ["country", "year", "sex", "age_group", "cause", "number"]
    missing_cols = [col for col in required_cols if col not in tb.columns]
    assert not missing_cols, f"Missing required columns: {missing_cols}"

    # Check for duplicates
    key_cols = ["country", "year", "sex", "age_group", "cause"]
    duplicates = tb.duplicated(subset=key_cols)
    assert not duplicates.any(), f"Found {duplicates.sum()} duplicate records"

    # Data types
    assert pd.api.types.is_numeric_dtype(tb["number"]), "Deaths count should be numeric"
    assert pd.api.types.is_integer_dtype(tb["year"]), "Year should be integer type"


def _validate_cancer_mortality_values(tb: Table) -> None:
    """Validate cancer-specific mortality data ranges."""
    # Death counts should be non-negative
    negative_deaths = tb[tb["number"] < 0]
    assert len(negative_deaths) == 0, f"Found {len(negative_deaths)} negative death counts"

    # Death rates per 100k should be reasonable for cancer
    if "death_rate_per_100_000_population" in tb.columns:
        death_rates = tb["death_rate_per_100_000_population"].dropna()
        # Cancer death rates typically don't exceed 1000 per 100k even for high-risk groups
        extreme_rates = death_rates[death_rates > 1000]
        if len(extreme_rates) > 0:
            log.warning(
                "Extremely high cancer death rates found",
                count=len(extreme_rates),
                examples=tb[tb["death_rate_per_100_000_population"].isin(extreme_rates)][
                    ["country", "year", "sex", "age_group", "cause", "death_rate_per_100_000_population"]
                ]
                .head()
                .to_dict("records"),
            )



def _validate_demographic_dimensions(tb: Table) -> None:
    """Validate sex and age group categories."""
    expected_sex_categories = {"All", "Female", "Male", "Unknown"}
    actual_sex = set(tb["sex"].unique())
    unexpected_sex = actual_sex - expected_sex_categories
    assert not unexpected_sex, f"Unexpected sex categories: {unexpected_sex}"

    # Check if age groups are in raw format or tidied format
    sample_age = next(iter(tb["age_group"].unique()), "")
    if sample_age and sample_age.startswith("["):
        # Raw format validation
        expected_age_groups = {
            "[All]",
            "[0]",
            "[1-4]",
            "[5-9]",
            "[10-14]",
            "[15-19]",
            "[20-24]",
            "[25-29]",
            "[30-34]",
            "[35-39]",
            "[40-44]",
            "[45-49]",
            "[50-54]",
            "[55-59]",
            "[60-64]",
            "[65-69]",
            "[70-74]",
            "[75-79]",
            "[80-84]",
            "[85+]",
            "[Unknown]",
        }
    else:
        # Tidied format validation
        expected_age_groups = {
            "All ages",
            "less than 1 year",
            "1-4 years",
            "5-9 years",
            "10-14 years",
            "15-19 years",
            "20-24 years",
            "25-29 years",
            "30-34 years",
            "35-39 years",
            "40-44 years",
            "45-49 years",
            "50-54 years",
            "55-59 years",
            "60-64 years",
            "65-69 years",
            "70-74 years",
            "75-79 years",
            "80-84 years",
            "85+ years",
            "Unknown age",
        }
    
    actual_ages = set(tb["age_group"].unique())
    unexpected_ages = actual_ages - expected_age_groups
    if unexpected_ages:
        log.warning("Unexpected age group categories", categories=list(unexpected_ages))


def _validate_cancer_causes(tb: Table) -> None:
    """Validate cancer cause categories."""
    cancer_causes = set(tb["cause"].unique())

    # Log what cancer types are covered
    log.info("Cancer causes in dataset", count=len(cancer_causes), causes=list(cancer_causes)[:10])  # Show first 10

    # Check for expected major cancer types
    expected_major_cancers = {
        "Malignant neoplasms",
        "Lung cancer",
        "Breast cancer",
        "Colorectal cancer",
        "Stomach cancer",
        "Liver cancer",
        "Prostate cancer",
    }
    covered_cancers = expected_major_cancers & cancer_causes
    if covered_cancers:
        log.info("Major cancer types covered", covered=list(covered_cancers))


def _validate_cancer_age_patterns(tb: Table) -> None:
    """Validate cancer mortality age patterns."""
    # Cancer mortality should generally increase with age
    for country in ["World", "United States", "Germany", "Japan"]:
        country_data = tb[
            (tb["country"] == country) & (tb["cause"] == "Malignant neoplasms") & (tb["sex"] == "Both sexes")
        ]

        if len(country_data) == 0:
            continue

        # Get recent year data
        recent_year = country_data["year"].max()
        recent_data = country_data[country_data["year"] == recent_year]

        # Check that cancer rates are higher in elderly vs young adults
        elderly_data = recent_data[recent_data["age_group"].isin(["65-69 years", "70-74 years"])]
        young_adult_data = recent_data[recent_data["age_group"].isin(["25-29 years", "30-34 years"])]

        if len(elderly_data) > 0 and len(young_adult_data) > 0:
            if "death_rate_per_100_000_population" in tb.columns:
                elderly_rate = elderly_data["death_rate_per_100_000_population"].mean()
                young_rate = young_adult_data["death_rate_per_100_000_population"].mean()

                if pd.notna(elderly_rate) and pd.notna(young_rate):
                    if elderly_rate <= young_rate * 2:  # Expect at least 2x higher in elderly
                        log.warning(
                            "Cancer mortality not significantly higher in elderly",
                            country=country,
                            year=recent_year,
                            elderly_rate=elderly_rate,
                            young_adult_rate=young_rate,
                        )


def _validate_temporal_patterns(tb: Table) -> None:
    """Check for reasonable temporal trends in cancer mortality."""
    # Validate year ranges
    min_year, max_year = 1950, 2025
    assert tb["year"].min() >= min_year, f"Year too early: {tb['year'].min()}"
    assert tb["year"].max() <= max_year, f"Year too late: {tb['year'].max()}"

    # Check for major jumps in cancer death counts
    for country in ["United States", "China", "India"]:
        country_data = tb[
            (tb["country"] == country)
            & (tb["cause"] == "Malignant neoplasms")
            & (tb["sex"] == "Both sexes")
            & (tb["age_group"] == "all ages")
        ].sort_values("year")

        if len(country_data) < 3:
            continue

        deaths = country_data["number"].values
        year_changes = np.diff(deaths) / deaths[:-1] * 100  # Percent change

        extreme_changes = np.abs(year_changes) > 40  # >40% change year-over-year
        if extreme_changes.any():
            problem_years = country_data["year"].iloc[1:][extreme_changes]
            log.warning(
                "Extreme year-over-year changes in cancer deaths",
                country=country,
                years=problem_years.tolist(),
                max_change=f"{year_changes[extreme_changes].max():.1f}%",
            )


def _validate_country_coverage(tb: Table) -> None:
    """Validate country coverage and geographic patterns."""
    countries = set(tb["country"].unique())

    # Check minimum country coverage
    min_countries = 50
    assert len(countries) >= min_countries, f"Only {len(countries)} countries found"

    # Check for major countries
    major_countries = {
        "United States",
        "China",
        "India",
        "Germany",
        "United Kingdom",
        "France",
        "Japan",
        "Brazil",
        "Russia",
    }
    missing_major = major_countries - countries
    if missing_major:
        log.warning("Missing major countries/entities", missing=list(missing_major))


def _validate_data_completeness(tb: Table) -> None:
    """Check for concerning data gaps in cancer mortality data."""
    # Check for missing data patterns
    total_records = len(tb)
    missing_deaths = tb["number"].isna().sum()

    if missing_deaths / total_records > 0.05:  # >5% missing
        log.warning("High proportion of missing death counts", missing_pct=f"{missing_deaths/total_records*100:.2f}%")

    # Check temporal completeness for major countries
    for country in ["United States", "Germany", "Japan"]:
        country_data = tb[tb["country"] == country]
        if len(country_data) == 0:
            continue

        years_available = set(country_data["year"].unique())
        expected_years = set(range(2000, 2023))  # Adjust based on expected coverage
        missing_years = expected_years - years_available

        if len(missing_years) > 5:  # More than 5 years missing
            log.warning(
                "Significant temporal gaps",
                country=country,
                missing_years=len(missing_years),
                sample_missing=sorted(list(missing_years))[:5],
            )
