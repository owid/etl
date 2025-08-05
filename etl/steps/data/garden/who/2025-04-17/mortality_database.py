"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    log.info("Starting WHO mortality database processing")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("mortality_database")
    tb = ds_meadow.read("mortality_database", safe_types=False)

    # Validate raw input data
    log.info("Validating input data")
    _validate_input_data(tb)
    _validate_mortality_values(tb)
    _validate_demographic_dimensions(tb)

    #
    # Process data.
    #
    log.info("Processing data")
    tb = tidy_sex_dimension(tb)
    tb = tidy_age_dimension(tb)
    tb = tidy_causes_dimension(tb)
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Final validation
    log.info("Validating processed data")
    _validate_country_coverage(tb)
    _validate_temporal_patterns(tb)
    _validate_cause_specific_patterns(tb)
    _validate_data_completeness(tb)

    tb = tb.drop(columns="broad_cause_group")
    tb = tb.format(["country", "year", "sex", "age_group", "cause", "icd10_codes"])

    # Save
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=False, default_metadata=ds_meadow.metadata)
    ds_garden.save()
    log.info("WHO mortality database processing completed successfully")


def tidy_causes_dimension(tb: Table) -> Table:
    """
    Improve the labelling of the cause column to make it clear that blood disorders are included in this category.
    """
    cause_dict = {"Diabetes mellitus and endocrine disorders": "Diabetes mellitus, blood and endocrine disorders"}
    tb["cause"] = tb["cause"].cat.rename_categories(lambda x: cause_dict.get(x, x))
    return tb


def tidy_sex_dimension(tb: Table) -> Table:
    """
    Improve the labelling of the sex column
    """
    sex_dict = {"All": "Both sexes", "Female": "Females", "Male": "Males", "Unknown": "Unknown sex"}
    tb["sex"] = tb["sex"].cat.rename_categories(lambda x: sex_dict.get(x, x))
    return tb


def _validate_input_data(tb: Table) -> None:
    """Basic integrity checks for mortality database."""
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


def _validate_mortality_values(tb: Table) -> None:
    """Validate mortality-specific data ranges."""
    # Death counts should be non-negative
    negative_deaths = tb[tb["number"] < 0]
    assert len(negative_deaths) == 0, f"Found {len(negative_deaths)} negative death counts"

    # Age-standardized rates should be reasonable (0-10,000 per 100k)
    if "age_standardized_death_rate_per_100_000_standard_population" in tb.columns:
        rate_col = "age_standardized_death_rate_per_100_000_standard_population"
        rates = tb[rate_col].dropna()
        extreme_rates = rates[(rates < 0) | (rates > 10000)]
        if len(extreme_rates) > 0:
            log.warning(
                "Extreme age-standardized rates found",
                count=len(extreme_rates),
                max_rate=extreme_rates.max(),
                min_rate=extreme_rates.min(),
            )

    # Death rates per 100k should be reasonable
    if "death_rate_per_100_000_population" in tb.columns:
        death_rates = tb["death_rate_per_100_000_population"].dropna()
        extreme_rates = death_rates[death_rates > 50000]  # Extreme threshold
        if len(extreme_rates) > 0:
            log.warning(
                "Extremely high death rates found",
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
    actual_ages = set(tb["age_group"].unique())
    unexpected_ages = actual_ages - expected_age_groups
    if unexpected_ages:
        log.warning("Unexpected age group categories", categories=list(unexpected_ages))


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

    tb["age_group"] = tb["age_group"].cat.rename_categories(lambda x: age_dict.get(x, x))
    return tb


def _validate_temporal_patterns(tb: Table) -> None:
    """Check for reasonable temporal trends."""
    # Validate year ranges
    min_year, max_year = 1950, 2025
    assert tb["year"].min() >= min_year, f"Year too early: {tb['year'].min()}"
    assert tb["year"].max() <= max_year, f"Year too late: {tb['year'].max()}"

    # Check for major jumps in death counts (possible data errors)
    for country in ["World", "United States", "China", "India"]:
        country_data = tb[
            (tb["country"] == country)
            & (tb["cause"] == "All Causes")
            & (tb["sex"] == "Both sexes")
            & (tb["age_group"] == "all ages")
        ].sort_values("year")

        if len(country_data) < 3:
            continue

        deaths = country_data["number"].values
        year_changes = np.diff(deaths) / deaths[:-1] * 100  # Percent change

        extreme_changes = np.abs(year_changes) > 50  # >50% change year-over-year
        if extreme_changes.any():
            problem_years = country_data["year"].iloc[1:][extreme_changes]
            log.warning(
                "Extreme year-over-year changes in deaths",
                country=country,
                years=problem_years.tolist(),
                max_change=f"{year_changes[extreme_changes].max():.1f}%",
            )


def _validate_country_coverage(tb: Table) -> None:
    """Validate country coverage and geographic patterns."""
    countries = set(tb["country"].unique())

    # Check minimum country coverage
    min_countries = 100
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


def _validate_cause_specific_patterns(tb: Table) -> None:
    """Validate cause-specific mortality patterns."""
    # Maternal mortality should only occur in females
    maternal_data = tb[tb["cause"] == "Maternal conditions"]
    if len(maternal_data) > 0:
        maternal_males = maternal_data[maternal_data["sex"] == "Males"]
        assert len(maternal_males) == 0, f"Found {len(maternal_males)} male maternal deaths"

        # Should primarily affect reproductive ages
        maternal_elderly = maternal_data[maternal_data["age_group"].isin(["over 85 years", "80-84 years"])]
        if len(maternal_elderly) > 0:
            elderly_deaths = maternal_elderly["number"].sum()
            total_maternal = maternal_data["number"].sum()
            if elderly_deaths / total_maternal > 0.01:  # >1% in elderly
                log.warning(
                    "High proportion of elderly maternal deaths",
                    elderly_pct=f"{elderly_deaths/total_maternal*100:.2f}%",
                )

    # Sudden infant death syndrome should only occur in infants
    sids_data = tb[tb["cause"] == "Sudden infant death syndrome"]
    if len(sids_data) > 0:
        # Only check records with actual deaths (> 0), ignore missing/zero values
        # Allow infant ages plus "all ages" and "unknown age" aggregates
        allowed_age_groups = ["less than 1 year", "all ages", "all ages", "Unknown age"]
        sids_adults = sids_data[
            (~sids_data["age_group"].isin(allowed_age_groups))
            & (sids_data["number"] > 0)
            & (sids_data["number"].notna())
        ]
        if len(sids_adults) > 0:
            log.warning(
                "SIDS deaths in non-infant age groups",
                count=len(sids_adults),
                age_groups=sids_adults["age_group"].unique().tolist(),
                countries=sids_adults["country"].unique().tolist(),
            )


def _validate_data_completeness(tb: Table) -> None:
    """Check for concerning data gaps."""
    # Check for missing data patterns
    total_records = len(tb)
    missing_deaths = tb["number"].isna().sum()

    if missing_deaths / total_records > 0.05:  # >5% missing
        log.warning("High proportion of missing death counts", missing_pct=f"{missing_deaths/total_records*100:.2f}%")

    # Check temporal completeness for major countries
    for country in ["World", "United States", "Germany", "Japan"]:
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
