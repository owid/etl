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
    log.info("Starting WHO self-inflicted injuries processing")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("self_inflicted_injuries")

    # Read table from meadow dataset.
    tb = ds_meadow["self_inflicted_injuries"].reset_index()

    # Validate raw input data
    log.info("Validating input data")
    _validate_input_data(tb)
    _validate_self_harm_mortality_values(tb)
    _validate_demographic_dimensions(tb)
    _validate_country_coverage(tb)

    #
    # Process data.
    #
    log.info("Processing data")
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tidy_sex_dimension(tb)
    tb = tidy_age_dimension(tb)

    # Final validation BEFORE formatting
    log.info("Validating processed data")
    _validate_self_harm_age_patterns(tb)
    _validate_temporal_patterns(tb)
    _validate_data_completeness(tb)

    # Format table (sets indexes) - do this AFTER validation
    tb = tb.format(["country", "year", "sex", "age_group"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
    log.info("WHO self-inflicted injuries processing completed successfully")


def tidy_sex_dimension(tb: Table) -> Table:
    """
    Improve the labelling of the sex column
    """
    sex_dict = {"All": "Both sexes", "Female": "Females", "Male": "Males", "Unknown": "Unknown sex"}
    tb["sex"] = tb["sex"].replace(sex_dict, regex=False)
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

    tb["age_group"] = tb["age_group"].replace(age_dict, regex=False)

    return tb


def _validate_input_data(tb: Table) -> None:
    """Basic integrity checks for self-inflicted injuries mortality database."""
    assert len(tb) > 0, "Dataset is empty"

    # Required columns
    required_cols = ["country", "year", "sex", "age_group", "number"]
    missing_cols = [col for col in required_cols if col not in tb.columns]
    assert not missing_cols, f"Missing required columns: {missing_cols}"

    # Check for duplicates
    key_cols = ["country", "year", "sex", "age_group"]
    duplicates = tb.duplicated(subset=key_cols)
    assert not duplicates.any(), f"Found {duplicates.sum()} duplicate records"

    # Data types
    assert pd.api.types.is_numeric_dtype(tb["number"]), "Deaths count should be numeric"
    assert pd.api.types.is_integer_dtype(tb["year"]), "Year should be integer type"


def _validate_self_harm_mortality_values(tb: Table) -> None:
    """Validate self-inflicted injury mortality data ranges."""
    # Death counts should be non-negative
    negative_deaths = tb[tb["number"] < 0]
    assert len(negative_deaths) == 0, f"Found {len(negative_deaths)} negative death counts"

    # Death rates per 100k should be reasonable for self-harm
    if "death_rate_per_100_000_population" in tb.columns:
        death_rates = tb["death_rate_per_100_000_population"].dropna()
        # Self-harm death rates rarely exceed 100 per 100k even in highest-risk groups
        extreme_rates = death_rates[death_rates > 100]
        if len(extreme_rates) > 0:
            extreme_records = tb[tb["death_rate_per_100_000_population"].isin(extreme_rates)]
            log.warning(
                "Extremely high self-harm death rates found",
                variable="death_rate_per_100_000_population",
                threshold=100,
                count=len(extreme_rates),
                examples=extreme_records[
                    ["country", "year", "sex", "age_group", "death_rate_per_100_000_population"]
                ]
                .head()
                .to_dict("records"),
            )

    # Check percentage values are within valid range
    if "percentage_of_cause_specific_deaths_out_of_total_deaths" in tb.columns:
        percentages = tb["percentage_of_cause_specific_deaths_out_of_total_deaths"].dropna()
        invalid_pct = percentages[(percentages < 0) | (percentages > 100)]
        assert len(invalid_pct) == 0, f"Found {len(invalid_pct)} invalid percentage values"


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
            "all ages",
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
            "over 85 years",
            "Unknown age",
        }

    actual_ages = set(tb["age_group"].unique())
    unexpected_ages = actual_ages - expected_age_groups
    if unexpected_ages:
        log.warning("Unexpected age group categories", categories=list(unexpected_ages))


def _validate_self_harm_age_patterns(tb: Table) -> None:
    """Validate self-inflicted injury mortality age patterns."""
    # Self-harm mortality typically peaks in middle-age groups and has specific patterns
    
    # Check for unexpectedly high rates in very young children (should be very rare)
    young_children_data = tb[
        (tb["age_group"].isin(["less than 1 year", "1-4 years", "5-9 years"]))
        & (tb["sex"] == "Both sexes")
    ]
    
    if len(young_children_data) > 0 and "death_rate_per_100_000_population" in tb.columns:
        high_child_rates = young_children_data[
            young_children_data["death_rate_per_100_000_population"] > 5.0
        ]
        if len(high_child_rates) > 0:
            log.warning(
                "Unusually high self-harm rates in young children",
                variable="death_rate_per_100_000_population",
                sex="Both sexes",
                age_groups=["less than 1 year", "1-4 years", "5-9 years"],
                threshold=5.0,
                count=len(high_child_rates),
                examples=high_child_rates[
                    ["country", "year", "sex", "age_group", "death_rate_per_100_000_population"]
                ]
                .head()
                .to_dict("records"),
            )

    # Check age patterns for major countries
    for country in ["World", "United States", "Germany", "Japan"]:
        country_data = tb[(tb["country"] == country) & (tb["sex"] == "Both sexes")]
        
        if len(country_data) == 0:
            continue

        # Get recent year data
        recent_year = country_data["year"].max()
        recent_data = country_data[country_data["year"] == recent_year]

        # Check that adolescent/young adult rates are concerning (often highest risk groups)
        adolescent_data = recent_data[recent_data["age_group"].isin(["15-19 years", "20-24 years"])]
        elderly_data = recent_data[recent_data["age_group"].isin(["75-79 years", "80-84 years"])]

        if len(adolescent_data) > 0 and len(elderly_data) > 0:
            if "death_rate_per_100_000_population" in tb.columns:
                adolescent_rate = adolescent_data["death_rate_per_100_000_population"].mean()
                elderly_rate = elderly_data["death_rate_per_100_000_population"].mean()

                if pd.notna(adolescent_rate) and pd.notna(elderly_rate):
                    # In many countries, adolescent/young adult self-harm rates are high
                    if adolescent_rate < elderly_rate * 0.5:  # Unusually low adolescent rates
                        log.warning(
                            "Unexpectedly low self-harm rates in adolescents compared to elderly",
                            variable="death_rate_per_100_000_population",
                            sex="Both sexes",
                            adolescent_age_groups=["15-19 years", "20-24 years"],
                            elderly_age_groups=["75-79 years", "80-84 years"],
                            country=country,
                            year=recent_year,
                            adolescent_rate=adolescent_rate,
                            elderly_rate=elderly_rate,
                        )


def _validate_temporal_patterns(tb: Table) -> None:
    """Check for reasonable temporal trends in self-harm mortality."""
    # Validate year ranges
    min_year, max_year = 1950, 2025
    assert tb["year"].min() >= min_year, f"Year too early: {tb['year'].min()}"
    assert tb["year"].max() <= max_year, f"Year too late: {tb['year'].max()}"

    # Check for major jumps in self-harm death counts
    for country in ["United States", "United Kingdom", "Australia", "France"]:
        country_data = tb[
            (tb["country"] == country)
            & (tb["sex"] == "Both sexes")
            & (tb["age_group"] == "all ages")
        ].sort_values("year")

        if len(country_data) < 3:
            continue

        deaths = country_data["number"].values
        # Handle zero values to avoid division issues
        deaths_nonzero = deaths[deaths > 0]
        if len(deaths_nonzero) < len(deaths) * 0.5:  # Skip if too many zeros
            continue
            
        year_changes = np.diff(deaths_nonzero) / deaths_nonzero[:-1] * 100  # Percent change

        extreme_changes = np.abs(year_changes) > 50  # >50% change year-over-year
        if extreme_changes.any():
            problem_indices = np.where(extreme_changes)[0] + 1  # +1 because diff removes first element
            problem_years = country_data[country_data["number"] > 0]["year"].iloc[problem_indices]
            log.warning(
                "Extreme year-over-year changes in self-harm deaths",
                variable="number",
                sex="Both sexes",
                age_group="all ages",
                country=country,
                years=problem_years.tolist()[:5],  # Show first 5
                max_change=f"{year_changes[extreme_changes].max():.1f}%",
            )


def _validate_country_coverage(tb: Table) -> None:
    """Validate country coverage and geographic patterns."""
    countries = set(tb["country"].unique())

    # Check minimum country coverage
    min_countries = 30  # Self-harm data often has lower coverage than other mortality causes
    assert len(countries) >= min_countries, f"Only {len(countries)} countries found"

    # Check for major countries that often report self-harm data
    expected_countries = {
        "United States",
        "United Kingdom", 
        "Germany",
        "France",
        "Japan",
        "Australia",
        "Canada",
        "Sweden",
        "Norway",
    }
    missing_major = expected_countries - countries
    if missing_major:
        log.warning("Missing countries with typically good self-harm reporting", missing=list(missing_major))


def _validate_data_completeness(tb: Table) -> None:
    """Check for concerning data gaps in self-harm mortality data."""
    # Check for missing data patterns
    total_records = len(tb)
    missing_deaths = tb["number"].isna().sum()

    if missing_deaths / total_records > 0.1:  # >10% missing (higher tolerance for sensitive data)
        log.warning("High proportion of missing death counts", missing_pct=f"{missing_deaths/total_records*100:.2f}%")

    # Check temporal completeness for countries with good reporting systems
    for country in ["United States", "United Kingdom", "Germany", "Australia"]:
        country_data = tb[tb["country"] == country]
        if len(country_data) == 0:
            continue

        years_available = set(country_data["year"].unique())
        expected_years = set(range(1999, 2023))  # Self-harm reporting improved around late 1990s
        missing_years = expected_years - years_available

        if len(missing_years) > 8:  # More than 8 years missing
            log.warning(
                "Significant temporal gaps in self-harm data",
                country=country,
                missing_years=len(missing_years),
                sample_missing=sorted(list(missing_years))[:5],
            )
