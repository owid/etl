"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

log = get_logger()


def _validate_basic_data_integrity(tb: Table) -> None:
    """Validate basic data integrity for guinea worm dataset."""
    # Dataset should not be empty
    assert len(tb) > 0, "Dataset is empty"
    log.info("✓ Dataset is not empty", rows=len(tb))

    # Check required columns exist
    required_cols = ["country", "year", "guinea_worm_reported_cases"]
    missing_cols = [col for col in required_cols if col not in tb.columns]
    assert not missing_cols, f"Missing required columns: {missing_cols}"
    log.info("✓ All required columns present")

    # Year should be integer and within reasonable range
    assert tb["year"].dtype in ["int64", "Int64"], "Year column should be integer"
    log.info("✓ Year column has correct data type")

    min_year, max_year = 1980, 2030
    assert tb["year"].min() >= min_year, f"Year too early: {tb['year'].min()}"
    assert tb["year"].max() <= max_year, f"Year too late: {tb['year'].max()}"
    log.info("✓ Year range is valid", min_year=int(tb["year"].min()), max_year=int(tb["year"].max()))

    # Case counts must be non-negative integers
    cases_col = "guinea_worm_reported_cases"
    assert tb[cases_col].min() >= 0, f"Negative case counts found: {tb[cases_col].min()}"
    log.info("✓ All case counts are non-negative")

    # Check for reasonable case count bounds (guinea worm is nearly eradicated)
    max_cases = tb[cases_col].max()
    if max_cases > 1000000:
        log.warning("Unusually high case counts found", max_cases=max_cases)
    else:
        log.info("✓ Case counts within reasonable bounds", max_cases=int(max_cases))


def _validate_guinea_worm_specific_logic(tb: Table) -> None:
    """Validate guinea worm disease-specific business logic."""
    # Valid certification statuses
    valid_statuses = {"Certified disease free", "Pre-certification", "Endemic", "Pending surveillance"}

    if "certification_status" in tb.columns:
        invalid_statuses = set(tb["certification_status"].dropna().unique()) - valid_statuses
        assert not invalid_statuses, f"Invalid certification statuses found: {invalid_statuses}"
        log.info("✓ All certification statuses are valid")

    # Countries certified as disease free should have 0 cases after certification
    if "certification_status" in tb.columns and "year_certified" in tb.columns:
        certified_countries = tb[tb["certification_status"] == "Certified disease free"]
        violations_found = False

        for _, row in certified_countries.iterrows():
            country = row["country"]
            cert_year = row["year_certified"]
            current_year = row["year"]
            cases = row["guinea_worm_reported_cases"]

            # Skip if certification year is missing or invalid
            if pd.isna(cert_year) or not str(cert_year).isdigit():
                continue

            cert_year = int(float(str(cert_year)))

            # If current year is after certification, cases should be 0
            if current_year >= cert_year and cases > 0:
                violations_found = True
                log.warning(
                    "Certified country has cases after certification",
                    country=country,
                    year=current_year,
                    cert_year=cert_year,
                    cases=cases,
                )

        if not violations_found:
            log.info("✓ Certified countries have no cases after certification")

    # Case counts should be very low overall (disease near eradication)
    recent_years = tb[tb["year"] >= 2020]
    if len(recent_years) > 0:
        global_cases = recent_years.groupby("year")["guinea_worm_reported_cases"].sum()
        max_global_cases = global_cases.max()

        # Guinea worm had <100 cases globally in recent years
        if max_global_cases > 1000:
            log.warning("Unexpectedly high global case counts in recent years", max_cases=max_global_cases)
        else:
            log.info("✓ Recent global case counts are appropriately low", max_cases=int(max_global_cases))


def _validate_temporal_consistency(tb: Table) -> None:
    """Validate temporal consistency of guinea worm data."""
    # Check for countries that may have backslid from certification
    if "certification_status" in tb.columns:
        regressions_found = False
        for country in tb["country"].unique():
            country_data = tb[tb["country"] == country].sort_values("year")

            # Check if country was ever certified
            certified_years = country_data[country_data["certification_status"] == "Certified disease free"]["year"]
            if len(certified_years) > 0:
                first_cert_year = certified_years.min()

                # Check for any non-certified status after certification
                post_cert_data = country_data[country_data["year"] > first_cert_year]
                non_certified = post_cert_data[post_cert_data["certification_status"] != "Certified disease free"]

                if len(non_certified) > 0:
                    regressions_found = True
                    # Get the specific statuses they regressed to
                    regression_statuses = non_certified["certification_status"].unique()
                    log.warning(
                        "Country appears to have lost certification status - please verify this is correct",
                        country=country,
                        first_certified_year=first_cert_year,
                        regression_years=non_certified["year"].tolist(),
                        new_statuses=regression_statuses.tolist(),
                    )

        if not regressions_found:
            log.info("✓ No certification status regressions detected")

    # Global trend should show overall decline (eradication program)
    if "guinea_worm_reported_cases" in tb.columns:
        # Calculate global totals by year
        global_data = tb.groupby("year")["guinea_worm_reported_cases"].sum().reset_index()
        global_data = global_data.sort_values("year")

        if len(global_data) >= 10:
            # Compare early vs recent periods
            early_avg = global_data.head(5)["guinea_worm_reported_cases"].mean()
            recent_avg = global_data.tail(5)["guinea_worm_reported_cases"].mean()

            # Should show significant decline due to eradication efforts
            if recent_avg >= early_avg * 0.5:  # Allow some variation but expect major decline
                log.warning(
                    "Global guinea worm cases haven't declined as expected", early_avg=early_avg, recent_avg=recent_avg
                )
            else:
                log.info("✓ Global case trend shows expected decline", early_avg=early_avg, recent_avg=recent_avg)


def _validate_geographic_coverage(tb: Table) -> None:
    """Validate geographic coverage includes expected countries."""
    countries = set(tb["country"].unique())

    # Known endemic/recently endemic countries that should be present
    expected_countries = {
        "Chad",
        "South Sudan",
        "Mali",
        "Ethiopia",
        "Angola",
        "Kenya",
        "Democratic Republic of Congo",  # Recently certified
    }

    missing_expected = expected_countries - countries
    if missing_expected:
        log.warning("Missing expected endemic countries", missing_countries=list(missing_expected))
    else:
        log.info("✓ All expected endemic countries present")

    # Should have reasonable country coverage (not just 1-2 countries)
    min_countries = 5
    assert len(countries) >= min_countries, f"Only {len(countries)} countries found, expected at least {min_countries}"
    log.info("✓ Adequate country coverage", num_countries=len(countries))

    # Check for obvious data quality issues in country names
    for country in countries:
        assert len(country) >= 2, f"Country name too short: '{country}'"
        assert not country.isdigit(), f"Country name appears to be numeric: '{country}'"
    log.info("✓ Country names pass quality checks")


def _validate_data_processing(tb_final: Table, tb_cases: Table, current_year: int) -> None:
    """Validate data processing steps completed correctly."""
    # Check that missing years were filled correctly
    years_in_cases = set(tb_cases["year"].unique())
    countries_in_cases = set(tb_cases["country"].unique())

    # Final table should have all year-country combinations for countries in cases data
    expected_combinations = len(years_in_cases) * len(countries_in_cases)
    actual_combinations = len(tb_final[tb_final["country"].isin(countries_in_cases)])

    # Allow some tolerance for countries that might not appear in all years
    tolerance_ratio = 0.8
    assert actual_combinations >= expected_combinations * tolerance_ratio, (
        f"Missing year-country combinations: expected ~{expected_combinations}, got {actual_combinations}"
    )
    log.info("✓ Year-country combinations filled correctly", expected=expected_combinations, actual=actual_combinations)

    # Current year data should be present
    current_year_data = tb_final[tb_final["year"] == current_year]
    assert len(current_year_data) > 0, f"No data found for current year {current_year}"
    log.info("✓ Current year data present", current_year=current_year, rows=len(current_year_data))

    # Verify no null values in case counts (should be filled with 0)
    null_cases = tb_final["guinea_worm_reported_cases"].isna().sum()
    assert null_cases == 0, f"Found {null_cases} null values in case counts after processing"
    log.info("✓ No null values in case counts after processing")
