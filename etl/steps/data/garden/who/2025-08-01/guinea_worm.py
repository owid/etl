"""Load a meadow dataset and create a garden dataset."""

from itertools import product

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

CURRENT_YEAR = 2024


def run() -> None:
    #
    # Load inputs.
    snap = paths.load_snapshot("guinea_worm_cases.csv")

    tb_cases = snap.read().astype({"year": int})

    # garden dataset (with certification status of countries)

    ds_garden = paths.load_dataset("guinea_worm_certification")
    # Read certification table
    tb_cert = ds_garden["guinea_worm"].reset_index()

    #
    # Process data.
    #
    # add missing years (with no data) to fasttrack dataset
    tb_cases = add_missing_years(tb_cases)

    # harmonize countries
    tb_cert = geo.harmonize_countries(
        df=tb_cert, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_cases = geo.harmonize_countries(
        df=tb_cases, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    # remove leading spaces from "year_certified" column and cast as string
    tb_cert["year_certified"] = tb_cert["year_certified"].str.strip()

    # add year in which country was certified as disease free to all rows
    tb = pr.merge(tb_cert, tb_cases, on=["country", "year"], how="outer")

    # add rows for current year
    tb = add_current_year(tb, tb_cases, year=CURRENT_YEAR)

    # fill N/As with 0 for case counts
    tb["guinea_worm_reported_cases"] = tb["guinea_worm_reported_cases"].fillna(0)

    #
    # Validate outputs
    #
    log.info("Validating guinea worm data")
    _validate_basic_data_integrity(tb)
    _validate_guinea_worm_specific_logic(tb)
    _validate_temporal_consistency(tb)
    _validate_geographic_coverage(tb)
    _validate_data_processing(tb, tb_cases)

    # format index
    tb = tb.format(["country", "year"], short_name="guinea_worm")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    # Do not check variables metadata, as this is added in grapher step
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=False, default_metadata=ds_garden.metadata)

    # Save changes in the new garden dataset
    ds_garden.save()


def add_current_year(tb: Table, tb_cases: Table, year: int = CURRENT_YEAR, changes_dict={}):
    """
    Add rows with current certification status & case numbers for each country
    tb (Table): table with certification status & case numbers until last year
    tb_cases (Table): table with case numbers for all (incl. current) years
    year (int): current year
    changes_dict (dict): changes to certification status since last year with key: country, value: certification status
    (changes_dict is empty for 2023, including it to make code reusable for future years,
    e.g. if Angola is certified in a future year, pass {"Angola": "Certified disease free"} as changes_dict for that year)
    """
    country_list = tb["country"].unique()
    last_year = year - 1

    # in case tb includes data for current year, remove it
    tb = tb.loc[tb["year"] != year]

    row_dicts = []

    # add rows for current year
    for country in country_list:
        cty_dict = {"country": country, "year": year}
        # get certification status for last year
        cty_row_last_year = tb.loc[(tb["country"] == country) & (tb["year"] == last_year)]
        if country in changes_dict:
            cty_dict["certification_status"] = changes_dict[country]
            if changes_dict[country] == "Certified disease free":
                cty_dict["year_certified"] = last_year
        else:
            if not cty_row_last_year.empty:
                cty_dict["certification_status"] = cty_row_last_year["certification_status"].values[0]
                cty_dict["year_certified"] = cty_row_last_year["year_certified"].values[0]
            else:
                # Handle the case where no data exists for the previous year
                cty_dict["certification_status"] = None
                cty_dict["year_certified"] = None
        # get case numbers for current year
        cases_df = tb_cases.loc[(tb_cases["country"] == country) & (tb_cases["year"] == year)]
        if cases_df.empty:
            cty_dict["guinea_worm_reported_cases"] = 0
        else:
            cty_dict["guinea_worm_reported_cases"] = cases_df["guinea_worm_reported_cases"].values[0]
        row_dicts.append(cty_dict)

    # create new table with current year
    new_year_tb = Table(
        pd.DataFrame(row_dicts)[
            ["country", "year", "year_certified", "certification_status", "guinea_worm_reported_cases"]
        ]
    )

    tb = pr.concat([tb, new_year_tb], ignore_index=True)

    return tb


def add_missing_years(tb: Table) -> Table:
    """
    Add full spectrum of year-country combinations to fast-track dataset so we have zeros where there is missing data
    """
    years = tb["year"].unique()
    countries = tb["country"].unique()
    comb_df = Table(pd.DataFrame(list(product(countries, years)), columns=["country", "year"]))

    tb = Table(pr.merge(tb, comb_df, on=["country", "year"], how="outer"), short_name=paths.short_name)

    return tb


def _validate_basic_data_integrity(tb: Table) -> None:
    """Validate basic data integrity for guinea worm dataset."""
    # Dataset should not be empty
    assert len(tb) > 0, "Dataset is empty"

    # Check required columns exist
    required_cols = ["country", "year", "guinea_worm_reported_cases"]
    missing_cols = [col for col in required_cols if col not in tb.columns]
    assert not missing_cols, f"Missing required columns: {missing_cols}"

    # Year should be integer and within reasonable range
    assert tb["year"].dtype in ["int64", "Int64"], "Year column should be integer"
    min_year, max_year = 1980, 2030
    assert tb["year"].min() >= min_year, f"Year too early: {tb['year'].min()}"
    assert tb["year"].max() <= max_year, f"Year too late: {tb['year'].max()}"

    # Case counts must be non-negative integers
    cases_col = "guinea_worm_reported_cases"
    assert tb[cases_col].min() >= 0, f"Negative case counts found: {tb[cases_col].min()}"

    # Check for reasonable case count bounds (guinea worm is nearly eradicated)
    max_cases = tb[cases_col].max()
    if max_cases > 1000000:
        log.warning("Unusually high case counts found", max_cases=max_cases)


def _validate_guinea_worm_specific_logic(tb: Table) -> None:
    """Validate guinea worm disease-specific business logic."""
    # Valid certification statuses
    valid_statuses = {"Certified disease free", "Pre-certification", "Endemic", "Pending surveillance"}

    if "certification_status" in tb.columns:
        invalid_statuses = set(tb["certification_status"].dropna().unique()) - valid_statuses
        assert not invalid_statuses, f"Invalid certification statuses found: {invalid_statuses}"

    # Countries certified as disease free should have 0 cases after certification
    if "certification_status" in tb.columns and "year_certified" in tb.columns:
        certified_countries = tb[tb["certification_status"] == "Certified disease free"]

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
                log.warning(
                    "Certified country has cases after certification",
                    country=country,
                    year=current_year,
                    cert_year=cert_year,
                    cases=cases,
                )

    # Case counts should be very low overall (disease near eradication)
    recent_years = tb[tb["year"] >= 2020]
    if len(recent_years) > 0:
        global_cases = recent_years.groupby("year")["guinea_worm_reported_cases"].sum()
        max_global_cases = global_cases.max()

        # Guinea worm had <100 cases globally in recent years
        if max_global_cases > 1000:
            log.warning("Unexpectedly high global case counts in recent years", max_cases=max_global_cases)


def _validate_temporal_consistency(tb: Table) -> None:
    """Validate temporal consistency of guinea worm data."""
    # Check for countries that may have backslid from certification
    if "certification_status" in tb.columns:
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
                    # Get the specific statuses they regressed to
                    regression_statuses = non_certified["certification_status"].unique()
                    log.warning(
                        "Country appears to have lost certification status - please verify this is correct",
                        country=country,
                        first_certified_year=first_cert_year,
                        regression_years=non_certified["year"].tolist(),
                        new_statuses=regression_statuses.tolist(),
                    )

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

    # Should have reasonable country coverage (not just 1-2 countries)
    min_countries = 5
    assert len(countries) >= min_countries, f"Only {len(countries)} countries found, expected at least {min_countries}"

    # Check for obvious data quality issues in country names
    for country in countries:
        assert len(country) >= 2, f"Country name too short: '{country}'"
        assert not country.isdigit(), f"Country name appears to be numeric: '{country}'"


def _validate_data_processing(tb_final: Table, tb_cases: Table) -> None:
    """Validate data processing steps completed correctly."""
    # Check that missing years were filled correctly
    years_in_cases = set(tb_cases["year"].unique())
    countries_in_cases = set(tb_cases["country"].unique())

    # Final table should have all year-country combinations for countries in cases data
    expected_combinations = len(years_in_cases) * len(countries_in_cases)
    actual_combinations = len(tb_final[tb_final["country"].isin(countries_in_cases)])

    # Allow some tolerance for countries that might not appear in all years
    tolerance_ratio = 0.8
    assert (
        actual_combinations >= expected_combinations * tolerance_ratio
    ), f"Missing year-country combinations: expected ~{expected_combinations}, got {actual_combinations}"

    # Current year data should be present
    current_year_data = tb_final[tb_final["year"] == CURRENT_YEAR]
    assert len(current_year_data) > 0, f"No data found for current year {CURRENT_YEAR}"

    # Verify no null values in case counts (should be filled with 0)
    null_cases = tb_final["guinea_worm_reported_cases"].isna().sum()
    assert null_cases == 0, f"Found {null_cases} null values in case counts after processing"
