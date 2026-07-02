"""Long-run employment and GDP shares by sector for ten currently rich countries.

This step reproduces Our World in Data's 2017 compilation "Historical employment and
output by sector" from its original sources. The base is Herrendorf, Rogerson and
Valentinyi (2014), updated country by country with the GGDC 10-Sector Database (January
2015 release) and the Swedish Historical National Accounts, following the recipe in the
2017 documentation:
https://assets.ourworldindata.org/uploads/2017/10/Documentation-for-Historical-employment-and-output-by-sector-%E2%80%93-OWID-2017.pdf

Unlike the 2017 compilation, US data is not updated with Bureau of Economic Analysis
releases: those updates only affected years that are superseded by World Bank data in
the combined dataset built downstream (see structural_transformation).

Sectors follow the convention of Herrendorf, Rogerson and Valentinyi (2014): utilities
are classified within services, not industry.
"""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SECTORS = ["agriculture", "industry", "services"]
EMPLOYMENT_COLUMNS = [f"number_employed_{sector}" for sector in SECTORS]
SHARE_COLUMNS = [f"share_gdp_{sector}" for sector in SECTORS]

# Compilation recipe, following the 2017 documentation. The base source is Herrendorf,
# Rogerson and Valentinyi (2014). "va" and "emp" list (first_year, last_year) spans that
# are replaced with the GGDC 10-Sector Database; last_year None means "onwards".
# "va_drop_years" / "emp_drop_years" remove observations:
#   - Finland: negative value added for services in 1917-1920 and 1945-1946.
#   - France: apparent typo in the 1937 value added data.
#   - United States: no services or industry employment in 1800-1830.
RECIPE = {
    "Belgium": {},
    "Spain": {"va": [(1970, None)]},
    "Finland": {"va_drop_years": [1917, 1918, 1919, 1920, 1945, 1946]},
    "France": {"va": [(1970, None)], "va_drop_years": [1937]},
    "Japan": {"va": [(1953, None)], "emp": [(1953, None)]},
    # NOTE: The 2017 documentation says South Korean value added was updated with GGDC from
    # 1953 onwards, but the published 2017 values match Herrendorf et al. through 1962 and
    # GGDC only from 1963 (the same year as the employment switch), so 1963 is used here.
    "South Korea": {"va": [(1963, None)], "emp": [(1963, None)]},
    "Netherlands": {"va": [(1970, None)], "emp": [(1949, 1949), (1960, None)]},
    "Sweden": {"replace_all": True},
    "United Kingdom": {"va": [(1960, None)], "emp": [(1948, None)]},
    "United States": {"emp_drop_years": [1800, 1810, 1820, 1830]},
}

# Countries to keep from the GGDC 10-Sector Database (before harmonization).
GGDC_COUNTRIES = ["ESP", "FRA", "GBR", "JPN", "KOR", "NLD"]

# Sheets of the Herrendorf, Rogerson and Valentinyi data with employment expressed in
# persons; all other sheets are expressed in thousands of persons.
HRV_EMPLOYMENT_IN_PERSONS = ["Netherlands", "Sweden"]

# Aggregation of the ten GGDC sectors into the three broad sectors, following the
# convention of Herrendorf, Rogerson and Valentinyi (2014): utilities in services.
GGDC_BUCKETS = {
    "agriculture": ["agriculture"],
    "industry": ["mining", "manufacturing", "construction"],
    "services": [
        "utilities",
        "trade_restaurants_hotels",
        "transport_communication",
        "finance_business_services",
        "government_services",
        "community_services",
    ],
}

# Aggregation of the SHNA main sectors into the three broad sectors. The SHNA does not
# separate utilities from manufacturing industry, so they remain within industry.
LUND_VA_BUCKETS = {
    "agriculture": ["agriculture"],
    "industry": ["manufacturing_industry", "building_construction"],
    "services": ["transport_communication", "private_services", "public_services", "services_of_dwellings"],
}
LUND_EMP_BUCKETS = {
    "agriculture": ["agriculture"],
    "industry": ["manufacturing_industry", "building_construction"],
    "services": ["transport_communication", "private_services", "public_services"],
}


def run() -> None:
    #
    # Load inputs.
    #
    ds_hrv = paths.load_dataset("herrendorf_rogerson_valentinyi")
    ds_ggdc = paths.load_dataset("ggdc_10_sector")
    ds_lund = paths.load_dataset("swedish_historical_national_accounts")

    tb_hrv = ds_hrv.read("herrendorf_rogerson_valentinyi")
    tb_ggdc = ds_ggdc.read("ggdc_10_sector")
    tb_lund_va = ds_lund.read("value_added")
    tb_lund_emp = ds_lund.read("employment")

    #
    # Process data.
    #
    # Keep only the countries needed from the GGDC 10-Sector Database and harmonize names.
    tb_ggdc = tb_ggdc[tb_ggdc["country"].isin(GGDC_COUNTRIES)].reset_index(drop=True)
    tb_hrv = paths.regions.harmonize_names(tb=tb_hrv)
    tb_ggdc = paths.regions.harmonize_names(tb=tb_ggdc)

    sanity_check_inputs(tb_hrv=tb_hrv, tb_ggdc=tb_ggdc, tb_lund_va=tb_lund_va, tb_lund_emp=tb_lund_emp)

    tb_hrv = prepare_hrv(tb_hrv)
    tb_ggdc = prepare_ggdc(tb_ggdc)
    tb_lund = prepare_lund(tb_lund_va, tb_lund_emp)

    tb = apply_recipe(tb_hrv=tb_hrv, tb_ggdc=tb_ggdc, tb_lund=tb_lund)

    # Round employment to whole persons.
    tb[EMPLOYMENT_COLUMNS] = tb[EMPLOYMENT_COLUMNS].round()

    # Each output column combines the three sources, so assign the union of origins.
    origins = []
    for tb_source in [tb_hrv, tb_ggdc, tb_lund]:
        for origin in tb_source["number_employed_agriculture"].m.origins:
            if origin not in origins:
                origins.append(origin)
    for column in EMPLOYMENT_COLUMNS + SHARE_COLUMNS:
        tb[column].metadata.origins = origins

    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_hrv.metadata)
    ds_garden.save()


def prepare_hrv(tb: Table) -> Table:
    """Convert the Herrendorf, Rogerson and Valentinyi data to the output schema."""
    tb = tb.copy()

    # Employment to persons: most sheets are in thousands, some are already in persons.
    persons_mask = tb["country"].isin(HRV_EMPLOYMENT_IN_PERSONS)
    for sector in SECTORS:
        tb[f"number_employed_{sector}"] = tb[f"emp_{sector}"] * 1000
        tb.loc[persons_mask, f"number_employed_{sector}"] = tb.loc[persons_mask, f"emp_{sector}"]

    # Value added shares of the total value added of the three sectors, at current prices.
    va_total = tb["va_agriculture"] + tb["va_industry"] + tb["va_services"]
    for sector in SECTORS:
        tb[f"share_gdp_{sector}"] = tb[f"va_{sector}"] / va_total * 100

    return tb[["country", "year"] + EMPLOYMENT_COLUMNS + SHARE_COLUMNS]


def prepare_ggdc(tb: Table) -> Table:
    """Aggregate the ten GGDC sectors into the three broad sectors and compute shares."""
    tb_emp = tb[tb["variable"] == "EMP"].copy()
    tb_va = tb[tb["variable"] == "VA"].copy()

    # Employment: thousands of persons engaged, aggregated into buckets.
    for bucket, columns in GGDC_BUCKETS.items():
        tb_emp[f"number_employed_{bucket}"] = aggregate_ggdc_bucket(tb_emp, columns) * 1000

    # Value added at current national prices, aggregated into buckets.
    for bucket, columns in GGDC_BUCKETS.items():
        tb_va[f"va_{bucket}"] = aggregate_ggdc_bucket(tb_va, columns)

    # The sum of the three buckets should reconcile with the reported summation of sector GDP,
    # which also guards against sectors genuinely missing from the bucket sums above.
    va_sum = tb_va["va_agriculture"] + tb_va["va_industry"] + tb_va["va_services"]
    reported = tb_va["total"]
    comparable = va_sum.notna() & reported.notna()
    error = "GGDC sector value added does not reconcile with the reported summation of sector GDP."
    assert ((va_sum[comparable] - reported[comparable]).abs() / reported[comparable] < 0.005).all(), error

    # Value added shares of the total value added of the three sectors, at current prices.
    for sector in SECTORS:
        tb_va[f"share_gdp_{sector}"] = tb_va[f"va_{sector}"] / va_sum * 100

    tb = pr.merge(
        tb_emp[["country", "year"] + EMPLOYMENT_COLUMNS],
        tb_va[["country", "year"] + SHARE_COLUMNS],
        on=["country", "year"],
        how="outer",
    )

    return tb


def aggregate_ggdc_bucket(tb: Table, columns: list) -> Table:
    """Sum GGDC sector columns into a bucket, per country.

    Some sectors are structurally absent for a country (entirely missing because they are
    merged into another sector, e.g. government services within community services for
    South Korea). Those are excluded from the requirement that all sectors be present,
    so that a bucket is only missing when a genuinely reported sector is missing.
    """
    result = tb["year"] * float("nan")
    for country, group in tb.groupby("country", observed=True):
        usable = [column for column in columns if not group[column].isna().all()]
        if usable:
            result.loc[group.index] = group[usable].sum(axis=1, min_count=len(usable))
    return result


def prepare_lund(tb_va: Table, tb_emp: Table) -> Table:
    """Aggregate the SHNA main sectors into the three broad sectors and compute shares."""
    tb_va = tb_va.copy()
    tb_emp = tb_emp.copy()

    # Employment in persons, aggregated into buckets.
    for bucket, columns in LUND_EMP_BUCKETS.items():
        tb_emp[f"number_employed_{bucket}"] = tb_emp[columns].sum(axis=1, min_count=len(columns))

    # The sum of the sectors should reconcile with the reported total employment.
    emp_columns = [column for bucket in LUND_EMP_BUCKETS.values() for column in bucket]
    emp_sum = tb_emp[emp_columns].sum(axis=1, min_count=len(emp_columns))
    comparable = emp_sum.notna() & tb_emp["total"].notna()
    error = "SHNA sector employment does not reconcile with the reported total employment."
    assert ((emp_sum[comparable] - tb_emp["total"][comparable]).abs() / tb_emp["total"][comparable] < 0.005).all(), (
        error
    )

    # Value added at current prices, aggregated into buckets.
    for bucket, columns in LUND_VA_BUCKETS.items():
        tb_va[f"va_{bucket}"] = tb_va[columns].sum(axis=1, min_count=len(columns))

    # The sum of the sectors should reconcile with the reported GDP at factor prices.
    va_columns = [column for bucket in LUND_VA_BUCKETS.values() for column in bucket]
    va_sum = tb_va[va_columns].sum(axis=1, min_count=len(va_columns))
    comparable = va_sum.notna() & tb_va["gdp"].notna()
    error = "SHNA sector value added does not reconcile with the reported GDP at factor prices."
    assert ((va_sum[comparable] - tb_va["gdp"][comparable]).abs() / tb_va["gdp"][comparable] < 0.01).all(), error

    # Value added shares of the total value added of the three sectors, at current prices.
    va_total = tb_va["va_agriculture"] + tb_va["va_industry"] + tb_va["va_services"]
    for sector in SECTORS:
        tb_va[f"share_gdp_{sector}"] = tb_va[f"va_{sector}"] / va_total * 100

    tb = pr.merge(
        tb_emp[["country", "year"] + EMPLOYMENT_COLUMNS],
        tb_va[["country", "year"] + SHARE_COLUMNS],
        on=["country", "year"],
        how="outer",
    )

    return tb


def apply_recipe(tb_hrv: Table, tb_ggdc: Table, tb_lund: Table) -> Table:
    """Apply the 2017 compilation recipe country by country."""
    tables = []
    for country, rules in RECIPE.items():
        if rules.get("replace_all"):
            tb_country = tb_lund[tb_lund["country"] == country].copy()
        else:
            tb_country = tb_hrv[tb_hrv["country"] == country].copy()
            tb_country = apply_overrides(tb_country, tb_ggdc, country, rules.get("emp", []), EMPLOYMENT_COLUMNS)
            tb_country = apply_overrides(tb_country, tb_ggdc, country, rules.get("va", []), SHARE_COLUMNS)
            for year in rules.get("va_drop_years", []):
                tb_country.loc[tb_country["year"] == year, SHARE_COLUMNS] = float("nan")
            for year in rules.get("emp_drop_years", []):
                tb_country.loc[tb_country["year"] == year, EMPLOYMENT_COLUMNS] = float("nan")
        tables.append(tb_country)

    tb = pr.concat(tables, ignore_index=True)

    # Drop rows without any data left.
    tb = tb.dropna(subset=EMPLOYMENT_COLUMNS + SHARE_COLUMNS, how="all")

    return tb


def apply_overrides(tb_country: Table, tb_override: Table, country: str, spans: list, columns: list) -> Table:
    """Replace the values of `columns` in the given year spans with the override source."""
    for first_year, last_year in spans:
        year_mask = tb_country["year"] >= first_year
        override_mask = tb_override["year"] >= first_year
        if last_year is not None:
            year_mask &= tb_country["year"] <= last_year
            override_mask &= tb_override["year"] <= last_year

        # Wipe the base values in the span; the span is defined by the override source.
        tb_country.loc[year_mask, columns] = float("nan")

        # Bring in the override values, adding rows for years beyond the base coverage.
        tb_span = tb_override[(tb_override["country"] == country) & override_mask][["year"] + columns]
        tb_country = pr.merge(tb_country, tb_span, on="year", how="outer", suffixes=("", "_override"))
        for column in columns:
            tb_country[column] = tb_country[f"{column}_override"].combine_first(tb_country[column])
        tb_country = tb_country.drop(columns=[f"{column}_override" for column in columns])
        tb_country["country"] = country

    return tb_country


def sanity_check_inputs(tb_hrv: Table, tb_ggdc: Table, tb_lund_va: Table, tb_lund_emp: Table) -> None:
    error = "Herrendorf, Rogerson and Valentinyi data does not contain the expected countries."
    assert set(tb_hrv["country"]) == set(RECIPE), error

    error = "GGDC 10-Sector data does not contain the expected countries after filtering."
    assert set(tb_ggdc["country"]) == {"France", "Japan", "Netherlands", "South Korea", "Spain", "United Kingdom"}, (
        error
    )

    error = "Duplicate (country, year) rows in Herrendorf, Rogerson and Valentinyi data."
    assert not tb_hrv.duplicated(subset=["country", "year"]).any(), error

    error = "Duplicate (country, variable, year) rows in GGDC 10-Sector data."
    assert not tb_ggdc.duplicated(subset=["country", "variable", "year"]).any(), error

    error = "SHNA tables should only contain Sweden."
    assert set(tb_lund_va["country"]) == set(tb_lund_emp["country"]) == {"Sweden"}, error


def sanity_check_outputs(tb: Table) -> None:
    error = "Expected exactly the ten compilation countries."
    assert set(tb["country"]) == set(RECIPE), error

    error = "Duplicate (country, year) rows in the compilation."
    assert not tb.duplicated(subset=["country", "year"]).any(), error

    error = "Value added shares must sum to 100."
    share_sum = tb[SHARE_COLUMNS].dropna().sum(axis=1)
    assert ((share_sum - 100).abs() < 0.1).all(), error

    error = "Negative or zero employment found."
    for column in EMPLOYMENT_COLUMNS:
        assert (tb[column].dropna() > 0).all(), error

    error = "Finland value added observations with negative services (1917-1920, 1945-1946) must be dropped."
    dropped = tb[(tb["country"] == "Finland") & (tb["year"].isin([1917, 1918, 1919, 1920, 1945, 1946]))]
    assert dropped[SHARE_COLUMNS].isna().all().all(), error

    error = "France 1937 value added observation (typo in the original data) must be dropped."
    assert tb[(tb["country"] == "France") & (tb["year"] == 1937)][SHARE_COLUMNS].isna().all().all(), error

    error = "United States employment in 1800-1830 must be dropped."
    dropped = tb[(tb["country"] == "United States") & (tb["year"] <= 1830)]
    assert dropped[EMPLOYMENT_COLUMNS].isna().all().all(), error

    # Spot checks against the frozen 2017 compilation (grapher dataset "Historical
    # employment and output by sector - OWID (2017)"), so recipe regressions fail loudly.
    spot_checks = [
        ("Belgium", 1846, "number_employed_agriculture", 681000, 1),
        ("United Kingdom", 1801, "number_employed_agriculture", 1426000, 1),
        ("Japan", 1953, "number_employed_agriculture", 17081689, 2),
        ("South Korea", 1963, "share_gdp_agriculture", 41.41, 0.1),
    ]
    for country, year, column, expected, tolerance in spot_checks:
        value = tb.loc[(tb["country"] == country) & (tb["year"] == year), column].iloc[0]
        error = f"Spot check failed: {country} {year} {column} = {value}, expected {expected}."
        assert abs(value - expected) <= tolerance, error
