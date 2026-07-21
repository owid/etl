"""Long-run employment and GDP shares by sector for ten countries: Belgium, Finland, France, Japan, the Netherlands, South Korea, Spain, Sweden, the United Kingdom and the United States.

This step combines several sources into long-run series of employment numbers and value
added shares by broad economic sector. The base is the dataset published by Herrendorf,
Rogerson and Valentinyi (2014), updated country by country with the GGDC 10-Sector
Database (January 2015 release), the Swedish Historical National Accounts (value added
only; see the NOTE in the recipe below), and BEA's GDP by industry statistics for the
United States (the historical statistics for 1947-1997 and the current accounts from 1998
onwards). The country-by-country combination follows the methodology described in
https://assets.ourworldindata.org/uploads/2017/10/Documentation-for-Historical-employment-and-output-by-sector-%E2%80%93-OWID-2017.pdf

Employment years from 1991 onwards are superseded by World Bank data in the combined
dataset built downstream (see structural_transformation_omm).

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

# Country-by-country combination recipe (see the methodology documentation linked in the
# module docstring). The base source is Herrendorf, Rogerson and Valentinyi (2014).
# "va" and "emp" list (source, first_year, last_year)
# spans that are replaced with another source; last_year None means "onwards".
# "va_drop_years" / "emp_drop_years" remove observations:
#   - Finland: negative value added for services in 1917-1920 and 1945-1946.
#   - France: apparent typo in the 1937 value added data.
#   - United States: no services or industry employment in 1800-1830.
RECIPE = {
    "Belgium": {},
    "Spain": {"va": [("ggdc", 1970, None)]},
    "Finland": {"va_drop_years": [1917, 1918, 1919, 1920, 1945, 1946]},
    "France": {"va": [("ggdc", 1970, None)], "va_drop_years": [1937]},
    "Japan": {"va": [("ggdc", 1953, None)], "emp": [("ggdc", 1953, None)]},
    # NOTE: The methodology documentation says South Korean value added switches to GGDC in
    # 1953, but the previously published values match Herrendorf et al. through 1962 and
    # GGDC only from 1963 (the same year as the employment switch), so 1963 is used here.
    "South Korea": {"va": [("ggdc", 1963, None)], "emp": [("ggdc", 1963, None)]},
    "Netherlands": {"va": [("ggdc", 1970, None)], "emp": [("ggdc", 1949, 1949), ("ggdc", 1960, None)]},
    # NOTE: The methodology documentation replaces all of Sweden with the Swedish Historical
    # National Accounts. The previously published employment values correspond to the Krantz
    # and Schön (2007) series embedded in Herrendorf et al. (the sources documented at
    # https://ourworldindata.org/agri-employment-sources), and the current SHNA release
    # revised historical employment levels upwards by 10-30% (rebased to a Statistics Sweden
    # benchmark in 2023), so the SHNA replaces value added only and employment stays with the
    # base source.
    "Sweden": {"va": [("lund", 1800, None)]},
    "United Kingdom": {"va": [("ggdc", 1960, None)], "emp": [("ggdc", 1948, None)]},
    "United States": {"va": [("bea", 1947, None)], "emp_drop_years": [1800, 1810, 1820, 1830]},
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

# Aggregation of the BEA (NAICS-based) industries into the three broad sectors, following
# the convention of Herrendorf, Rogerson and Valentinyi (2014): utilities in services.
BEA_BUCKETS = {
    "agriculture": ["Agriculture, forestry, fishing, and hunting"],
    "industry": ["Mining", "Construction", "Manufacturing"],
    "services": [
        "Utilities",
        "Wholesale trade",
        "Retail trade",
        "Transportation and warehousing",
        "Information",
        "Finance, insurance, real estate, rental, and leasing",
        "Professional and business services",
        "Educational services, health care, and social assistance",
        "Arts, entertainment, recreation, accommodation, and food services",
        "Other services, except government",
        "Government",
    ],
}


def run() -> None:
    #
    # Load inputs.
    #
    ds_hrv = paths.load_dataset("herrendorf_rogerson_valentinyi")
    ds_ggdc = paths.load_dataset("ggdc_10_sector")
    ds_lund = paths.load_dataset("swedish_historical_national_accounts")
    ds_bea = paths.load_dataset("gdp_by_industry_historical")
    ds_bea_current = paths.load_dataset("gdp_by_industry")

    tb_hrv = ds_hrv.read("herrendorf_rogerson_valentinyi")
    tb_ggdc = ds_ggdc.read("ggdc_10_sector")
    tb_lund_va = ds_lund.read("value_added")
    tb_lund_emp = ds_lund.read("employment")
    tb_bea = ds_bea.read("gdp_by_industry_historical")
    tb_bea_current = ds_bea_current.read("gdp_by_industry")

    #
    # Process data.
    #
    # Keep only the countries needed from the GGDC 10-Sector Database and harmonize names.
    tb_ggdc = tb_ggdc[tb_ggdc["country"].isin(GGDC_COUNTRIES)].reset_index(drop=True)
    tb_hrv = paths.regions.harmonize_names(tb=tb_hrv)
    tb_ggdc = paths.regions.harmonize_names(tb=tb_ggdc)

    sanity_check_inputs(
        tb_hrv=tb_hrv,
        tb_ggdc=tb_ggdc,
        tb_lund_va=tb_lund_va,
        tb_lund_emp=tb_lund_emp,
        tb_bea=tb_bea,
        tb_bea_current=tb_bea_current,
    )

    tb_hrv = prepare_hrv(tb_hrv)
    tb_ggdc = prepare_ggdc(tb_ggdc)
    tb_lund = prepare_lund(tb_lund_va, tb_lund_emp)

    # BEA: the historical statistics through 1997, the current accounts from 1998 onwards.
    tb_bea = pr.concat([prepare_bea(tb_bea), prepare_bea(tb_bea_current).query("year >= 1998")], ignore_index=True)
    sanity_check_bea_vintage_seam(tb_bea)

    # Route every input through the harmonization mapping, for transparency (the Lund and
    # BEA labels are identity mappings).
    tb_lund = paths.regions.harmonize_names(tb=tb_lund)
    tb_bea = paths.regions.harmonize_names(tb=tb_bea)

    tb = apply_recipe(tb_hrv=tb_hrv, overrides={"ggdc": tb_ggdc, "bea": tb_bea, "lund": tb_lund})

    # Round employment to whole persons.
    tb[EMPLOYMENT_COLUMNS] = tb[EMPLOYMENT_COLUMNS].round()

    # Each output column combines several sources, so assign the union of origins.
    employment_origins = union_source_origins(
        [
            tb_hrv["number_employed_agriculture"],
            tb_ggdc["number_employed_agriculture"],
        ]
    )
    share_origins = union_source_origins(
        [
            tb_hrv["share_gdp_agriculture"],
            tb_ggdc["share_gdp_agriculture"],
            tb_lund["share_gdp_agriculture"],
            tb_bea["share_gdp_agriculture"],
        ]
    )
    for column in EMPLOYMENT_COLUMNS:
        tb[column].metadata.origins = employment_origins
    for column in SHARE_COLUMNS:
        tb[column].metadata.origins = share_origins

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


def prepare_bea(tb: Table) -> Table:
    """Aggregate the BEA industries into the three broad sectors and compute shares."""
    tb = tb.pivot(index="year", columns="industry", values="value_added").reset_index()

    # Value added at current prices (millions of dollars), aggregated into buckets.
    for bucket, industries in BEA_BUCKETS.items():
        tb[f"va_{bucket}"] = tb[industries].sum(axis=1, min_count=len(industries))

    # The sum of the three buckets covers every industry, so it should reconcile with GDP.
    va_sum = tb["va_agriculture"] + tb["va_industry"] + tb["va_services"]
    error = "BEA sector value added does not reconcile with the reported gross domestic product."
    assert ((va_sum - tb["Gross domestic product"]).abs() / tb["Gross domestic product"] < 0.001).all(), error

    # Value added shares of the total value added of the three sectors, at current prices.
    for sector in SECTORS:
        tb[f"share_gdp_{sector}"] = tb[f"va_{sector}"] / va_sum * 100

    tb["country"] = "United States"

    return tb[["country", "year"] + SHARE_COLUMNS]


def apply_recipe(tb_hrv: Table, overrides: dict) -> Table:
    """Apply the country-by-country combination recipe."""
    tables = []
    for country, rules in RECIPE.items():
        tb_country = tb_hrv[tb_hrv["country"] == country].copy()
        tb_country = apply_overrides(tb_country, overrides, country, rules.get("emp", []), EMPLOYMENT_COLUMNS)
        tb_country = apply_overrides(tb_country, overrides, country, rules.get("va", []), SHARE_COLUMNS)
        for year in rules.get("va_drop_years", []):
            tb_country.loc[tb_country["year"] == year, SHARE_COLUMNS] = float("nan")
        for year in rules.get("emp_drop_years", []):
            tb_country.loc[tb_country["year"] == year, EMPLOYMENT_COLUMNS] = float("nan")
        tables.append(tb_country)

    tb = pr.concat(tables, ignore_index=True)

    # Drop rows without any data left.
    tb = tb.dropna(subset=EMPLOYMENT_COLUMNS + SHARE_COLUMNS, how="all")

    return tb


def apply_overrides(tb_country: Table, overrides: dict, country: str, spans: list, columns: list) -> Table:
    """Replace the values of `columns` in the given year spans with the override source."""
    for source, first_year, last_year in spans:
        tb_override = overrides[source]
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


def sanity_check_bea_vintage_seam(tb_bea: Table) -> None:
    """The BEA historical statistics (through 1997) and current accounts (1998 onwards) are
    different vintages of the same accounts; the seam between them should be small."""
    v1997 = tb_bea[tb_bea["year"] == 1997].iloc[0]
    v1998 = tb_bea[tb_bea["year"] == 1998].iloc[0]
    for column in SHARE_COLUMNS:
        error = f"Large break between the BEA vintages at 1997/1998 in {column}."
        assert abs(v1998[column] - v1997[column]) < 5, error


def union_source_origins(variables: list) -> list:
    """Union of the origins of several variables, preserving order."""
    origins = []
    for variable in variables:
        for origin in variable.m.origins:
            if origin not in origins:
                origins.append(origin)
    return origins


def sanity_check_inputs(
    tb_hrv: Table, tb_ggdc: Table, tb_lund_va: Table, tb_lund_emp: Table, tb_bea: Table, tb_bea_current: Table
) -> None:
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

    bea_industries = [industry for bucket in BEA_BUCKETS.values() for industry in bucket]
    error = "BEA historical data does not contain the expected industries."
    assert set(bea_industries) <= set(tb_bea["industry"]), error

    error = "BEA historical data does not cover the expected years."
    assert tb_bea["year"].min() == 1947 and tb_bea["year"].max() == 1997, error

    error = "BEA current accounts data does not contain the expected industries."
    assert set(bea_industries) <= set(tb_bea_current["industry"]), error

    error = "BEA current accounts data does not cover the expected years."
    assert tb_bea_current["year"].min() == 1997 and tb_bea_current["year"].max() >= 2024, error


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

    # Spot checks against the previously published version of this data (grapher dataset
    # "Historical employment and output by sector - OWID (2017)"), so recipe regressions
    # fail loudly.
    spot_checks = [
        ("Belgium", 1846, "number_employed_agriculture", 681000, 1),
        ("United Kingdom", 1801, "number_employed_agriculture", 1426000, 1),
        ("Japan", 1953, "number_employed_agriculture", 17081689, 2),
        # Sweden employment stays with the Krantz and Schön (2007) series embedded in
        # Herrendorf et al. (see the NOTE in the recipe).
        ("Sweden", 1900, "number_employed_agriculture", 1089129, 1),
        ("South Korea", 1963, "share_gdp_agriculture", 41.41, 0.1),
        # BEA-sourced (the frozen value is 6.6, rounded to one decimal in the BEA source).
        ("United States", 1950, "share_gdp_agriculture", 6.6, 0.15),
    ]
    for country, year, column, expected, tolerance in spot_checks:
        value = tb.loc[(tb["country"] == country) & (tb["year"] == year), column].iloc[0]
        error = f"Spot check failed: {country} {year} {column} = {value}, expected {expected}."
        assert abs(value - expected) <= tolerance, error
