"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Pension entitlements in the closing balance sheet: the stock at the end of the year. The table's other items are
# the flows that change it during the year (contributions, benefits, revaluations), which are not used here.
STOCK_ITEM = "F63_LE"

# Schemes Eurostat publishes as a share of GDP, and the column each becomes.
SCHEMES_AS_SHARE_OF_GDP = {
    # All schemes, in and outside the core national accounts (column I of the ESA 2010 table 29).
    "S1P": "total_share_gdp",
    # Government schemes outside the core accounts: social security and government employer schemes (G + H).
    "S13PU": "unfunded_share_gdp",
    # Schemes in the core accounts: private schemes and the government schemes recorded there (C + D + E + F).
    "S1PF": "private_and_funded_share_gdp",
}
# Schemes Eurostat publishes only in currency units, and the column each becomes once expressed as a share of GDP.
SCHEMES_IN_CURRENCY = {
    # H: social security pension schemes.
    "S13PS": "social_security_share_gdp",
    # G: defined benefit schemes for government employees, classified in general government, outside the core accounts.
    "S13PBX": "government_employee_defined_benefit_outside_core_share_gdp",
    # F: defined benefit schemes for government employees, classified in general government, in the core accounts.
    "S13PBI": "government_employee_defined_benefit_in_core_share_gdp",
    # E: defined benefit schemes for government employees, classified in financial corporations.
    "S12PBI": "government_employee_defined_benefit_financial_corporations_share_gdp",
    # D: defined contribution schemes of general government.
    "S13PC": "government_defined_contribution_share_gdp",
    # C = A + B: private pension schemes.
    "S12P": "private_share_gdp",
    # B: private defined benefit schemes.
    "S12PB": "private_defined_benefit_share_gdp",
    # A: private defined contribution schemes.
    "S12PC": "private_defined_contribution_share_gdp",
}
# The parts that make up each aggregate, used to check the data adds up.
PARTS = {
    "unfunded_share_gdp": ["social_security_share_gdp", "government_employee_defined_benefit_outside_core_share_gdp"],
    "private_and_funded_share_gdp": [
        "private_share_gdp",
        "government_defined_contribution_share_gdp",
        "government_employee_defined_benefit_financial_corporations_share_gdp",
        "government_employee_defined_benefit_in_core_share_gdp",
    ],
    "private_share_gdp": ["private_defined_benefit_share_gdp", "private_defined_contribution_share_gdp"],
}
# Eurostat rounds the shares of GDP it publishes to whole percentages, so sums of shares can miss by a little.
TOLERANCE_PERCENTAGE_POINTS = 1.5

# Number of countries in the table: EU member states, the UK (for the years it reported) and EFTA countries except
# Liechtenstein.
# NOTE: Check at each update whether countries were added or dropped.
N_COUNTRIES = 31


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("pension_entitlements")
    tb = ds_meadow.read("pension_entitlements")

    #
    # Process data.
    #
    # Step 1: Keep the stock of pension entitlements at the end of each year.
    tb = tb[tb["na_item"] == STOCK_ITEM].drop(columns=["na_item"]).reset_index(drop=True)
    sanity_check_inputs(tb)

    # Step 2: Take the shares of GDP Eurostat publishes, and keep their flags for the chart notes.
    tb_published = tb[(tb["unit"] == "PC_GDP") & (tb["penscheme"].isin(SCHEMES_AS_SHARE_OF_GDP))]
    tb_flags = create_flags_table(tb_published)
    tb_shares = tb_published.pivot(index=["country", "year"], columns="penscheme", values="value").rename(
        columns=SCHEMES_AS_SHARE_OF_GDP, errors="raise"
    )

    # Step 3: Express the finer schemes, published only in national currency, as a share of GDP. GDP is not in the
    # table, so it is implied by the total in national currency and the total as a share of GDP. That keeps the
    # finer schemes consistent with the published shares, instead of mixing in GDP figures from another release.
    tb_currency = tb[tb["unit"] == "MIO_NAC"].pivot(index=["country", "year"], columns="penscheme", values="value")
    gdp = tb_currency["S1P"] / tb_shares["total_share_gdp"] * 100
    for scheme, column in SCHEMES_IN_CURRENCY.items():
        tb_shares[column] = tb_currency[scheme] / gdp * 100

    # Step 4: Drop the country-years with no data at all.
    tb_shares = tb_shares.dropna(how="all").reset_index()

    # Step 5: Harmonize country names.
    tb_shares = paths.regions.harmonize_names(tb=tb_shares)
    tb_flags = paths.regions.harmonize_names(tb=tb_flags, warn_on_unused_countries=False)

    # Step 6: Improve tables format.
    sanity_check_outputs(tb_shares)
    tb_shares = tb_shares.format(["country", "year"], short_name=paths.short_name)
    tb_flags = tb_flags.format(["country", "year", "indicator"], short_name="pension_entitlements_flags")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_shares, tb_flags], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def create_flags_table(tb_published: Table) -> Table:
    """Eurostat's flags on the published shares of GDP: one row per flagged value.

    Eurostat marks values that are, for example, provisional ("p"), estimated ("e"), defined differently from the
    standard ("d") or after a break in the series ("b"). Values flagged "m" (missing) have no value, so they are left
    out.
    """
    tb_flags = tb_published[tb_published["flag"].notna() & tb_published["value"].notna()][
        ["country", "year", "penscheme", "flag"]
    ].copy()
    tb_flags["indicator"] = tb_flags["penscheme"].astype(str).map(SCHEMES_AS_SHARE_OF_GDP)
    tb_flags["flag"] = tb_flags["flag"].astype(str)
    return tb_flags.drop(columns=["penscheme"])


def sanity_check_inputs(tb: Table) -> None:
    expected_schemes = set(SCHEMES_AS_SHARE_OF_GDP) | set(SCHEMES_IN_CURRENCY)
    missing = expected_schemes - set(tb["penscheme"])
    assert not missing, f"Schemes missing from the table: {missing}"
    assert {"PC_GDP", "MIO_NAC"} <= set(tb["unit"]), f"Units missing from the table: {set(tb['unit'])}"
    assert tb["country"].nunique() == N_COUNTRIES, f"Expected {N_COUNTRIES} countries, got {tb['country'].nunique()}."
    assert (tb["value"].dropna() >= 0).all(), "Negative pension entitlements."

    # The total in currency units is exactly the sum of the six columns of the table (C to H).
    currency = tb[tb["unit"] == "MIO_NAC"].pivot(index=["country", "year"], columns="penscheme", values="value")
    columns_c_to_h = ["S12P", "S13PC", "S12PBI", "S13PBI", "S13PBX", "S13PS"]
    gap = (currency["S1P"] - currency[columns_c_to_h].sum(axis=1)).abs() / currency["S1P"]
    assert (gap.dropna() < 1e-3).all(), f"Total does not equal the sum of its schemes: {gap[gap >= 1e-3].index.tolist()}"


def sanity_check_outputs(tb: Table) -> None:
    columns = list(SCHEMES_AS_SHARE_OF_GDP.values()) + list(SCHEMES_IN_CURRENCY.values())
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    assert (tb[columns].min() >= 0).all(), "Negative share of GDP."
    # The lowest published total is Denmark's (72%) and the highest Spain's (501%).
    assert tb["total_share_gdp"].dropna().between(30, 800).all(), "Total entitlements outside the expected range."

    # Every aggregate equals the sum of its parts, within the rounding of the published shares.
    for aggregate, parts in PARTS.items():
        rows = tb[[aggregate] + parts].dropna()
        gap = (rows[aggregate] - rows[parts].sum(axis=1)).abs()
        assert len(rows) > 0, f"No country-year has {aggregate} and all its parts."
        assert (gap <= TOLERANCE_PERCENTAGE_POINTS).all(), f"{aggregate} does not equal the sum of its parts."
    rows = tb[["total_share_gdp", "unfunded_share_gdp", "private_and_funded_share_gdp"]].dropna()
    gap = (rows["total_share_gdp"] - rows["unfunded_share_gdp"] - rows["private_and_funded_share_gdp"]).abs()
    assert (gap <= TOLERANCE_PERCENTAGE_POINTS).all(), "Total does not equal unfunded plus private and funded."

    # Soft signal: a country whose total is missing while its two parts are published.
    only_parts = tb[tb["total_share_gdp"].isna() & tb["unfunded_share_gdp"].notna()]
    if len(only_parts):
        log.warning(
            "Total missing where its parts are published",
            rows=list(zip(only_parts["country"], only_parts["year"])),
        )
