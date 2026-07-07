"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

INDICATOR_NAMES = {
    "Percentage of GDP": "share_gdp",
    "Percentage of expenditure on health": "share_expenditure",
    "US dollars per person, PPP converted": "ppp_dollars_per_capita",
    "US dollars, PPP converted": "ppp_dollars",
}

# Financing schemes expected from the source (ICHA-HF classification, plus aggregates).
EXPECTED_FINANCING_SCHEMES = {
    "Total",
    "Government/compulsory schemes",
    "Government schemes",
    "Compulsory contributory health insurance schemes",
    "Social health insurance schemes",
    "Compulsory private insurance schemes",
    "Voluntary schemes/household out-of-pocket payments",
    "Voluntary healthcare payment schemes",
    "Voluntary health insurance schemes",
    "NPISH financing schemes",
    "Enterprise financing schemes",
    "Household out-of-pocket payments",
    "Out-of-pocket excluding cost-sharing",
    "Cost-sharing with third-party payers",
    "Rest of the world financing schemes (non-resident)",
    "Unknown",
}

# Coverage floor: the 2026-07-06 release has 61 reference areas. A drop usually means a
# parsing or mapping regression, not a real change — re-audit before lowering this.
MIN_COUNTRIES = 61

# NOTE: Base year of the constant-price PPP series. If the OECD rebases, update this
# constant AND the `ppp_year` definition in health_expenditure.meta.yml together.
PPP_YEAR = 2020

# Indicators expressed in constant PPP dollars (the ones the base year applies to).
PPP_INDICATORS = ["US dollars, PPP converted", "US dollars per person, PPP converted"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("health_expenditure")

    # Read table from meadow dataset.
    tb = ds_meadow.read("health_expenditure")

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)

    # Make the indicators wide
    tb = make_indicators_wide(tb, INDICATOR_NAMES)

    # Transform health expenditure, saved originally in millions of dollars
    tb["ppp_dollars"] *= 1e6

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year", "financing_scheme"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_indicators_wide(tb: Table, indicator_names: dict[str, str]) -> Table:
    """
    This function makes the indicators wide
    """
    # Rename the indicators
    tb["indicator"] = tb["indicator"].map(indicator_names)

    # Make the indicators wide
    tb = tb.pivot(
        index=["country", "year", "financing_scheme"],
        columns="indicator",
        values="value",
    ).reset_index()

    # Make share_expenditure null when financing_scheme is "Total"
    tb.loc[tb["financing_scheme"] == "Total", "share_expenditure"] = None

    return tb


def sanity_check_inputs(tb: Table) -> None:
    """
    Check assumptions about the meadow input before transforming it.
    """
    assert set(tb["indicator"]) == set(INDICATOR_NAMES), (
        f"Unexpected indicator set in meadow input: {sorted(set(tb['indicator']) ^ set(INDICATOR_NAMES))}. "
        "An unmapped indicator would silently become NaN in make_indicators_wide."
    )
    assert set(tb["financing_scheme"]) == EXPECTED_FINANCING_SCHEMES, (
        f"Financing schemes changed in the source: {sorted(set(tb['financing_scheme']) ^ EXPECTED_FINANCING_SCHEMES)}"
    )
    negative = tb[tb["value"] < 0]
    assert negative.empty, (
        f"Negative expenditure values in meadow input: {negative[['country', 'year', 'indicator']].head()}"
    )
    base_years = set(tb.loc[tb["indicator"].isin(PPP_INDICATORS), "base_period"].dropna())
    assert base_years == {PPP_YEAR}, (
        f"PPP base year changed: {sorted(base_years)} != {{{PPP_YEAR}}}. The OECD rebased the constant-price "
        "series — update PPP_YEAR here and `ppp_year` in health_expenditure.meta.yml together."
    )


def sanity_check_outputs(tb: Table) -> None:
    """
    Check the wide table right before formatting and saving.
    """
    assert tb["country"].nunique() >= MIN_COUNTRIES, (
        f"Country coverage shrank: {tb['country'].nunique()} < {MIN_COUNTRIES}. Possible parsing or mapping regression."
    )
    assert tb.columns[tb.isna().all()].empty, f"Fully-NaN column(s) in output: {list(tb.columns[tb.isna().all()])}"

    # Value bounds, grounded in the built data (US share_gdp peaked at ~18.7% in recent years).
    assert tb["share_gdp"].min() >= 0 and tb["share_gdp"].max() < 30, (
        f"share_gdp out of [0, 30): min={tb['share_gdp'].min()}, max={tb['share_gdp'].max()}"
    )
    assert tb["share_expenditure"].min() >= 0 and tb["share_expenditure"].max() <= 100, (
        f"share_expenditure out of [0, 100]: min={tb['share_expenditure'].min()}, max={tb['share_expenditure'].max()}"
    )
    assert (tb["ppp_dollars"].dropna() >= 0).all() and (tb["ppp_dollars_per_capita"].dropna() >= 0).all(), (
        "Negative PPP dollar values in output."
    )
    # Magnitude guard for the millions conversion: total US health spending is in the trillions,
    # so after multiplying by 1e6 the maximum must sit between 1e12 and 1e14.
    assert 1e12 < tb["ppp_dollars"].max() < 1e14, (
        f"ppp_dollars magnitude looks wrong (max={tb['ppp_dollars'].max():.3g}) — was the 1e6 conversion lost or applied twice?"
    )
