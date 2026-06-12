"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define indicator columns and their new names
INDICATOR_COLUMNS = {
    "rev": "revenue",
    "exp": "expenditure",
    "ie": "interest_expense",
    "prim_exp": "primary_expenditure",
    "pb": "primary_balance",
    # NOTE: The Dec 2025 release renamed the gross debt column from "debt" to "d".
    "d": "gross_debt",
    "rltir": "real_long_term_interest_rate",
    "rgc": "real_growth_rate",
    "gg_budg": "gg_budg",
    "gg_debt": "gg_debt",
}

# Expected columns in the meadow table (catches source schema changes, like the Dec 2025 debt -> d rename).
EXPECTED_INPUT_COLUMNS = {"country", "year", "isocode", "ifscode"} | set(INDICATOR_COLUMNS)

# Indicators measured as a (non-negative) share of GDP; primary_balance, real_long_term_interest_rate,
# and real_growth_rate can legitimately be negative.
NON_NEGATIVE_COLUMNS = ["revenue", "expenditure", "interest_expense", "primary_expenditure", "gross_debt"]

# Expected cases of expenditure above 100% of GDP in the source data, all with tiny or collapsed GDP
# denominators. The two event-driven cases are bounded to their years; Kiribati's is a recurring
# structural pattern, so any year is allowed (None):
# - Kuwait (1990-1991): GDP roughly halved under the Iraqi occupation while war, welfare, and
#   coalition-transfer spending soared (documented in IMF/academic Gulf War accounts).
# - Equatorial Guinea (1985-1995, up to 595%): the pre-oil economy was tiny, aid-dependent, and badly
#   measured; the extreme ratios are as shipped by the IMF source and may partly reflect the GDP
#   denominator rather than a verified economic event. Kept as published.
# - Kiribati: IMF Article IV consultations report public expenditure of 119% of GDP (2017) and 143%
#   (2018), funded mainly by fishing-license revenue (~72% of GDP) and development partners — new
#   years above 100% are expected, not a regression.
# Any other (country, year) above 100% should be reviewed before being added here.
EXPENDITURE_OVER_100_EXPECTED: dict[str, range | None] = {
    "Kuwait": range(1990, 1992),
    "Equatorial Guinea": range(1985, 1996),
    "Kiribati": None,
}
# Kiribati's open-ended exception is magnitude-capped: the IMF reports expenditure peaking at 143% of
# GDP (2018, Country Report 19/26), so values above this ceiling are likely data errors rather than
# the structural pattern.
EXPENDITURE_CEILING_KIRIBATI = 150

# Coverage floors from the Dec 2025 release: 153 source country labels (151 after harmonization, since
# the source ships duplicate labels for Congo and Bahamas). A drop below these in a future release is
# usually a parsing or mapping regression, not a real change — re-audit before bumping.
EXPECTED_INPUT_COUNTRIES = 153
EXPECTED_OUTPUT_COUNTRIES = 151


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("public_finances_modern_history")

    # Read table from meadow dataset.
    tb = ds_meadow["public_finances_modern_history"].reset_index()

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)

    # Drop ifscode and isocode columns.
    tb = tb.drop(columns=["ifscode", "isocode"], errors="raise")

    # Rename columns
    tb = tb.rename(columns=INDICATOR_COLUMNS, errors="raise")

    # NOTE: format() also asserts (country, year) uniqueness — the source ships duplicate labels for
    # Congo and Bahamas, which must not overlap in years once harmonized to the same name.
    tb = tb.format(["country", "year"])

    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb.columns) == EXPECTED_INPUT_COLUMNS, (
        f"IMF file schema changed — unexpected column difference: {set(tb.columns) ^ EXPECTED_INPUT_COLUMNS}"
    )
    assert tb["country"].nunique() >= EXPECTED_INPUT_COUNTRIES, (
        f"Source country coverage shrank: {tb['country'].nunique()} < {EXPECTED_INPUT_COUNTRIES}"
    )
    for col in ["gg_budg", "gg_debt"]:
        assert not tb[col].isna().any(), f"Sector coverage flag {col} has missing values."
        assert set(tb[col].unique()) <= {0, 1}, f"Sector coverage flag {col} has values outside {{0, 1}}."


def sanity_check_outputs(tb: Table) -> None:
    assert set(tb.columns) == set(INDICATOR_COLUMNS.values()), (
        f"Unexpected output columns: {set(tb.columns) ^ set(INDICATOR_COLUMNS.values())}"
    )
    assert tb.columns[tb.isna().all()].empty, "Output has a fully-NaN column."
    assert tb.index.get_level_values("country").nunique() >= EXPECTED_OUTPUT_COUNTRIES, (
        "Harmonized country coverage shrank: "
        f"{tb.index.get_level_values('country').nunique()} < {EXPECTED_OUTPUT_COUNTRIES}"
    )
    for col in NON_NEGATIVE_COLUMNS:
        assert tb[col].min() >= 0, f"Negative value in {col} (min: {tb[col].min()}) — source error or unit mistake."
    # Expenditure should stay below 100% of GDP outside the known (country, year) exceptions.
    unexpected_over_100 = sorted(
        (country, year)
        for country, year in tb[tb["expenditure"] > 100].index
        if country not in EXPENDITURE_OVER_100_EXPECTED
        or (EXPENDITURE_OVER_100_EXPECTED[country] is not None and year not in EXPENDITURE_OVER_100_EXPECTED[country])
    )
    assert not unexpected_over_100, f"Expenditure above 100% of GDP outside the known exceptions: {unexpected_over_100}"
    kiribati_expenditure = tb[tb.index.get_level_values("country") == "Kiribati"]["expenditure"]
    assert kiribati_expenditure.max() < EXPENDITURE_CEILING_KIRIBATI, (
        f"Kiribati expenditure above its plausible ceiling (max: {kiribati_expenditure.max()})."
    )
