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
# NOTE: these can legitimately exceed 100% of GDP (Dec 2025 data: gross debt up to 495%, expenditure up
# to 595% in crisis years), so the upper bound is a generous ceiling that catches unit mistakes
# (a x100 error would blow past it) rather than a 0-100 percentage check.
NON_NEGATIVE_COLUMNS = ["revenue", "expenditure", "interest_expense", "primary_expenditure", "gross_debt"]
SHARE_OF_GDP_CEILING = 1000

# Coverage floors from the Dec 2025 release: 153 source country labels (151 after harmonization, since
# the source ships duplicate labels for Congo and Bahamas). A drop below these in a future release is
# usually a parsing or mapping regression, not a real change — re-audit before bumping.
EXPECTED_INPUT_COUNTRIES = 153
EXPECTED_OUTPUT_COUNTRIES = 151


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
        assert tb[col].max() < SHARE_OF_GDP_CEILING, (
            f"Implausibly large value in {col} (max: {tb[col].max()}) — likely a unit mistake."
        )


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
