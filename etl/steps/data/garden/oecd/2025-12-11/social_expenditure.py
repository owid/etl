"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define indicator columns and their new names.
INDICATOR_COLUMNS = {
    "Percentage of GDP": "share_gdp",
    "Percentage of general government expenditure": "share_gov_expenditure",
    "US dollars per person, PPP converted": "usd_per_person_ppp",
}
VALUE_COLUMNS = list(INDICATOR_COLUMNS.values())

# Columns that identify a data point, except for the expenditure source.
DIMENSIONS = ["country", "year", "spending_type", "programme_type_category", "programme_type"]

# Expenditure sources involved in the calculation of mandatory private spending.
PUBLIC = "Public"
PUBLIC_AND_MANDATORY_PRIVATE = "Public and mandatory private"
MANDATORY_PRIVATE = "Mandatory private"
EXPECTED_EXPENDITURE_SOURCES = {
    PUBLIC,
    PUBLIC_AND_MANDATORY_PRIVATE,
    MANDATORY_PRIVATE,
    "Voluntary private",
    "Mandatory private and voluntary private",
    "Net public",
    "Net total",
}

# Country-years where SOCX's shares of government spending imply an impossible total government spending, which
# the .corrections.yml file removes: Japan 2004 (8.8% of GDP, against 34.9% in 2005), the United Kingdom 1989
# (10.3%, against 38.9% in 1990), and Mexico 2020 (45.1%, against 29.3% in the OECD's national accounts).
# NOTE: Check on every update whether SOCX still publishes these values.
GOVERNMENT_SPENDING_ERRORS = {("Japan", 2004), ("Mexico", 2020), ("United Kingdom", 1989)}
# Country-years with unusual but real government spending: Ireland 2010 (64.9% of GDP, the same in the OECD's
# national accounts, because of the bank rescue).
GOVERNMENT_SPENDING_UNUSUAL = {("Ireland", 2010)}

# Name of the OECD average after harmonization.
OECD_AVERAGE = "OECD countries"

# Countries in SOCX that the OECD average covers: every country in the data except Bulgaria, Croatia, Peru and
# Romania, which aren't OECD members.
# NOTE: Update when the OECD gains members (e.g. if Croatia or Bulgaria joins).
OECD_MEMBERS = {
    "Australia", "Austria", "Belgium", "Canada", "Chile", "Colombia", "Costa Rica", "Czechia", "Denmark", "Estonia",
    "Finland", "France", "Germany", "Greece", "Hungary", "Iceland", "Ireland", "Israel", "Italy", "Japan", "Latvia",
    "Lithuania", "Luxembourg", "Mexico", "Netherlands", "New Zealand", "Norway", "Poland", "Portugal", "Slovakia",
    "Slovenia", "South Korea", "Spain", "Sweden", "Switzerland", "Turkey", "United Kingdom", "United States",
}  # fmt: skip

# SOCX rounds its values to three decimals, so the difference of two published values can differ from a
# third published value by up to about 0.0015. The relative part covers float32 precision for large values.
TOLERANCE_ABSOLUTE = 0.003
TOLERANCE_RELATIVE = 1e-6


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("social_expenditure")

    # Read table from meadow dataset.
    tb = ds_meadow.read("social_expenditure")

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)

    # Pivot table to have indicator as columns.
    tb = tb.pivot(
        index=["country", "year", "expenditure_source", "spending_type", "programme_type_category", "programme_type"],
        columns="indicator",
        values="value",
        join_column_levels_with="_",
    )

    # Rename columns
    tb = tb.rename(columns=INDICATOR_COLUMNS)

    # Change the categories of the total rows in the table.
    tb = change_total_categories(tb)

    sanity_check_inputs(tb)

    # Check that the only years with an implausible share of government spending are the known source errors.
    assert set(find_implausible_government_spending(tb)) == GOVERNMENT_SPENDING_ERRORS, (
        f"Years with implausible total government spending changed: {find_implausible_government_spending(tb)}. "
        "Update GOVERNMENT_SPENDING_ERRORS and the .corrections.yml file."
    )

    # Remove known errors in the source (see the .corrections.yml file).
    tb = paths.apply_corrections(tb)
    assert not find_implausible_government_spending(tb), "Implausible shares of government spending remain."

    # Replace SOCX's mandatory private spending with public and mandatory private minus public spending.
    tb_corrected = tb.copy()
    tb = derive_mandatory_private(tb)

    sanity_check_outputs(tb, tb_corrected)

    tb = tb.format(
        ["country", "year", "expenditure_source", "spending_type", "programme_type_category", "programme_type"]
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def change_total_categories(tb):
    """
    Change the categories of the total rows in the table.
    This allows for easier reading of data pages.
    """
    tb.loc[tb["spending_type"] == "Total", "spending_type"] = "In-cash and in-kind spending"
    tb.loc[tb["spending_type"] == "In cash spending", "spending_type"] = "In-cash spending"
    tb.loc[tb["spending_type"] == "In kind spending", "spending_type"] = "In-kind spending"

    tb.loc[tb["programme_type_category"] == "Total", "programme_type_category"] = "All"

    return tb


def derive_mandatory_private(tb: Table) -> Table:
    """Calculate mandatory private spending as public and mandatory private spending minus public spending.

    SOCX only publishes mandatory private spending for countries that have such schemes, and its OECD average
    only covers those countries. The difference gives 0 for countries without these schemes, and an OECD
    average across all countries. For example, old age and survivors, in cash, 2021: SOCX's OECD averages are
    8.04% of GDP (public) and 8.61% (public and mandatory private), so mandatory private is 0.57%, while SOCX's
    own mandatory private average is 1.44%.

    The three units subtract cleanly: in each row, public and combined spending share the same denominator
    (GDP, general government expenditure, or population and PPP conversion factor).

    Where the difference can't be calculated (no public value), individual countries keep SOCX's published
    mandatory private value. The OECD average doesn't, because SOCX's average covers a different set of
    countries.
    """
    tb_public = tb.loc[tb["expenditure_source"] == PUBLIC, DIMENSIONS + VALUE_COLUMNS]
    tb_combined = tb.loc[tb["expenditure_source"] == PUBLIC_AND_MANDATORY_PRIVATE, DIMENSIONS + VALUE_COLUMNS]
    tb_published = tb.loc[tb["expenditure_source"] == MANDATORY_PRIVATE, DIMENSIONS + VALUE_COLUMNS]

    # Subtract public from public and mandatory private spending.
    tb_derived = pr.merge(tb_combined, tb_public, on=DIMENSIONS, how="inner", suffixes=("", "_public"))
    is_oecd = tb_derived["country"] == OECD_AVERAGE
    for col in VALUE_COLUMNS:
        tolerance = TOLERANCE_ABSOLUTE + TOLERANCE_RELATIVE * tb_derived[col].abs()
        tb_derived[col] = tb_derived[col] - tb_derived[f"{col}_public"]

        # Countries: combined spending should never be below public spending.
        negative = (tb_derived[col] < -tolerance) & ~is_oecd
        assert not negative.any(), (
            f"Public and mandatory private spending is below public spending ({col}) in "
            f"{negative.sum()} country rows:\n{tb_derived.loc[negative, DIMENSIONS + [col]]}"
        )

        # OECD average: in some years before all OECD members report, SOCX's public and combined averages
        # aren't consistent, and the difference is negative. Remove those values.
        negative = (tb_derived[col] < -tolerance) & is_oecd
        if negative.any():
            log.info(
                f"Removing {negative.sum()} OECD average values of {col} where the public and mandatory private "
                "average is below the public average."
            )
            tb_derived.loc[negative, col] = None

        # Remove rounding noise below zero, and round to three decimals, like SOCX.
        tb_derived.loc[tb_derived[col] < 0, col] = 0
        tb_derived[col] = tb_derived[col].round(3)
    tb_derived = tb_derived[DIMENSIONS + VALUE_COLUMNS]

    # Check the difference against SOCX's published mandatory private spending, and the OECD average against
    # the average of its members.
    check_against_published(tb_derived=tb_derived, tb_published=tb_published)
    check_oecd_average(tb_derived=tb_derived)

    # Countries keep SOCX's published value where the difference can't be calculated.
    tb_mandatory = pr.merge(tb_derived, tb_published, on=DIMENSIONS, how="outer", suffixes=("", "_published"))
    is_country = tb_mandatory["country"] != OECD_AVERAGE
    for col in VALUE_COLUMNS:
        missing = tb_mandatory[col].isna() & is_country
        tb_mandatory.loc[missing, col] = tb_mandatory.loc[missing, f"{col}_published"]
    tb_mandatory = tb_mandatory[DIMENSIONS + VALUE_COLUMNS].dropna(subset=VALUE_COLUMNS, how="all")
    tb_mandatory["expenditure_source"] = MANDATORY_PRIVATE

    # Replace SOCX's mandatory private rows.
    tb = pr.concat([tb[tb["expenditure_source"] != MANDATORY_PRIVATE], tb_mandatory], ignore_index=True)

    return tb


def check_against_published(tb_derived: Table, tb_published: Table) -> None:
    """Assert that, for individual countries, the difference matches SOCX's published mandatory private
    spending where SOCX publishes it, and is 0 where it doesn't."""
    tb_compare = pr.merge(tb_derived, tb_published, on=DIMENSIONS, how="left", suffixes=("", "_published"))
    tb_compare = tb_compare[tb_compare["country"] != OECD_AVERAGE]
    is_published = tb_compare[[f"{col}_published" for col in VALUE_COLUMNS]].notna().any(axis=1)
    for col in VALUE_COLUMNS:
        tolerance = TOLERANCE_ABSOLUTE + TOLERANCE_RELATIVE * tb_compare[col].abs()
        difference = (tb_compare[col] - tb_compare[f"{col}_published"]).abs()
        mismatch = is_published & (difference > tolerance)
        assert not mismatch.any(), (
            f"Calculated mandatory private spending ({col}) differs from SOCX's published value in "
            f"{mismatch.sum()} country rows:\n{tb_compare.loc[mismatch, DIMENSIONS + [col, f'{col}_published']]}"
        )
        nonzero = ~is_published & (tb_compare[col] > tolerance)
        assert not nonzero.any(), (
            f"Calculated mandatory private spending ({col}) is above 0 in {nonzero.sum()} country rows where SOCX "
            f"publishes no mandatory private value:\n{tb_compare.loc[nonzero, DIMENSIONS + [col]]}"
        )


def check_oecd_average(tb_derived: Table) -> None:
    """Assert that, in years when all OECD members report, the calculated OECD average of mandatory private
    spending (share of GDP) equals the simple average of the members' calculated values.

    SOCX's OECD averages are simple averages of its members once they all report; before that they are SOCX's own
    estimates, which this check can't reproduce.
    """
    tb_check = tb_derived.dropna(subset=["share_gdp"])
    groups = ["year", "spending_type", "programme_type_category", "programme_type"]
    tb_members = tb_check[tb_check["country"].isin(OECD_MEMBERS)]
    members = tb_members.groupby(groups, observed=True)["share_gdp"].agg(["mean", "count"]).reset_index()
    members = members[members["count"] == len(OECD_MEMBERS)]
    tb_compare = pr.merge(tb_check[tb_check["country"] == OECD_AVERAGE], members, on=groups, how="inner")
    assert len(tb_compare) > 0, "No year with all OECD members reporting: check OECD_MEMBERS."
    log.info(f"Checking the calculated OECD average against the average of its members in {len(tb_compare)} rows.")
    difference = (tb_compare["share_gdp"] - tb_compare["mean"]).abs()
    assert (difference <= TOLERANCE_ABSOLUTE).all(), (
        f"Calculated OECD average of mandatory private spending differs from the average of its members in "
        f"{(difference > TOLERANCE_ABSOLUTE).sum()} rows:\n{tb_compare[difference > TOLERANCE_ABSOLUTE]}"
    )


def find_implausible_government_spending(tb: Table) -> list[tuple[str, int]]:
    """Find the country-years whose share of government spending implies an implausible total government spending.

    Dividing the share of GDP by the share of government spending gives total government spending as a share of
    GDP. It should be the same for every row of a country and year, and close to the country's usual level. For
    example, SOCX's values for Japan in 2004 imply government spending of 8.8% of GDP, against 34.9% in 2005.
    """
    tb_check = tb.dropna(subset=["share_gdp", "share_gov_expenditure"])
    tb_check = tb_check[(tb_check["share_gdp"] > 0.05) & (tb_check["share_gov_expenditure"] > 0.05)].copy()
    tb_check["government_spending"] = tb_check["share_gdp"] / tb_check["share_gov_expenditure"] * 100
    tb_years = tb_check.groupby(["country", "year"], observed=True)["government_spending"].median().reset_index()
    usual = tb_years.groupby("country", observed=True)["government_spending"].transform("median")
    ratio = tb_years["government_spending"] / usual
    implausible = tb_years[(ratio < 0.6) | (ratio > 1.6)]
    found = zip(implausible["country"].astype(str), implausible["year"].astype(int))
    return sorted(set(found) - GOVERNMENT_SPENDING_UNUSUAL)


def sanity_check_inputs(tb: Table) -> None:
    sources = set(tb["expenditure_source"])
    assert sources == EXPECTED_EXPENDITURE_SOURCES, (
        f"Unexpected expenditure sources: {sources ^ EXPECTED_EXPENDITURE_SOURCES}"
    )
    assert not tb.duplicated(subset=DIMENSIONS + ["expenditure_source"]).any(), "Duplicate rows in the input."
    assert OECD_AVERAGE in set(tb["country"]), f"'{OECD_AVERAGE}' not found after harmonization."
    # NOTE: SOCX has a few small negative public values (e.g. Denmark 2001-2002 unemployment, -0.001 US$ per
    # person), so only the share of GDP is checked to be non-negative.
    assert tb["share_gdp"].min() >= 0, "Negative share of GDP in the input."


def sanity_check_outputs(tb: Table, tb_input: Table) -> None:
    # Sources other than mandatory private are unchanged.
    tb_other = tb[tb["expenditure_source"] != MANDATORY_PRIVATE]
    tb_input_other = tb_input[tb_input["expenditure_source"] != MANDATORY_PRIVATE]
    assert len(tb_other) == len(tb_input_other), "Rows of sources other than mandatory private changed."
    assert not tb.duplicated(subset=DIMENSIONS + ["expenditure_source"]).any(), "Duplicate rows in the output."

    # Mandatory private spending is never negative and adds up with public to the combined total.
    tb_mandatory = tb[tb["expenditure_source"] == MANDATORY_PRIVATE]
    assert (tb_mandatory[VALUE_COLUMNS].min() >= 0).all(), "Negative mandatory private spending."
    tb_check = pr.merge(
        tb[tb["expenditure_source"] == PUBLIC_AND_MANDATORY_PRIVATE][DIMENSIONS + VALUE_COLUMNS],
        tb[tb["expenditure_source"] == PUBLIC][DIMENSIONS + VALUE_COLUMNS],
        on=DIMENSIONS,
        suffixes=("", "_public"),
    )
    tb_check = pr.merge(tb_check, tb_mandatory[DIMENSIONS + VALUE_COLUMNS], on=DIMENSIONS, suffixes=("", "_mandatory"))
    for col in VALUE_COLUMNS:
        tolerance = TOLERANCE_ABSOLUTE + TOLERANCE_RELATIVE * tb_check[col].abs()
        difference = (tb_check[f"{col}_public"] + tb_check[f"{col}_mandatory"] - tb_check[col]).abs()
        assert not (difference > tolerance).any(), f"Public plus mandatory private doesn't add up to the total ({col})."

    # Every entity with combined spending has a mandatory private value (0 if it has no such schemes).
    countries_mandatory = set(tb_mandatory["country"])
    countries_combined = set(tb[tb["expenditure_source"] == PUBLIC_AND_MANDATORY_PRIVATE]["country"])
    assert countries_mandatory == countries_combined, (
        f"Entities with mandatory private and combined spending differ: {countries_mandatory ^ countries_combined}"
    )
