"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions to aggregate.
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]
# Labels used by the source for the totals.
ALL_AGENTS = "All infectious agents"
ALL_CANCERS = "All cancers but non-melanoma skin cancer (C00-97, but C44)"
EXPECTED_AGENTS = {
    ALL_AGENTS,
    "EBV",
    "HPV",
    "Helicobacter pylori",
    "Hepatitis B virus",
    "Hepatitis C virus",
    "Human T-cell lymphotropic virus",
    "Human herpesvirus type 8",
    "Opisthorchis viverrini and Clonorchis sinensis",
    "Schistosoma haematobium",
}
EXPECTED_NUM_CANCERS = 21  # 20 infection-related cancer sites plus the all-cancers total.
EXPECTED_YEARS = {2020}
# Number of countries in the current version of the data.
# NOTE: A lower count on the next update usually means a parsing or harmonization regression, not a real change.
MIN_NUM_COUNTRIES = 185
# The source's own both-sexes total of attributable cases differs from the sum of men and women by more than 1% in
# two countries (The Gambia and Zambia in the 2020 estimates). We keep the source values as published.
TOLERANCE_SOURCE_SEX_TOTALS = 0.01


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gco_infections")

    # Read table from meadow dataset.
    tb = ds_meadow.read("gco_infections")

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Add region aggregates for the number of new cases and of attributable cases (rates and fractions can't be summed).
    tb = paths.regions.add_aggregates(
        tb,
        index_columns=["country", "year", "sex", "agent", "cancer"],
        regions=REGIONS,
        aggregations={"cases": "sum", "attr_cases": "sum"},
        min_num_values_per_year=1,
    )

    # The source reports the cancer-site breakdown for men and women separately only. Add both-sexes rows for those
    # sites by summing new cases and attributable cases over men and women.
    tb_sites = tb[tb["cancer"] != ALL_CANCERS]
    tb_both = tb_sites.groupby(["country", "year", "agent", "cancer"], as_index=False, observed=True).agg(
        {"cases": "sum", "attr_cases": "sum"}
    )
    tb_both["sex"] = "both"
    tb = pr.concat([tb, tb_both], ignore_index=True)

    # Share of new cases attributable to infections (also available for regions and for the added both-sexes rows,
    # unlike the source's population attributable fraction).
    tb["attr_cases_share"] = tb["attr_cases"] / tb["cases"] * 100

    # Improve table format.
    tb = tb.format(["country", "year", "sex", "agent", "cancer"], short_name=paths.short_name)

    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    """Check the structure and value ranges of the meadow table."""
    error = "Unexpected set of infectious agents in the source data."
    assert set(tb["agent"]) == EXPECTED_AGENTS, error
    error = f"Expected {EXPECTED_NUM_CANCERS} cancer categories, found {tb['cancer'].nunique()}."
    assert tb["cancer"].nunique() == EXPECTED_NUM_CANCERS, error
    error = "Unexpected years in the source data."
    assert set(tb["year"]) == EXPECTED_YEARS, error
    error = "Unexpected sex categories in the source data."
    assert set(tb["sex"]) == {"both", "males", "females"}, error
    error = f"Only {tb['country'].nunique()} countries in the source data, fewer than the {MIN_NUM_COUNTRIES} expected."
    assert tb["country"].nunique() >= MIN_NUM_COUNTRIES, error

    value_columns = ["cases", "attr_cases", "asir_att", "paf", "asir"]
    error = "Negative values found in the source data."
    assert (tb[value_columns].min() >= 0).all(), error
    error = "Population attributable fractions must be shares between 0% and 100%."
    assert tb["paf"].dropna().between(0, 100).all(), error
    has_both = tb["cases"].notna() & tb["attr_cases"].notna()
    error = "Attributable cases exceed the total number of new cases."
    assert (tb.loc[has_both, "attr_cases"] <= tb.loc[has_both, "cases"] * (1 + 1e-6)).all(), error

    # Soft check: the source's both-sexes totals should be close to the sum of men and women.
    totals = tb[(tb["agent"] == ALL_AGENTS) & (tb["cancer"] == ALL_CANCERS)].pivot(
        index="country", columns="sex", values="attr_cases"
    )
    relative_gap = ((totals["males"] + totals["females"] - totals["both"]) / totals["both"]).abs()
    inconsistent = sorted(relative_gap[relative_gap > TOLERANCE_SOURCE_SEX_TOTALS].index)
    if inconsistent:
        log.warning(
            "Source both-sexes attributable cases differ from men + women by more than "
            f"{TOLERANCE_SOURCE_SEX_TOTALS:.0%} in: {inconsistent}"
        )


def sanity_check_outputs(tb: Table) -> None:
    """Check the output table, in particular the aggregates we compute ourselves."""
    tb = tb.reset_index()
    error = "Some region aggregates are missing."
    assert set(REGIONS) <= set(tb["country"]), error
    error = "Attributable shares must be between 0% and 100%."
    assert tb["attr_cases_share"].dropna().between(0, 100 + 1e-6).all(), error
    error = "There should be no columns with only NaNs."
    assert tb.columns[tb.isna().all()].empty, error

    # World must equal the sum of all countries (regions excluded) for the all-cancers, all-agents totals.
    totals = tb[(tb["agent"] == ALL_AGENTS) & (tb["cancer"] == ALL_CANCERS) & (tb["sex"] == "both")]
    countries_sum = totals[~totals["country"].isin(REGIONS)][["cases", "attr_cases"]].sum()
    world = totals[totals["country"] == "World"][["cases", "attr_cases"]].iloc[0]
    error = "World aggregate does not match the sum of countries."
    assert np.allclose(world.astype(float), countries_sum.astype(float), rtol=1e-6), error

    # The both-sexes rows we add for cancer sites must equal men + women.
    sites = tb[(tb["cancer"] != ALL_CANCERS) & (tb["agent"] == ALL_AGENTS)]
    by_sex = sites.pivot(index=["country", "cancer"], columns="sex", values="attr_cases")
    men_plus_women = by_sex[["males", "females"]].sum(axis=1, min_count=1)
    error = "Both-sexes attributable cases for cancer sites do not equal men + women."
    assert np.allclose(by_sex["both"].astype(float), men_plus_women.astype(float), rtol=1e-6, equal_nan=True), error
