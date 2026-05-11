"""Build garden tables for the LGBTI National Policy Dataset (Velasco, v2.0).

The source is long format: one row per (country, year, law, status). We:
  1. Harmonize country names.
  2. Drop structural-placeholder (law, status) combinations (all-zero across the panel).
  3. Emit the country-level table with `proportion`.
  4. Emit a regional aggregates table with country counts and population by
     "full implementation" (proportion >= 1) vs. "no/partial" (< 1).
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# OWID regions to aggregate over (matches v1).
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]

# Threshold to binarize the continuous proportion into "full implementation" yes/no.
FULL_IMPL_THRESHOLD = 1.0


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("lgbti_national_policy_dataset")
    ds_population = paths.load_dataset("population")
    tb = ds_meadow.read("lgbti_national_policy_dataset", safe_types=False)

    #
    # Process data.
    #
    # Keep only the columns we publish (drop sources, supplementary metadata, ISO/COW codes).
    tb = tb[["country", "year", "law", "status", "proportion"]].copy()

    # Harmonize country names (modern API).
    tb = paths.regions.harmonize_names(tb=tb, country_col="country")

    # Drop structural-placeholder (law, status) combinations — combos that are all-zero per the codebook.
    placeholders = _detect_structural_placeholders(tb)
    paths.log.info(f"Dropping {len(placeholders)} structural-placeholder (law, status) combinations.")
    mask = tb.set_index(["law", "status"]).index.isin(placeholders)
    tb = tb.loc[~mask].reset_index(drop=True)

    # Country-level table.
    tb_country = tb.copy()

    # Regional aggregates table (counts of countries + population by status per region).
    tb_regions = _build_regional_aggregates(tb_country, ds_population=ds_population)

    # Format and short-name both tables.
    tb_country = tb_country.format(
        ["country", "year", "law", "status"],
        short_name="lgbti_national_policy_dataset",
        sort_columns=True,
    )
    tb_regions = tb_regions.format(
        ["country", "year", "law", "status"],
        short_name="lgbti_national_policy_dataset_regions",
        sort_columns=True,
    )

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb_country, tb_regions],
        default_metadata=ds_meadow.metadata,
    )
    ds_garden.save()


def _detect_structural_placeholders(tb):
    """Return the set of (law, status) combos that are zero for every country and year.

    The codebook reports 19 placeholders; we detect them dynamically and assert the count
    is in a reasonable range so a coding change in the source surfaces as a test failure.
    """
    placeholder_series = tb.groupby(["law", "status"], observed=True)["proportion"].max()
    placeholders = set(placeholder_series[placeholder_series == 0].index)
    assert 17 <= len(placeholders) <= 20, (
        f"Expected 17-20 placeholder (law, status) combos per codebook v2.0; found {len(placeholders)}."
    )
    return placeholders


def _build_regional_aggregates(tb, ds_population):
    """Compute country counts and population by status for each (region, year, law, status)."""
    tb = paths.regions.add_population(tb=tb, warn_on_missing_countries=False)

    tb["n_countries_yes"] = (tb["proportion"] >= FULL_IMPL_THRESHOLD).astype("int64")
    tb["n_countries_no"] = (tb["proportion"] < FULL_IMPL_THRESHOLD).astype("int64")
    tb["pop_yes"] = tb["n_countries_yes"] * tb["population"]
    tb["pop_no"] = tb["n_countries_no"] * tb["population"]

    tb_regions = paths.regions.add_aggregates(
        tb=tb,
        index_columns=["country", "year", "law", "status"],
        regions=REGIONS,
        aggregations={
            "n_countries_yes": "sum",
            "n_countries_no": "sum",
            "pop_yes": "sum",
            "pop_no": "sum",
        },
    )

    # Keep only the synthetic region rows.
    tb_regions = tb_regions[tb_regions["country"].isin(REGIONS)].reset_index(drop=True)
    return tb_regions[["country", "year", "law", "status", "n_countries_yes", "n_countries_no", "pop_yes", "pop_no"]]
