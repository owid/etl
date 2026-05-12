"""Build garden tables for the LGBTI National Policy Dataset (Velasco, v2.0).

The source is long format: one row per (country, year, law, status). We:
  1. Harmonize country names.
  2. Drop structural-placeholder (law, status) combinations (all-zero across the panel).
  3. Emit the country-level table with `proportion`.
  4. Emit a regional aggregates table with country counts and population by
     "full implementation" (proportion >= 1) vs. "no/partial" (< 1).
  5. Emit a combined-categorical table reproducing v1's `age_of_consent`,
     `marriage`, and `lgb_military_join` ordinal indicators.
"""

from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# OWID regions to aggregate over (matches v1).
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]

# Threshold to binarize the continuous proportion into "full implementation" yes/no.
FULL_IMPL_THRESHOLD = 1.0

# Mappings from v1's bucketed-key strings to categorical labels (same wording as v1).
AGE_OF_CONSENT_MAP = {
    "equal: 1 unequal: 0": "Equal",
    "equal: 1 unequal: 0.5": "Equal",
    "equal: 1 unequal: 1": "Equal",  # transition-year edge case
    "equal: 0.5 unequal: 0.5": "Partial implementation",
    "equal: 0.5 unequal: 0": "Partial implementation",
    "equal: 0 unequal: 0.5": "Partial implementation",
    "equal: 0 unequal: 0": "No legal provisions",
    "equal: 0 unequal: 1": "Unequal",
}
MARRIAGE_MAP = {
    "equality: 1 ban: 0 civil_unions: 0": "Legal",
    "equality: 1 ban: 0 civil_unions: 1": "Legal",
    "equality: 1 ban: 0 civil_unions: 0.5": "Legal",
    "equality: 0.5 ban: 0 civil_unions: 0.5": "Partially legal",
    "equality: 0.5 ban: 0 civil_unions: 1": "Partially legal",  # Brazil 2011–2012
    "equality: 0 ban: 0 civil_unions: 1": "Partially legal",
    "equality: 0 ban: 0 civil_unions: 0.5": "Partially legal",
    "equality: 0 ban: 0 civil_unions: 0": "No legal provisions",
    "equality: 0 ban: 0.5 civil_unions: 0.5": "Ban and marriage both partial",
    "equality: 0.5 ban: 0.5 civil_unions: 0.5": "Ban and marriage both partial",
    "equality: 0.5 ban: 0.5 civil_unions: 1": "Ban and marriage both partial",  # UK 2013–2018
    "equality: 0.5 ban: 1 civil_unions: 0.5": "Partially banned",
    "equality: 0.5 ban: 1 civil_unions: 1": "Partially banned",  # Canada 2003–2004
    "equality: 0 ban: 0.5 civil_unions: 0": "Partially banned",
    "equality: 0 ban: 1 civil_unions: 1": "Partially banned",
    "equality: 0 ban: 1 civil_unions: 0.5": "Partially banned",
    "equality: 0 ban: 1 civil_unions: 0": "Banned",
}


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("lgbti_national_policy_dataset")
    tb = ds_meadow.read("lgbti_national_policy_dataset", safe_types=False)

    #
    # Process data.
    #
    # Keep only the columns we publish (drop sources, supplementary metadata, ISO/COW codes).
    tb = tb[["country", "year", "law", "status", "proportion"]].copy()

    # Harmonize country names
    tb = paths.regions.harmonize_names(tb=tb)

    # Drop structural-placeholder (law, status) combinations — combos that are all-zero per the codebook.
    placeholders = _detect_structural_placeholders(tb)
    mask = tb.set_index(["law", "status"]).index.isin(placeholders)
    tb = tb.loc[~mask].reset_index(drop=True)

    # Country-level table.
    tb_country = tb.copy()

    # Regional aggregates table (counts of countries + population by status per region).
    tb_regions = _build_regional_aggregates(tb_country)

    # Combined-categorical table reproducing v1's three ordinal indicators.
    tb_combined = _build_combined_categorical_table(tb_country)

    # Format and short-name all tables.
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
    tb_combined = tb_combined.format(
        ["country", "year"],
        short_name="lgbti_national_policy_dataset_combined",
        sort_columns=True,
    )

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb_country, tb_regions, tb_combined],
        default_metadata=ds_meadow.metadata,
    )
    ds_garden.save()


def _detect_structural_placeholders(tb):
    """Return the set of (law, status) combos that are zero for every country and year.

    Codebook v2.0 §1.1 reports 18 placeholders and 36 active combinations. We detect them
    dynamically and assert the count to surface any source-side coding changes.
    """
    placeholder_series = tb.groupby(["law", "status"], observed=True)["proportion"].max()
    placeholders = set(placeholder_series[placeholder_series == 0].index)
    assert len(placeholders) == 18, (
        f"Expected 18 placeholder (law, status) combos per codebook v2.0 §1.1; found {len(placeholders)}."
    )
    return placeholders


def _build_regional_aggregates(tb):
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

    # Keep only relevant columns
    tb_regions = tb_regions[
        ["country", "year", "law", "status", "n_countries_yes", "n_countries_no", "pop_yes", "pop_no"]
    ]

    return tb_regions


def _build_combined_categorical_table(tb):
    """Recreate v1's three combined ordinal indicators: age_of_consent, marriage, lgb_military_join.

    The v1 logic bucketed each policy's Proportion into 0 / 0.5 / 1 (where 0.5 means "0 < x < 1"),
    concatenated the bucket strings of the relevant policies, and mapped to a human-readable
    category. We replicate that approach here on the long-format v2 data.
    """
    # Pivot the long table to wide so we can address each (law, status) column by name.
    wide = tb.pivot(index=["country", "year"], columns=["law", "status"], values="proportion")
    wide.columns = [f"{law}__{status}" for law, status in wide.columns]
    wide = wide.reset_index()

    def bucket(series):
        # 1 if exactly 1, "0.5" if 0 < x < 1 (any subnational partial), else "0".
        return series.apply(lambda v: "1" if v == 1 else "0.5" if v > 0 else "0")

    # AGE OF CONSENT — built from equal_age and unequal_age proportions.
    equal_b = bucket(wide["age_of_consent__equal"])
    unequal_b = bucket(wide["age_of_consent__unequal"])
    age_keys = "equal: " + equal_b + " unequal: " + unequal_b
    wide["age_of_consent"] = map_series(
        series=age_keys,
        mapping=AGE_OF_CONSENT_MAP,
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=False,
    )
    wide["age_of_consent"] = wide["age_of_consent"].copy_metadata(wide["country"])

    # MARRIAGE — built from marriage_equality (Legal + Illegal) and civil_unions (Legal).
    eq_b = bucket(wide["marriage_equality__legal"])
    ban_b = bucket(wide["marriage_equality__illegal"])
    cu_b = bucket(wide["civil_unions__legal"])
    marriage_keys = "equality: " + eq_b + " ban: " + ban_b + " civil_unions: " + cu_b
    wide["marriage"] = map_series(
        series=marriage_keys,
        mapping=MARRIAGE_MAP,
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=False,
    )
    wide["marriage"] = wide["marriage"].copy_metadata(wide["country"])

    # LGB MILITARY — three pure-binary categories: Allowed / Banned / No policy.
    mil_legal = wide["lgb_military__legal"]
    mil_ban = wide["lgb_military__illegal"]
    wide["lgb_military_join"] = None
    wide.loc[(mil_legal == 1) & (mil_ban == 0), "lgb_military_join"] = "Allowed"
    wide.loc[(mil_legal == 0) & (mil_ban == 1), "lgb_military_join"] = "Banned"
    wide.loc[(mil_legal == 0) & (mil_ban == 0), "lgb_military_join"] = "No policy"
    wide["lgb_military_join"] = wide["lgb_military_join"].copy_metadata(wide["country"])

    return wide[["country", "year", "age_of_consent", "marriage", "lgb_military_join"]]
