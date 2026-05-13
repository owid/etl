"""Build garden tables for the LGBTI National Policy Dataset (Velasco, v2.0).

The source is long format: one row per (country, year, law, status). We:
  1. Harmonize country names.
  2. Drop structural-placeholder (law, status) combinations (all-zero across the panel).
  3. Emit the country-level table with `proportion`.
  4. Emit a regional aggregates table with country counts and population by
     "full implementation" (proportion >= 1) vs. "no/partial" (< 1).
  5. Emit a combined-categorical table reproducing v1's `age_of_consent`,
     `marriage`, and `lgb_military_join` ordinal indicators.
  6. Emit regional aggregates of the combined-categorical indicators: country
     counts and population by category, per region.
"""

from owid.catalog.utils import underscore
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# OWID regions to aggregate over: the 6 continents + World, plus the 4 World Bank income groups.
REGIONS = [
    "Europe",
    "Asia",
    "North America",
    "South America",
    "Africa",
    "Oceania",
    "World",
    "High-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "Low-income countries",
]

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
LGB_MILITARY_MAP = {
    "legal: 1 illegal: 0": "Allowed",
    "legal: 0 illegal: 1": "Banned",
    "legal: 0 illegal: 0": "No policy",
}

# Per-indicator config for combined-categorical indicators.
# Each entry says which (law, status) proportion columns feed the bucket key,
# which label each bucket gets via the bucket-key string, and the final category map.
# `sources` is a list of (column_name, key_label) pairs; the bucket key is built by
# joining "<label>: <bucket>" for each source with spaces.
COMBINED_CONFIGS = [
    {
        "short_name": "age_of_consent",
        "sources": [
            ("age_of_consent__equal", "equal"),
            ("age_of_consent__unequal", "unequal"),
        ],
        "category_map": AGE_OF_CONSENT_MAP,
    },
    {
        "short_name": "marriage",
        "sources": [
            ("marriage_equality__legal", "equality"),
            ("marriage_equality__illegal", "ban"),
            ("civil_unions__legal", "civil_unions"),
        ],
        "category_map": MARRIAGE_MAP,
    },
    {
        "short_name": "lgb_military_join",
        "sources": [
            ("lgb_military__legal", "legal"),
            ("lgb_military__illegal", "illegal"),
        ],
        "category_map": LGB_MILITARY_MAP,
    },
]


def _bucket(series):
    """Bucket a continuous proportion (0..1) into '0', '0.5', '1' strings.

    '1' = exactly 1, '0.5' = any 0 < x < 1 (subnational partial or transition year),
    '0' = exactly 0.
    """
    return series.apply(lambda v: "1" if v == 1 else "0.5" if v > 0 else "0")


def _build_one_combined(wide, config):
    """Build one combined categorical indicator from its config.

    The bucket key is "<label1>: <b1> <label2>: <b2> ...", looked up in `category_map`.
    Unmapped key combinations produce NaN in the output.
    """
    parts = []
    for col, label in config["sources"]:
        parts.append(f"{label}: " + _bucket(wide[col]))
    keys = parts[0]
    for part in parts[1:]:
        keys = keys + " " + part
    result = map_series(
        series=keys,
        mapping=config["category_map"],
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=False,
    )
    return result.copy_metadata(wide["country"])


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

    # Regional aggregates of the combined-categorical indicators (counts + population by category).
    tb_combined_regions = _build_combined_categorical_regional_aggregates(tb_combined)

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
    tb_combined_regions = tb_combined_regions.format(
        ["country", "year"],
        short_name="lgbti_national_policy_dataset_combined_regions",
        sort_columns=True,
    )

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb_country, tb_regions, tb_combined, tb_combined_regions],
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
    """Create combined ordinal indicators from the long-format (country, year, law, status) table.

    For each entry in COMBINED_CONFIGS, bucket the relevant `proportion` columns into
    '0' / '0.5' / '1', concatenate bucket strings into a key, and map to a category label.
    Unmapped key combinations produce NaN in the output column.
    """
    # Pivot the long table to wide so we can address each (law, status) column by name.
    wide = tb.pivot(index=["country", "year"], columns=["law", "status"], values="proportion")
    wide.columns = [f"{law}__{status}" for law, status in wide.columns]
    wide = wide.reset_index()

    for config in COMBINED_CONFIGS:
        wide[config["short_name"]] = _build_one_combined(wide, config)

    output_cols = ["country", "year"] + [c["short_name"] for c in COMBINED_CONFIGS]
    return wide[output_cols]


def _build_combined_categorical_regional_aggregates(tb_combined):
    """Build regional aggregates for the three combined ordinal indicators.

    For each (indicator, category) pair, produce two region-level series:
      - `<indicator>_<category>_count`: number of countries in the region in that category
      - `<indicator>_<category>_pop`:   total population in those countries

    Output is wide-format indexed by (country, year), with one column per
    (indicator, category) × {count, pop}.
    """
    tb = tb_combined.copy()
    tb = paths.regions.add_population(tb=tb, warn_on_missing_countries=False)

    new_cols = []
    for indicator in [c["short_name"] for c in COMBINED_CONFIGS]:
        for category in tb[indicator].dropna().unique():
            cat_snake = underscore(category)
            count_col = f"{indicator}_{cat_snake}_count"
            pop_col = f"{indicator}_{cat_snake}_pop"
            tb[count_col] = (tb[indicator] == category).astype("int64")
            tb[pop_col] = tb[count_col] * tb["population"]
            new_cols.extend([count_col, pop_col])

    aggregations = {col: "sum" for col in new_cols}
    tb_regions = paths.regions.add_aggregates(
        tb=tb[["country", "year"] + new_cols],
        regions=REGIONS,
        aggregations=aggregations,
    )
    tb_regions = tb_regions[tb_regions["country"].isin(REGIONS)].reset_index(drop=True)

    tb_regions = tb_regions[["country", "year"] + new_cols]

    return tb_regions
