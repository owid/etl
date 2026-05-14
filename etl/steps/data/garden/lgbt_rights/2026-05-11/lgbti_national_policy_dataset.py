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
    "equal: 0.5 unequal: 0.5": "Varies by region",
    "equal: 0.5 unequal: 0": "Varies by region",
    "equal: 0 unequal: 0.5": "Varies by region",
    "equal: 0 unequal: 0": "No legal provisions",
    "equal: 0 unequal: 1": "Unequal",
}
MARRIAGE_MAP = {
    "equality: 1 ban: 0 civil_unions: 0": "Legal",
    "equality: 1 ban: 0 civil_unions: 1": "Legal",
    "equality: 1 ban: 0 civil_unions: 0.5": "Legal",
    "equality: 0.5 ban: 0 civil_unions: 0.5": "Varies by region",
    "equality: 0.5 ban: 0 civil_unions: 1": "Varies by region",  # Brazil 2011–2012
    "equality: 0 ban: 0 civil_unions: 1": "Varies by region",
    "equality: 0 ban: 0 civil_unions: 0.5": "Varies by region",
    "equality: 0 ban: 0 civil_unions: 0": "No legal provisions",
    "equality: 0 ban: 0.5 civil_unions: 0.5": "Varies by region",
    "equality: 0.5 ban: 0.5 civil_unions: 0.5": "Varies by region",
    "equality: 0.5 ban: 0.5 civil_unions: 1": "Varies by region",  # UK 2013–2018
    "equality: 0.5 ban: 1 civil_unions: 0.5": "Varies by region",
    "equality: 0.5 ban: 1 civil_unions: 1": "Varies by region",  # Canada 2003–2004
    "equality: 0 ban: 0.5 civil_unions: 0": "Varies by region",
    "equality: 0 ban: 1 civil_unions: 1": "Varies by region",
    "equality: 0 ban: 1 civil_unions: 0.5": "Varies by region",
    "equality: 0 ban: 1 civil_unions: 0": "Banned",
}
LGB_MILITARY_MAP = {
    "legal: 1 illegal: 0": "Allowed",
    "legal: 0 illegal: 1": "Banned",
    "legal: 0 illegal: 0": "No policy",
}


def _binary_map(*, prog_key, reg_key, prog_label, reg_label, neither_label, mixed_label):
    """Standard bucket map for a two-direction policy (progressive vs regressive).

    The bucket key is "<prog_key>: <b1> <reg_key>: <b2>". The five resolved cases:
      - Both 0                       → neither_label  ("no policy in either direction")
      - Progressive 1, regressive 0  → prog_label
      - Progressive 0, regressive 1  → reg_label
      - Any partial (subnational variation) or both = 1 → mixed_label

    Note on `both = 1`: codebook §2.3 / §5.2.15 calls these "transition-year artefacts"
    (a mid-year policy change recorded as both directions for the calendar year) and
    recommends using the end-of-year status to disambiguate. In v2.0.x the producer
    cleaned up almost all of these (we counted 1 stray row across the panel, in Brazil
    GAC 2025), and the v2.0+ subnational coverage means most countries with both
    directions populated are real subnational mixes. For both interpretations,
    "Varies by region" is a safer default than picking one direction arbitrarily.
    """
    m = {
        f"{prog_key}: 0 {reg_key}: 0": neither_label,
        f"{prog_key}: 1 {reg_key}: 0": prog_label,
        f"{prog_key}: 0 {reg_key}: 1": reg_label,
        f"{prog_key}: 0.5 {reg_key}: 0": mixed_label,
        f"{prog_key}: 0 {reg_key}: 0.5": mixed_label,
        f"{prog_key}: 0.5 {reg_key}: 0.5": mixed_label,
        f"{prog_key}: 1 {reg_key}: 0.5": mixed_label,
        f"{prog_key}: 0.5 {reg_key}: 1": mixed_label,
        f"{prog_key}: 1 {reg_key}: 1": mixed_label,  # was prog_label — see docstring
    }
    return m


def _single_direction_map(*, key, yes_label, neither_label, mixed_label):
    """Standard 3-bucket map for a single-substantive-direction policy.

    The bucket key is "<key>: <b>". The three resolved cases:
      - 0     → neither_label
      - 0.5   → mixed_label (any subnational variation)
      - 1     → yes_label (the substantive direction is fully in effect)
    """
    return {
        f"{key}: 0": neither_label,
        f"{key}: 0.5": mixed_label,
        f"{key}: 1": yes_label,
    }


def _pair_map(*, key_a, key_b, both_label, a_only_label, b_only_label, neither_label, mixed_label):
    """Map for combining two related single-direction protections into one indicator.

    Used for SO + GI pairs (employment, hate crime, constitutional, goods/services
    discrimination). Each input is a single-direction protection (the substantive
    direction is fully in effect when its proportion = 1).

    The bucket key is "<key_a>: <b_a> <key_b>: <b_b>". The five resolved cases:
      - Both 1     → both_label  ("Both protected")
      - a=1, b=0   → a_only_label  ("Sexual orientation only")
      - a=0, b=1   → b_only_label  ("Gender identity only")
      - Both 0     → neither_label
      - Any partial → mixed_label
    """
    return {
        f"{key_a}: 1 {key_b}: 1": both_label,
        f"{key_a}: 1 {key_b}: 0": a_only_label,
        f"{key_a}: 0 {key_b}: 1": b_only_label,
        f"{key_a}: 0 {key_b}: 0": neither_label,
        f"{key_a}: 0.5 {key_b}: 0": mixed_label,
        f"{key_a}: 0 {key_b}: 0.5": mixed_label,
        f"{key_a}: 0.5 {key_b}: 0.5": mixed_label,
        f"{key_a}: 0.5 {key_b}: 1": mixed_label,
        f"{key_a}: 1 {key_b}: 0.5": mixed_label,
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
    # NOTE: enforcement_refinement is set on the 4 combined indicators whose source
    # (law, status) has any `Evidence_of_Enforcement == 0` rows in v2.0 — same_sex_acts,
    # transgender_military, lgb_military_join, morality_propaganda. Every other policy
    # has EoE uniformly 1 (codebook default). On each new data release, audit whether
    # other indicators acquire EoE=0 cases; if so, add `enforcement_refinement` entries
    # below and corresponding sort/description_key/region YAML entries.
    {
        "short_name": "lgb_military_join",
        "sources": [
            ("lgb_military__legal", "legal"),
            ("lgb_military__illegal", "illegal"),
        ],
        "category_map": LGB_MILITARY_MAP,
        "enforcement_refinement": {
            "eoe_source": ("lgb_military", "illegal"),
            "from_label": "Banned",
            "to_label": "Banned but not enforced",
        },
    },
    # ── Two-direction policies (both Legal and Illegal carry substantive data) ──────────
    {
        "short_name": "same_sex_acts",
        "sources": [
            ("same_sex_acts__legal", "legal"),
            ("same_sex_acts__illegal", "illegal"),
        ],
        "category_map": _binary_map(
            prog_key="legal",
            reg_key="illegal",
            prog_label="Legal",
            reg_label="Criminalized",
            neither_label="No legal provisions",
            mixed_label="Varies by region",
        ),
        "enforcement_refinement": {
            "eoe_source": ("same_sex_acts", "illegal"),
            "from_label": "Criminalized",
            "to_label": "Criminalized but not enforced",
        },
    },
    {
        "short_name": "blood_donations",
        "sources": [
            ("blood_donations__legal", "legal"),
            ("blood_donations__illegal", "illegal"),
        ],
        "category_map": _binary_map(
            prog_key="legal",
            reg_key="illegal",
            prog_label="No MSM restrictions",
            reg_label="Deferral or ban",
            neither_label="No policy",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "transgender_military",
        "sources": [
            ("transgender_military__legal", "legal"),
            ("transgender_military__illegal", "illegal"),
        ],
        "category_map": _binary_map(
            prog_key="legal",
            reg_key="illegal",
            prog_label="Allowed",
            reg_label="Banned",
            neither_label="No policy",
            mixed_label="Varies by region",
        ),
        "enforcement_refinement": {
            "eoe_source": ("transgender_military", "illegal"),
            "from_label": "Banned",
            "to_label": "Banned but not enforced",
        },
    },
    # ── Single-direction progressive policies (Legal carries the substantive data) ──────
    {
        "short_name": "civil_unions",
        "sources": [("civil_unions__legal", "legal")],
        "category_map": _single_direction_map(
            key="legal",
            yes_label="Legally recognized",
            neither_label="Not recognized",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "joint_adoption",
        "sources": [("joint_adoption__legal", "legal")],
        "category_map": _single_direction_map(
            key="legal",
            yes_label="Permitted",
            neither_label="Not permitted",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "constitutional_protections_sexual_orientation",
        "sources": [("constitutional_protections_sexual_orientation__legal", "legal")],
        "category_map": _single_direction_map(
            key="legal",
            yes_label="Constitutionally protected",
            neither_label="Not protected",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "constitutional_protections_gender_identity",
        "sources": [("constitutional_protections_gender_identity__legal", "legal")],
        "category_map": _single_direction_map(
            key="legal",
            yes_label="Constitutionally protected",
            neither_label="Not protected",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "hate_crime_protections_sexual_orientation",
        "sources": [("hate_crime_protections_sexual_orientation__legal", "legal")],
        "category_map": _single_direction_map(
            key="legal",
            yes_label="Covered by hate crime laws",
            neither_label="Not covered",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "hate_crime_protections_gender_identity",
        "sources": [("hate_crime_protections_gender_identity__legal", "legal")],
        "category_map": _single_direction_map(
            key="legal",
            yes_label="Covered by hate crime laws",
            neither_label="Not covered",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "incitement_to_hatred",
        "sources": [("incitement_to_hatred__legal", "legal")],
        "category_map": _single_direction_map(
            key="legal",
            yes_label="Prohibited",
            neither_label="Not prohibited",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "third_gender_recognition",
        "sources": [("third_gender_recognition__legal", "legal")],
        "category_map": _single_direction_map(
            key="legal",
            yes_label="Recognized",
            neither_label="Not recognized",
            mixed_label="Varies by region",
        ),
    },
    # ── Single-direction progressive policies (Illegal carries the substantive data) ────
    {
        "short_name": "employment_discrimination_sexual_orientation",
        "sources": [("employment_discrimination_sexual_orientation__illegal", "illegal")],
        "category_map": _single_direction_map(
            key="illegal",
            yes_label="Discrimination prohibited",
            neither_label="No protection",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "employment_discrimination_gender_identity",
        "sources": [("employment_discrimination_gender_identity__illegal", "illegal")],
        "category_map": _single_direction_map(
            key="illegal",
            yes_label="Discrimination prohibited",
            neither_label="No protection",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "goods_services_discrimination_sexual_orientation",
        "sources": [("goods_services_discrimination_sexual_orientation__illegal", "illegal")],
        "category_map": _single_direction_map(
            key="illegal",
            yes_label="Discrimination prohibited",
            neither_label="No protection",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "goods_services_discrimination_gender_identity",
        "sources": [("goods_services_discrimination_gender_identity__illegal", "illegal")],
        "category_map": _single_direction_map(
            key="illegal",
            yes_label="Discrimination prohibited",
            neither_label="No protection",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "conversion_therapies",
        "sources": [("conversion_therapies__illegal", "illegal")],
        "category_map": _single_direction_map(
            key="illegal",
            yes_label="Banned",
            neither_label="Not banned",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "gender_assignment_surgeries_on_children",
        "sources": [("gender_assignment_surgeries_on_children__illegal", "illegal")],
        "category_map": _single_direction_map(
            key="illegal",
            yes_label="Non-consensual surgeries banned",
            neither_label="Not banned",
            mixed_label="Varies by region",
        ),
    },
    # ── Single-direction regressive policies (Legal direction is the substantive one) ───
    {
        "short_name": "death_penalty",
        "sources": [("death_penalty__legal", "legal")],
        "category_map": _single_direction_map(
            key="legal",
            yes_label="Death penalty applies",
            neither_label="No death penalty",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "morality_propaganda",
        "sources": [("morality_propaganda__legal", "legal")],
        "category_map": _single_direction_map(
            key="legal",
            yes_label="Restrictions in effect",
            neither_label="No restrictions",
            mixed_label="Varies by region",
        ),
        "enforcement_refinement": {
            "eoe_source": ("morality_propaganda", "legal"),
            "from_label": "Restrictions in effect",
            "to_label": "Restrictions in effect but not enforced",
        },
    },
    {
        "short_name": "lgbtq_civil_society_restrictions",
        "sources": [("lgbtq_civil_society_restrictions__legal", "legal")],
        "category_map": _single_direction_map(
            key="legal",
            yes_label="Restrictions in effect",
            neither_label="No restrictions",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "religious_exemption_laws",
        "sources": [("religious_exemption_laws__legal", "legal")],
        "category_map": _single_direction_map(
            key="legal",
            yes_label="Religious exemptions in effect",
            neither_label="No religious exemptions",
            mixed_label="Varies by region",
        ),
    },
    # ── SO + GI pair combinations ────────────────────────────────────
    {
        "short_name": "employment_discrimination",
        "sources": [
            ("employment_discrimination_sexual_orientation__illegal", "so"),
            ("employment_discrimination_gender_identity__illegal", "gi"),
        ],
        "category_map": _pair_map(
            key_a="so",
            key_b="gi",
            both_label="Both protected",
            a_only_label="Sexual orientation only",
            b_only_label="Gender identity only",
            neither_label="No protection",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "goods_services_discrimination",
        "sources": [
            ("goods_services_discrimination_sexual_orientation__illegal", "so"),
            ("goods_services_discrimination_gender_identity__illegal", "gi"),
        ],
        "category_map": _pair_map(
            key_a="so",
            key_b="gi",
            both_label="Both protected",
            a_only_label="Sexual orientation only",
            b_only_label="Gender identity only",
            neither_label="No protection",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "hate_crime_protections",
        "sources": [
            ("hate_crime_protections_sexual_orientation__legal", "so"),
            ("hate_crime_protections_gender_identity__legal", "gi"),
        ],
        "category_map": _pair_map(
            key_a="so",
            key_b="gi",
            both_label="Both covered",
            a_only_label="Sexual orientation only",
            b_only_label="Gender identity only",
            neither_label="Not covered",
            mixed_label="Varies by region",
        ),
    },
    {
        "short_name": "constitutional_protections",
        "sources": [
            ("constitutional_protections_sexual_orientation__legal", "so"),
            ("constitutional_protections_gender_identity__legal", "gi"),
        ],
        "category_map": _pair_map(
            key_a="so",
            key_b="gi",
            both_label="Both protected",
            a_only_label="Sexual orientation only",
            b_only_label="Gender identity only",
            neither_label="Not protected",
            mixed_label="Varies by region",
        ),
    },
    # ── Gender-affirming care: adults + minors combined ──────────────
    # 4 source columns (adults covered / restricted, minors covered / restricted) →
    # one categorical indicator. Of 81 possible 4-tuple patterns, only 12 occur in the
    # v2 data; we map the 5 most common ones and route everything else to the
    # "Varies by region or other" default.
    #
    # NOTE: as of v2.0 the only country-year reaching the "or other" part of the
    # default bucket is Brazil 2025 (a transition-year artefact with both covered=1
    # and restricted=1 for adults). On each new data release, re-check whether more
    # transition-year cases appear here and across the two-direction policies — if
    # the count grows materially, revisit whether to surface them as their own
    # category or apply the codebook's end-of-year recoding rule.
    {
        "short_name": "gender_affirming_care",
        "sources": [
            ("gender_affirming_care_adults__covered", "ac"),
            ("gender_affirming_care_adults__restricted", "ar"),
            ("gender_affirming_care_minors__covered", "mc"),
            ("gender_affirming_care_minors__restricted", "mr"),
        ],
        "category_map": {
            "ac: 1 ar: 0 mc: 1 mr: 0": "Covered for adults and minors",
            "ac: 1 ar: 0 mc: 0 mr: 0": "Covered for adults only",
            "ac: 1 ar: 0 mc: 0 mr: 1": "Adults covered, minors restricted",
            "ac: 0 ar: 1 mc: 0 mr: 1": "Restricted for both",
            "ac: 0 ar: 0 mc: 0 mr: 0": "Neither covered nor restricted",
        },
        "default_category": "Varies by region or other",
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
    Unmapped key combinations get the optional `default_category` value (typically used
    for "Varies by region or other" buckets where enumerating every pattern is noisy);
    if `default_category` is unset, unmapped keys are left as their raw bucket string,
    which surfaces them as a sanity check rather than silently dropping.

    If `enforcement_refinement` is set, country-years whose bucketed label equals
    `from_label` and whose `evidence_of_enforcement` for the configured (law, status)
    is exactly 0 are reassigned to `to_label`. EoE NaN or 1 keeps `from_label`
    (codebook default-1 rule).
    """
    parts = []
    for col, label in config["sources"]:
        parts.append(f"{label}: " + _bucket(wide[col]))
    keys = parts[0]
    for part in parts[1:]:
        keys = keys + " " + part
    cat_map = config["category_map"]
    if "default_category" in config:
        result = keys.map(lambda k: cat_map.get(k, config["default_category"]))
    else:
        result = map_series(
            series=keys,
            mapping=cat_map,
            warn_on_missing_mappings=False,
            warn_on_unused_mappings=False,
        )
    # Apply optional enforcement refinement: split `from_label` into `to_label` where
    # EoE for the configured (law, status) source is 0. Strict equality so NaN keeps
    # the unrefined label.
    refinement = config.get("enforcement_refinement")
    if refinement is not None:
        law, status = refinement["eoe_source"]
        eoe = wide[f"{law}__{status}__eoe"]
        unenforced_mask = (result == refinement["from_label"]) & (eoe == 0)
        result = result.where(~unenforced_mask, refinement["to_label"])
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
    # Keep the columns we publish (drop sources, ISO/COW codes, most supplementary
    # metadata) but retain `gender_change_requirement` (used by the GMC combined
    # indicator to refine "legal" countries into procedural categories) and
    # `evidence_of_enforcement` (used by 4 combined indicators to surface
    # "X but not enforced" categories — see COMBINED_CONFIGS for the affected set).
    tb = tb[
        ["country", "year", "law", "status", "proportion", "gender_change_requirement", "evidence_of_enforcement"]
    ].copy()

    # Harmonize country names
    tb = paths.regions.harmonize_names(tb=tb)

    # Drop structural-placeholder (law, status) combinations — combos that are all-zero per the codebook.
    placeholders = _detect_structural_placeholders(tb)
    mask = tb.set_index(["law", "status"]).index.isin(placeholders)
    tb = tb.loc[~mask].reset_index(drop=True)

    # Country-level table — drop the requirement and enforcement columns here; they're
    # only used to build combined indicators and aren't published as per-row long-table
    # indicators.
    tb_country = tb.drop(columns=["gender_change_requirement", "evidence_of_enforcement"]).copy()

    # Regional aggregates table (counts of countries + population by status per region).
    tb_regions = _build_regional_aggregates(tb_country)

    # Combined-categorical table reproducing v1's three ordinal indicators.
    # Pass the full tb (with requirement) so the GMC combined indicator can use it.
    tb_combined = _build_combined_categorical_table(tb)

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

    Also extracts `gender_change_requirement` for `gender_marker_change/legal` rows so the
    GMC combined indicator can refine "legal" countries by their legal pathway
    (Self-ID, Medical/Psychological, Surgery required, Surgery and sterilization required).
    """
    # Pivot the long table to wide so we can address each (law, status) column by name.
    wide = tb.pivot(index=["country", "year"], columns=["law", "status"], values="proportion")
    wide.columns = [f"{law}__{status}" for law, status in wide.columns]
    wide = wide.reset_index()

    # Pivot `evidence_of_enforcement` on the same (law, status) axes and merge side
    # columns into `wide` with naming `<law>__<status>__eoe`. Used by the
    # enforcement_refinement field on COMBINED_CONFIGS entries.
    eoe_wide = tb.pivot(index=["country", "year"], columns=["law", "status"], values="evidence_of_enforcement")
    eoe_wide.columns = [f"{law}__{status}__eoe" for law, status in eoe_wide.columns]
    wide = wide.merge(eoe_wide.reset_index(), on=["country", "year"], how="left")

    # Extract the per-(country, year) GMC requirement and merge into wide as a side column.
    gmc_req = tb[(tb["law"] == "gender_marker_change") & (tb["status"] == "legal")][
        ["country", "year", "gender_change_requirement"]
    ].rename(columns={"gender_change_requirement": "gender_marker_change_requirement"})
    wide = wide.merge(gmc_req, on=["country", "year"], how="left")

    # Build the GMC combined indicator (proportion + requirement) inline before the loop.
    wide["gender_marker_change"] = _build_gmc_combined(wide)

    for config in COMBINED_CONFIGS:
        wide[config["short_name"]] = _build_one_combined(wide, config)

    # Combined LGBT military service indicators — built AFTER the loop because they read
    # the outputs of `lgb_military_join` and `transgender_military` (also categorical).
    # Two variants: one preserves the enforcement refinement (8 categories), the other
    # folds enforcement away (7 categories, legal-status only).
    wide["lgbt_military"] = _build_lgbt_military_combined(wide)
    wide["lgbt_military_no_enforcement"] = _build_lgbt_military_no_enforcement_combined(wide)

    output_cols = (
        ["country", "year", "gender_marker_change"]
        + [c["short_name"] for c in COMBINED_CONFIGS]
        + ["lgbt_military", "lgbt_military_no_enforcement"]
    )
    return wide[output_cols]


def _build_gmc_combined(wide):
    """Build the gender_marker_change combined indicator with requirement refinement.

    Source: `gender_marker_change__legal` (continuous proportion) +
            `gender_marker_change_requirement` (string).

    Categories:
      - Not legally possible            : proportion = 0
      - Varies by region             : 0 < proportion < 1
      - Self-declaration                : proportion = 1, requirement in {Self-ID, Self-Declaration}
      - Medical/psychological diagnosis : proportion = 1, requirement = Medical/Psychological
      - Surgery required                : proportion = 1, requirement = Surgery
      - Surgery and sterilization       : proportion = 1, requirement = Surgery+Sterilization
      - Legally possible, requirement unknown : proportion = 1, requirement is NaN
    """
    prop = wide["gender_marker_change__legal"]
    req = wide["gender_marker_change_requirement"]

    REQ_LABELS = {
        "Self-ID": "Self-declaration",
        "Self-Declaration": "Self-declaration",
        "Medical/Psychological": "Medical or psychological diagnosis",
        "Surgery": "Surgery required",
        "Surgery+Sterilization": "Surgery and sterilization required",
    }

    def classify(p, r):
        if p == 0:
            return "Not legally possible"
        if 0 < p < 1:
            return "Varies by region"
        # p == 1: refine by requirement
        if r in REQ_LABELS:
            return REQ_LABELS[r]
        return "Legally possible, requirement unknown"

    out = [classify(p, r) for p, r in zip(prop, req)]
    from owid.catalog import Variable

    result = Variable(out, index=wide.index, name="gender_marker_change")
    # Copy origins/metadata from the source proportion column so the indicator carries provenance.
    return result.copy_metadata(wide["gender_marker_change__legal"])


def _build_lgbt_military_combined(wide):
    """Combine `lgb_military_join` and `transgender_military` into a single LGBT military indicator.

    Reads the already-built combined-categorical outputs of the two source indicators
    (Allowed / No policy / Banned but not enforced / Banned each) and maps the
    cross-product to a label that names which group(s) are affected. Only the 8
    combinations that actually occur in the v2.0 data have explicit labels; anything
    unexpected falls back to a descriptive "Other (lgb / t)" placeholder, which surfaces
    as a sanity check rather than silently aggregating.
    """
    from owid.catalog import Variable

    lgb = wide["lgb_military_join"]
    t = wide["transgender_military"]

    LABEL_MAP = {
        ("Allowed", "Allowed"): "Open service permitted",
        ("Allowed", "No policy"): "Allowed for LGB only",
        ("No policy", "Allowed"): "Allowed for transgender only",
        ("Allowed", "Banned but not enforced"): "Mixed (LGB allowed, transgender banned but not enforced)",
        ("No policy", "No policy"): "No policy",
        ("Banned but not enforced", "No policy"): "Banned for LGB only, not enforced",
        ("Banned", "No policy"): "Banned for LGB only",
        ("Banned", "Banned"): "Service banned",
    }

    out = [LABEL_MAP.get((a, b), f"Other ({a} / {b})") for a, b in zip(lgb, t)]
    result = Variable(out, index=wide.index, name="lgbt_military")
    return result.copy_metadata(wide["lgb_military__legal"])


def _build_lgbt_military_no_enforcement_combined(wide):
    """Combine `lgb_military_join` and `transgender_military` into a single LGBT military indicator,
    collapsing the enforcement refinement back to legal-status only.

    Same shape as `_build_lgbt_military_combined` but `"Banned but not enforced"` folds into
    `"Banned"` on each side before mapping. Drops from 8 to 7 categories: removes the
    `"Banned for LGB only, not enforced"` bucket and collapses the US-2025 case into
    `"Mixed (LGB allowed, transgender banned)"`.
    """
    from owid.catalog import Variable

    def _fold(label):
        return "Banned" if label == "Banned but not enforced" else label

    lgb = [_fold(x) for x in wide["lgb_military_join"]]
    t = [_fold(x) for x in wide["transgender_military"]]

    LABEL_MAP = {
        ("Allowed", "Allowed"): "Open service permitted",
        ("Allowed", "No policy"): "Allowed for LGB only",
        ("No policy", "Allowed"): "Allowed for transgender only",
        ("Allowed", "Banned"): "Mixed (LGB allowed, transgender banned)",
        ("No policy", "No policy"): "No policy",
        ("Banned", "No policy"): "Banned for LGB only",
        ("Banned", "Banned"): "Service banned",
    }

    out = [LABEL_MAP.get((a, b), f"Other ({a} / {b})") for a, b in zip(lgb, t)]
    result = Variable(out, index=wide.index, name="lgbt_military_no_enforcement")
    return result.copy_metadata(wide["lgb_military__legal"])


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

    # All combined-categorical indicators: the config-driven ones + the GMC indicator
    # that's built inline (via _build_gmc_combined) and isn't in COMBINED_CONFIGS.
    indicator_cols = [c["short_name"] for c in COMBINED_CONFIGS] + [
        "gender_marker_change",
        "lgbt_military",
        "lgbt_military_no_enforcement",
    ]

    new_cols = []
    for indicator in indicator_cols:
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
