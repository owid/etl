"""Conflict Data Source explorer.

Each data source is described by a small `SourceSpec` and built programmatically
from its grapher table(s) via the same `build_source_explorer` pipeline:

  1. `_load_and_adjust` reads the source's table(s) and rewrites each column's
     metadata to expose the explorer's dimensions (`measure`, `conflict_type`,
     `conflict_sub_type`, `sub_measure`) plus a helper `_estimate` dim used to
     identify confidence-interval (CI) variants.
  2. `paths.create_collection(tb=…)` auto-expands one view per column.
  3. `group_views` collapses the (best, low, high) helper choices on `_estimate`
     into single CI-stacked deaths views.
  4. `_build_by_sub_type_views` constructs the stacked-across-children views
     declared in `spec.by_sub_type_labels`.
  5. The intrastate-variant helper conflict_type slugs (`_intrastate_int` /
     `_intrastate_non_int`) are remapped to `intrastate_conflicts` with the
     appropriate `conflict_sub_type=only_*_conflicts` value.
  6. `_set_view_config` and `_set_view_displays` fill in titles, subtitles,
     notes, and per-indicator display blocks via templates parameterised by
     the spec.

The seven sub-explorers are merged by `combine_collections` under a leading
`data_source` dropdown (introduced by `force_collection_dimension=True`).
The YAML carries only dim definitions + explorer-level config — every source
is built programmatically.

UCDP-style sources (ucdp, ucdp_prio, mars, cow, prio) use the existing CT
pipeline. MIC-style sources (mie, cow_mid) use `style="mic"`: their columns
carry a `hostility_level` / `hostility` dim that maps to helper
`conflict_sub_type` slugs (stacked into `by_sub_type` by
`_build_mic_post_process`); COW-MID's deaths view is an 8-indicator
fatality-bucket stack assembled via the universal `_estimate` CI-collapse
step.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from owid.catalog.tables import Table

from etl.collection.core.combine import combine_collections
from etl.collection.model.view import Indicator, View, ViewIndicators
from etl.helpers import PathFinder

paths = PathFinder(__file__)


# ===========================================================================
# Module-level constants
# ===========================================================================


class CT:
    """Conflict-type slugs — single source of truth for the explorer's
    `conflict_type` dim choices. Every reference in this module goes through
    one of these constants rather than re-typing the slug string, so a typo
    becomes a Python `AttributeError` at import time instead of a silent
    missing-view or KeyError at run time.

    The seven public names map directly to the explorer's `conflict_type`
    dropdown choices. The two leading-underscore names are transient slugs
    used only during the build to keep the intrastate sub-types separable
    (so the by_sub_type stack builder can pick them up as siblings); they get
    remapped back to `CT.INTRASTATE` via `INTRASTATE_VARIANT_REMAP` before save.
    """

    ALL_ARMED = "all_armed_conflicts"
    ALL_STATE_BASED = "all_state_based_conflicts"
    INTERSTATE = "interstate_conflicts"
    INTRASTATE = "intrastate_conflicts"
    EXTRASTATE = "extrastate_conflicts"
    NON_STATE = "non_state_conflicts"
    ONE_SIDED = "one_sided_violence"
    # Build-time helpers (transient; see INTRASTATE_VARIANT_REMAP below).
    _INTRA_INT = "_intrastate_int"
    _INTRA_NON_INT = "_intrastate_non_int"


class M:
    """Measure slugs — single source of truth for the explorer's `measure` dim
    choices. Used in `spec.measures`, the per-table indicator-name lists, and
    branching logic throughout the FAUST helpers.
    """

    DEATHS = "conflict_deaths"
    DEATH_RATE = "death_rate"
    N_CONFLICTS = "number_of_conflicts"
    CONFLICT_RATE = "conflict_rate"
    LOCATIONS = "conflict_locations"
    PARTICIPANTS = "conflict_participants"


class CST:
    """Conflict sub-type slugs — single source of truth for the explorer's
    `conflict_sub_type` dim choices. `NA` is the "not applicable" value used
    for views without a meaningful sub-type split; `BY_SUB_TYPE` tags the
    stacked-across-children views; the `ONLY_*` values flag intrastate
    variants (and `ONLY_WARS` for the COW-MID interstate cut).
    """

    NA = "na"
    BY_SUB_TYPE = "by_sub_type"
    ALL_SUB_TYPES = "all_sub_types"
    ONLY_NON_INTERNATIONALIZED = "only_non_internationalized_conflicts"
    ONLY_INTERNATIONALIZED = "only_internationalized_conflicts"
    ONLY_WARS = "only_wars"
    # Build-time helpers (MIE / COW-MID hostility levels). Stacked into
    # BY_SUB_TYPE by `_build_mic_post_process`, with `_HOSTLEV_WAR` renamed to
    # `ONLY_WARS` and the rest dropped. Never appear in the saved explorer.
    _HOSTLEV_THREAT = "_hostlev_threat"
    _HOSTLEV_DISPLAY = "_hostlev_display"
    _HOSTLEV_USE = "_hostlev_use"
    _HOSTLEV_WAR = "_hostlev_war"


# === Definition-on-Demand anchors (one source of truth per data source) =====
# Each `#dod:...` URL is typed once here and referenced symbolically below so a
# typo becomes `AttributeError` at import. The spec's `dod` / `dod_by_sub_type`
# fields use these via f-strings; helpers like `_dod_url` later extract the
# anchor portion back out for subtitle templates.


class DOD_UCDP:
    """UCDP DoD anchor URLs."""

    ARMED = "#dod:armed-conflict-ucdp"
    INTERSTATE = "#dod:interstate-ucdp"
    INTRASTATE = "#dod:intrastate-ucdp"
    EXTRASYSTEMIC = "#dod:extrasystemic-ucdp"
    NONSTATE = "#dod:nonstate-ucdp"
    ONESIDED = "#dod:onesided-ucdp"


class DOD_COW:
    """COW DoD anchor URLs. Note the consistent `-war-cow` suffix."""

    WAR = "#dod:war-cow"
    STATE_BASED = "#dod:state-based-war-cow"
    INTERSTATE = "#dod:interstate-war-cow"
    INTRASTATE = "#dod:intrastate-war-cow"
    EXTRASTATE = "#dod:extrastate-war-cow"
    NON_STATE = "#dod:nonstate-war-cow"


class DOD_MARS:
    """MARS DoD anchor URLs."""

    INTERSTATE = "#dod:interstate-war-mars"
    CIVIL_WAR = "#dod:civil-war-mars"
    NON_CIVIL_WAR = "#dod:non-civil-war-mars"
    CONVENTIONAL_WARS = "#dod:conventional-war-mars"
    MAJOR_PARTICIPANT = "#dod:major-participant-mars"


class DOD_MIE:
    """MIE DoD anchor URLs (Militarized Interstate Events)."""

    FORCE = "#dod:force-mic"
    WAR = "#dod:interstate-war-mic"


class DOD_COW_MID:
    """COW-MID DoD anchor URLs (Militarized Interstate Disputes — uses COW
    war anchors but the same MIC-style ``force`` anchor schema)."""

    FORCE = "#dod:force-cow"
    WAR = "#dod:interstate-war-cow"


# Used in every conflict_participants subtitle (override per-source if needed).
DOD_PRIMARY_PARTICIPANT = "#dod:primary-participant-ucdp"


# Per-indicator display blocks for confidence-interval (CI) stacks.
BEST_DISPLAY = {"name": "Best estimate", "color": "#B13507"}
LOW_DISPLAY = {"name": "Low estimate", "color": "#C3AEA6"}
HIGH_DISPLAY = {"name": "High estimate", "color": "#C3AEA6"}

# Map colorScaleNumericBins for deaths / rate / boolean (locations/participants).
DEATHS_MAP_BINS = "0.99, #E8F4EA, 0;10;100;1000;10000;100000"
RATE_MAP_BINS = "0.1;0.3;1;3;10;30;100"
BOOL_MAP_BINS = "0,#92C5DE,No;1,#F4A582,Yes"

# Shared view-config blocks.
STACKED_CONFIG = {
    "type": "StackedBar",
    "baseColorScheme": "stackedAreaDefault",
    "hideRelativeToggle": False,
    "selectedFacetStrategy": "entity",
}
MAP_CONFIG = {"hasMapTab": True, "tab": "map", "selectedFacetStrategy": "entity"}

# Note shown on country-level map views for sources with historical states.
MISSING_STATES_NOTE = "Some states are not shown in the map because they do not exist anymore."

# === MIC (MIE / COW-MID) constants ==========================================

# Map source-table `hostility_level` / `hostility` dim values → helper
# conflict_sub_type slug + stack display label. Stack order (bottom→top)
# follows insertion order: threat / display / use / war.
HOSTLEV_RAW_TO_HELPER: dict[str, tuple[str, str]] = {
    "Threat to use force": (CST._HOSTLEV_THREAT, "Threats of force"),
    "Display of force": (CST._HOSTLEV_DISPLAY, "Displays of force"),
    "Use of force": (CST._HOSTLEV_USE, "Uses of force"),
    "War": (CST._HOSTLEV_WAR, "Wars"),
}
HOSTLEV_LABEL: dict[str, str] = {helper: label for helper, label in HOSTLEV_RAW_TO_HELPER.values()}

# COW-MID fatality buckets used in the by-bucket deaths stack. Each tuple
# carries (raw `fatality` dim value, helper `_estimate` slug, display label).
# Helper estimates are grouped into the `_ci` collapse alongside best/low/high,
# then the dim is auto-dropped by `drop_dimensions_if_single_choice`.
FATALITY_BUCKETS: list[tuple[str, str, str]] = [
    ("Unknown", "_fat_unknown", "No deaths data"),
    ("No deaths", "_fat_no_deaths", "No deaths"),
    ("1-25 deaths", "_fat_1_25", "1-25 deaths"),
    ("26-100 deaths", "_fat_26_100", "26-100 deaths"),
    ("101-250 deaths", "_fat_101_250", "101-250 deaths"),
    ("251-500 deaths", "_fat_251_500", "251-500 deaths"),
    ("501-999 deaths", "_fat_501_999", "501-999 deaths"),
    ("> 999 deaths", "_fat_gt_999", "1000 deaths or more"),
]
FATALITY_RAW_TO_HELPER: dict[str, str] = {raw: helper for raw, helper, _ in FATALITY_BUCKETS}
FATALITY_HELPER_TO_LABEL: dict[str, str] = {helper: label for _, helper, label in FATALITY_BUCKETS}

# Low/high range-plot displays (used by MARS and MIE — sources whose deaths
# view carries only low+high, no center). Distinct from `LOW_DISPLAY` /
# `HIGH_DISPLAY` (grey) which fade out around a colored center estimate.
RANGE_LOW_DISPLAY = {"name": "Low estimate", "color": "#00295B"}
RANGE_HIGH_DISPLAY = {"name": "High estimate", "color": "#B13507"}

# Human-readable conflict_type names + short labels used in templates. Specs
# can override individual entries via `ct_name` / `ct_short` on the spec.
CT_NAME = {
    CT.ALL_ARMED: "armed conflicts",
    CT.ALL_STATE_BASED: "state-based conflicts",
    CT.INTERSTATE: "interstate conflicts",
    CT.INTRASTATE: "intrastate conflicts",
    CT.EXTRASTATE: "extrasystemic conflicts",
    CT.NON_STATE: "non-state conflicts",
    CT.ONE_SIDED: "one-sided violence",
}
CT_SHORT = {
    CT.INTERSTATE: "Interstate",
    CT.INTRASTATE: "Intrastate",
    CT.EXTRASTATE: "Extrasystemic",
    CT.NON_STATE: "Non-state",
    CT.ONE_SIDED: "One-sided violence",
}

# "Parent" conflict types: their single-indicator (or CI-stacked) view uses
# `conflict_sub_type=all_sub_types` rather than `na`. For deaths/counts/rate
# the parents are the aggregates plus intrastate (which has the
# internationalized sub-types). For locations/participants only intrastate is
# treated as a parent — see `_conflict_sub_type`.
PARENT_CTS = {CT.ALL_ARMED, CT.ALL_STATE_BASED, CT.INTRASTATE}

# Helper conflict_type slugs used during the build to keep the two intrastate
# sub-types separable (so by_sub_type stacks can pick them up as children).
# They get remapped to `intrastate_conflicts` + a matching `conflict_sub_type`
# in `build_source_explorer`.
INTRASTATE_VARIANT_REMAP = {
    CT._INTRA_INT: CT.INTRASTATE,
    CT._INTRA_NON_INT: CT.INTRASTATE,
}

# Canonical dimension orderings applied via `Collection.sort_choices` after the build.
CONFLICT_TYPE_ORDER = [
    CT.ALL_ARMED,
    CT.ALL_STATE_BASED,
    CT.INTERSTATE,
    CT.INTRASTATE,
    CT.EXTRASTATE,
    CT.NON_STATE,
    CT.ONE_SIDED,
]
CONFLICT_SUB_TYPE_ORDER = [
    CST.NA,
    CST.BY_SUB_TYPE,
    CST.ALL_SUB_TYPES,
    CST.ONLY_NON_INTERNATIONALIZED,
    CST.ONLY_INTERNATIONALIZED,
    CST.ONLY_WARS,
]


# ===========================================================================
# SourceSpec — everything source-specific lives here
# ===========================================================================


@dataclass
class SourceSpec:
    """Declarative description of one data source.

    Most fields default to UCDP-style naming; override per source. Use
    `_intrastate_int` / `_intrastate_non_int` as helper conflict_type slugs in
    `ct_map` for the internationalized sub-types — these get remapped to
    `intrastate_conflicts` after by_sub_type stacks are built.
    """

    # Identity ---------------------------------------------------------------
    slug: str  # data_source dim choice (e.g. "ucdp")
    name: str  # display name (e.g. "Uppsala Conflict Data Program")
    dataset_path: str  # path passed to paths.load_dataset()

    # Tables -----------------------------------------------------------------
    main_table: str  # deaths / counts / rates
    country_table: str | None = None  # for conflict_participants
    locations_table: str | None = None  # for conflict_locations

    # Column family base names. Override only when the source deviates from the
    # default UCDP-style naming (e.g. PRIO appends "_battle" to the deaths
    # family; COW-MID uses "disputes" instead of "conflicts").
    deaths_family: str = "number_deaths_ongoing_conflicts"
    new_conflicts_family: str = "number_new_conflicts"
    ongoing_conflicts_family: str = "number_ongoing_conflicts"

    # Subset of the explorer's six measures this source contributes.
    measures: set[str] = field(default_factory=set)

    # Map: table conflict_type value (e.g. "interstate") → explorer slug.
    # Unrecognised table values cause the column to be dropped.
    # TODO: Clarify this field, i don't understand.
    ct_map: dict[str, str] = field(default_factory=dict)

    # by_sub_type structure: parent CT slug → ordered list of (child_slug, label).
    # Children are stacked bottom-to-top.
    by_sub_type_labels: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    # Which measures support by_sub_type for each parent CT.
    by_sub_type_measures: dict[str, set[str]] = field(default_factory=dict)

    # `sub_measure` for deaths views. UCDP overrides to "country_and_region_data"
    # (its deaths data is split across the main + locations tables); everyone
    # else uses just "regional_data".
    deaths_sub_measure: str = "regional_data"

    # Per-CT name dicts used in titles + display labels. Defaults from
    # CT_NAME / CT_SHORT, overlaid with the spec's own entries in
    # `__post_init__`. COW uses this to swap "conflicts" → "wars".
    ct_name: dict[str, str] = field(default_factory=dict)
    ct_short: dict[str, str] = field(default_factory=dict)

    # FAUST ------------------------------------------------------------------
    # Noun used in deaths titles. PRIO/UCDP say "Deaths"; "Battle deaths" would
    # surface here for any source that adopts the legacy PRIO wording later.
    deaths_noun: str = "Deaths"
    # Whose deaths the source counts. UCDP / UCDP+PRIO / PRIO count combatants
    # *and* civilians; MARS and COW count only combatants.
    deaths_subjects: str = "combatants and civilians"
    # Cause clause in deaths subtitles ("due to {deaths_cause}"). COW adds
    # "disease, and starvation"; everyone else is just "fighting".
    deaths_cause: str = "fighting"
    # Plural noun for a single conflict event, used in count/rate subtitles and
    # regional notes ("number of {unit_plural} divided…", "Some {unit_plural}
    # affect several regions…"). COW / MARS say "wars".
    unit_plural: str = "conflicts"
    # Whether the source's deaths carry a labelled center estimate (best/low/high)
    # → drives the "'Best' estimates as identified by X." note. COW (single) and
    # MARS (low/high only) don't.
    deaths_note_best: bool = True
    # Whether count/rate by_sub_type views carry the "Some … affect several
    # regions" note. MARS data isn't region-split, so it doesn't.
    count_note_regions: bool = True
    # Per-view note overrides for count/rate views where the source's note
    # doesn't follow the regional-note rule (COW attaches "The number of states
    # has increased…" to specific views). Keyed by
    # (conflict_type, measure, conflict_sub_type, sub_measure); when present it
    # replaces the default note.
    count_note_override: dict[tuple[str, str, str, str], str] = field(default_factory=dict)
    # CT slug → DoD-link markdown (e.g. "[interstate conflicts](#dod:interstate-ucdp)").
    dod: dict[str, str] = field(default_factory=dict)
    # CT slug (parent) → combined DoD link used in by_sub_type subtitles.
    dod_by_sub_type: dict[str, str] = field(default_factory=dict)
    # DoD anchor (full markdown link, "#dod:...") used in the conflict_participants
    # subtitle ("…states that were [primary participants](…)").
    dod_primary_participant: str = DOD_PRIMARY_PARTICIPANT
    # conflict_participants subtitle shape. "named": "states that were
    # [{participant_role}](dod) in at least one …" (UCDP/MARS). "plain":
    # "states that participated in at least one …" (COW, no role link).
    participant_style: Literal["named", "plain"] = "named"
    participant_role: str = "primary participants"  # MARS: "major participants"
    # When True, all_state_based participants reference the singular of
    # `dod[ALL_STATE_BASED]` (MARS → "conventional war") instead of the
    # interstate/intrastate/extrastate list.
    participant_all_state_based_single: bool = False
    # conflict_locations subtitle verb clause. UCDP: "caused at least one death
    # in the country that year"; COW: "were ongoing that year".
    locations_verb: str = "caused at least one death in the country that year"
    # Override the conflict-type name used in conflict_locations titles (COW's
    # locations data only covers interstate + intrastate, so its all_state_based
    # locations read "interstate or intrastate wars" rather than "state-based wars").
    locations_name: dict[str, str] = field(default_factory=dict)
    # Whether country-level (map) views carry the "Some states are not shown in
    # the map because they do not exist anymore." note. True for sources with
    # historical states (COW, MARS, MIE, COW-MID).
    map_note_missing_states: bool = False
    # Source name used in the "'Best' estimates as identified by X." note.
    # Most sources mirror UCDP's wording so the default is fine.
    # TODO: where is this surfaced?
    ci_estimate_source: str = "UCDP"

    # Map-tab + per-best colorScale settings for deaths views, keyed by
    # (conflict_type, conflict_sub_type).
    deaths_map_views: set[tuple[str, str]] = field(default_factory=set)
    deaths_map_with_cs: set[tuple[str, str]] = field(default_factory=set)

    # ------------------------------------------------------------------------
    # MIC family (MIE / COW-MID) — interstate-only sources whose `conflict_sub_type`
    # carries a hostility-level split instead of an intrastate split. Defaults
    # apply to UCDP-style sources; MIC specs set `style="mic"` and the MIC
    # builder branch is taken.
    # ------------------------------------------------------------------------
    style: Literal["ucdp", "mic"] = "ucdp"
    # Deaths kind for MIC sources: "low_high" (MIE — 2-indicator range plot)
    # or "fatality_stack" (COW-MID — 8-indicator stacked bar). Ignored for
    # UCDP-style sources, which always use the CI (best/low/high) shape.
    mic_deaths_kind: Literal["low_high", "fatality_stack"] | None = None
    # DoD anchor URLs for the MIC subtitles ("force was threatened, displayed,
    # used" / "interstate wars"). The templates wrap each URL with the right
    # link text per usage site.
    mic_dod_force_url: str = ""
    mic_dod_war_url: str = ""

    def __post_init__(self) -> None:
        # Overlay the spec's partial CT name dicts onto the module defaults so
        # callers can just read `spec.ct_name[ct]` without falling back.
        self.ct_name = {**CT_NAME, **self.ct_name}
        self.ct_short = {**CT_SHORT, **self.ct_short}


# ===========================================================================
# Dimension assignment (column → explorer dims)
# ===========================================================================


def _conflict_sub_type(measure: str, ctype: str) -> str:
    """Return the `conflict_sub_type` value for a non-stacked / non-CI view."""
    if ctype == CT._INTRA_INT:
        return CST.ONLY_INTERNATIONALIZED
    if ctype == CT._INTRA_NON_INT:
        return CST.ONLY_NON_INTERNATIONALIZED
    if measure in (M.DEATHS, M.DEATH_RATE, M.N_CONFLICTS, M.CONFLICT_RATE):
        return CST.ALL_SUB_TYPES if ctype in PARENT_CTS else CST.NA
    # locations / participants: only intrastate behaves as a parent.
    return CST.ALL_SUB_TYPES if ctype == CT.INTRASTATE else CST.NA


def _parse_main_col(spec: SourceSpec, short: str, dims_raw: dict[str, Any]) -> dict[str, Any] | None:
    """Map a `main_table` column to explorer dim values; return None to drop."""
    ct_raw = dims_raw.get("conflict_type")
    ctype = spec.ct_map.get(ct_raw)
    if ctype is None:
        return None

    # Deaths / death-rate families (CI: best/low/high × _per_capita).
    deaths_variants = {
        spec.deaths_family: (M.DEATHS, "best"),
        f"{spec.deaths_family}_low": (M.DEATHS, "low"),
        f"{spec.deaths_family}_high": (M.DEATHS, "high"),
        f"{spec.deaths_family}_per_capita": (M.DEATH_RATE, "best"),
        f"{spec.deaths_family}_low_per_capita": (M.DEATH_RATE, "low"),
        f"{spec.deaths_family}_high_per_capita": (M.DEATH_RATE, "high"),
    }
    if short in deaths_variants:
        measure, estimate = deaths_variants[short]
        if measure not in spec.measures:
            return None
        # Intrastate-variant helpers carry sub_measure="na" — that's how the
        # legacy explorer labels the only_*_internationalized views.
        sub_measure = "na" if ctype in INTRASTATE_VARIANT_REMAP else spec.deaths_sub_measure
        return _dim_dict(measure, ctype, sub_measure, estimate)

    # Count / rate families. Only interstate-ongoing uses per_country_pair;
    # everything else uses per_country (or no suffix for plain counts).
    for is_ongoing, family in (
        (True, spec.ongoing_conflicts_family),
        (False, spec.new_conflicts_family),
    ):
        sub_measure = "all_ongoing_conflicts" if is_ongoing else "only_new_conflicts"

        if short == family:
            return _maybe_dim_dict(spec, M.N_CONFLICTS, ctype, sub_measure)

        if short == f"{family}_per_country_pair":
            # Only kept for interstate-ongoing rates; drop the rest.
            if not (is_ongoing and ctype == CT.INTERSTATE):
                return None
            return _maybe_dim_dict(spec, M.CONFLICT_RATE, ctype, sub_measure)

        if short == f"{family}_per_country":
            # Interstate-ongoing rate uses the per_country_pair variant instead.
            if ctype == CT.INTERSTATE and is_ongoing:
                return None
            return _maybe_dim_dict(spec, M.CONFLICT_RATE, ctype, sub_measure)

    return None  # any other column family (civilians/combatants/unknown, etc.)


def _parse_country_col(spec: SourceSpec, short: str, dims_raw: dict[str, Any]) -> dict[str, Any] | None:
    """Map a `country_table` column (conflict_participants) to explorer dims."""
    ct_raw = dims_raw.get("conflict_type")
    ctype = spec.ct_map.get(ct_raw)
    if ctype is None or ctype == CT.NON_STATE:
        return None  # non-state has no "primary state participant" semantics
    if M.PARTICIPANTS not in spec.measures:
        return None

    if short == "number_participants":
        return _maybe_dim_dict(spec, M.PARTICIPANTS, ctype, "regional_data")
    if short == "participated_in_conflict":
        return _maybe_dim_dict(spec, M.PARTICIPANTS, ctype, "country_level_data")
    return None


def _parse_locations_col(spec: SourceSpec, short: str, dims_raw: dict[str, Any]) -> dict[str, Any] | None:
    """Map a `locations_table` column (conflict_locations) to explorer dims."""
    ct_raw = dims_raw.get("conflict_type")
    ctype = spec.ct_map.get(ct_raw)
    if ctype is None:
        return None
    if M.LOCATIONS not in spec.measures:
        return None

    if short == "number_locations":
        return _maybe_dim_dict(spec, M.LOCATIONS, ctype, "regional_data")
    if short == "is_location_of_conflict":
        return _maybe_dim_dict(spec, M.LOCATIONS, ctype, "country_level_data")
    return None


def _parse_mic_main_col(spec: SourceSpec, short: str, dims_raw: dict[str, Any]) -> dict[str, Any] | None:
    """Map a MIE / COW-MID main_table column to explorer dims.

    Columns carry source-specific helper dims (`hostility_level` for MIE,
    `hostility` + `fatality` for COW-MID). Output dims align with the rest
    of the explorer: hostility levels become helper `conflict_sub_type` slugs
    that get stacked into `by_sub_type` by `_build_mic_post_process`; fatality
    buckets become helper `_estimate` slugs that get folded into the deaths
    stack by the universal CI-collapse step.
    """
    hostility_raw = dims_raw.get("hostility_level") or dims_raw.get("hostility")
    fatality_raw = dims_raw.get("fatality")

    # COW-MID fatality-bucket deaths columns: only the plain ongoing-counts
    # × hostility=all family contributes (per_country / per_country_pair
    # variants exist in the source but aren't surfaced).
    if fatality_raw is not None and fatality_raw != "all":
        if short != spec.ongoing_conflicts_family or hostility_raw != "all":
            return None
        if M.DEATHS not in spec.measures:
            return None
        helper = FATALITY_RAW_TO_HELPER.get(fatality_raw)
        if helper is None:
            return None
        return {
            "measure": M.DEATHS,
            "conflict_type": CT.INTERSTATE,
            "conflict_sub_type": CST.NA,
            "sub_measure": spec.deaths_sub_measure,
            "_estimate": helper,
        }

    # MIE deaths CI columns (low/high, with optional _per_capita for death_rate).
    deaths_variants = {
        f"{spec.deaths_family}_low": (M.DEATHS, "low"),
        f"{spec.deaths_family}_high": (M.DEATHS, "high"),
        f"{spec.deaths_family}_low_per_capita": (M.DEATH_RATE, "low"),
        f"{spec.deaths_family}_high_per_capita": (M.DEATH_RATE, "high"),
    }
    if short in deaths_variants:
        if hostility_raw != "all":
            return None
        measure, estimate = deaths_variants[short]
        if measure not in spec.measures:
            return None
        return {
            "measure": measure,
            "conflict_type": CT.INTERSTATE,
            "conflict_sub_type": CST.NA,
            "sub_measure": spec.deaths_sub_measure,
            "_estimate": estimate,
        }

    # Count / rate columns — hostility=all → all_sub_types, otherwise a helper
    # _hostlev_<level> slug that gets stacked into `by_sub_type` later.
    if hostility_raw is None:
        return None
    if hostility_raw == "all":
        sub_type = CST.ALL_SUB_TYPES
    else:
        helper_label = HOSTLEV_RAW_TO_HELPER.get(hostility_raw)
        if helper_label is None:
            return None
        sub_type = helper_label[0]

    for is_ongoing, family in (
        (True, spec.ongoing_conflicts_family),
        (False, spec.new_conflicts_family),
    ):
        sub_measure = "all_ongoing_conflicts" if is_ongoing else "only_new_conflicts"
        if short == family:
            return _mic_count_dim_dict(spec, M.N_CONFLICTS, sub_type, sub_measure)
        if short == f"{family}_per_country_pair":
            return _mic_count_dim_dict(spec, M.CONFLICT_RATE, sub_type, sub_measure)
        # _per_country variants exist in the source but aren't surfaced.
    return None


def _parse_mic_country_col(spec: SourceSpec, short: str, dims_raw: dict[str, Any]) -> dict[str, Any] | None:
    """Map a MIE / COW-MID country_table column (conflict_participants) to explorer dims.

    Only the aggregate `hostlev=all` columns are surfaced; the per-hostility
    variants exist in the source but the legacy explorer doesn't surface them.
    """
    hostlev_raw = dims_raw.get("hostlev")
    if hostlev_raw != "all":
        return None
    if M.PARTICIPANTS not in spec.measures:
        return None
    if short == "number_participants":
        return _mic_count_dim_dict(spec, M.PARTICIPANTS, CST.NA, "regional_data")
    if short == "participated_in_conflict":
        return _mic_count_dim_dict(spec, M.PARTICIPANTS, CST.NA, "country_level_data")
    return None


def _mic_count_dim_dict(spec: SourceSpec, measure: str, sub_type: str, sub_measure: str) -> dict[str, str] | None:
    """Dim assignment for a non-CI MIC view (count / rate / participants)."""
    if measure not in spec.measures:
        return None
    return {
        "measure": measure,
        "conflict_type": CT.INTERSTATE,
        "conflict_sub_type": sub_type,
        "sub_measure": sub_measure,
        "_estimate": "best",
    }


def _dim_dict(measure: str, ctype: str, sub_measure: str, estimate: str) -> dict[str, str]:
    """Build the dimension-assignment dict consumed by `_adjust_table`."""
    return {
        "measure": measure,
        "conflict_type": ctype,
        "conflict_sub_type": _conflict_sub_type(measure, ctype),
        "sub_measure": sub_measure,
        "_estimate": estimate,
    }


def _maybe_dim_dict(spec: SourceSpec, measure: str, ctype: str, sub_measure: str) -> dict[str, str] | None:
    """Same as `_dim_dict` for non-CI measures, gated on the spec carrying this measure.

    Non-CI columns carry `_estimate="best"` — semantically the single available
    value IS the best estimate, and using the same value as the CI columns'
    center lets `group_views` collapse all `_estimate` choices uniformly (and
    `drop_dimensions_if_single_choice` then auto-drops the now-trivial dim).
    """
    if measure not in spec.measures:
        return None
    return _dim_dict(measure, ctype, sub_measure, "best")


def _adjust_table(tb: Table, spec: SourceSpec, parse: Callable) -> Table:
    """Rewrite each column's metadata so create_collection picks up explorer dims."""
    drops: list[str] = []
    for col in list(tb.columns):
        if col in ("year", "country"):
            continue
        meta = tb[col].metadata
        dims_raw = dict(meta.dimensions or {})
        if not meta.original_short_name:
            drops.append(col)
            continue
        dims = parse(spec, meta.original_short_name, dims_raw)
        if dims is None:
            drops.append(col)
            continue
        meta.original_short_name = dims.pop("measure")
        meta.dimensions = dims
    if drops:
        tb = tb.drop(columns=drops)
    # Make sure the table-level dim list mentions the helper dims we added.
    if isinstance(tb.metadata.dimensions, list):
        existing = {d.get("slug") for d in tb.metadata.dimensions}
        for slug in ("conflict_type", "conflict_sub_type", "sub_measure", "_estimate"):
            if slug not in existing:
                tb.metadata.dimensions.append({"name": slug, "slug": slug})
    return tb


def _load_and_adjust(spec: SourceSpec) -> list[Table]:
    """Load every table the spec references and apply `_adjust_table` to each."""
    ds = paths.load_dataset(spec.dataset_path)
    if spec.style == "mic":
        main_parser: Callable = _parse_mic_main_col
        country_parser: Callable = _parse_mic_country_col
    else:
        main_parser = _parse_main_col
        country_parser = _parse_country_col
    tables: list[Table] = [_adjust_table(ds.read(spec.main_table, load_data=False), spec, main_parser)]
    if spec.country_table:
        tables.append(_adjust_table(ds.read(spec.country_table, load_data=False), spec, country_parser))
    if spec.locations_table:
        # MIC sources don't surface conflict_locations.
        assert spec.style != "mic", "MIC sources don't support a locations_table"
        tables.append(_adjust_table(ds.read(spec.locations_table, load_data=False), spec, _parse_locations_col))
    return tables


# ===========================================================================
# by_sub_type stacks (built manually rather than via group_views)
# ===========================================================================
#
# Children of a parent CT carry heterogeneous `conflict_sub_type` and
# `sub_measure` values (atomic types use "na" / regional_data, intrastate
# variants use "na" / "na" because the legacy explorer labels them that way),
# so `group_views(dimension="conflict_type", choices=[...])` can't find a
# matching cross-product to combine. We build the stacks explicitly instead.


def _build_by_sub_type_views(c, spec: SourceSpec) -> None:
    """Stack the children declared in `spec.by_sub_type_labels` into new views."""
    # Index existing views by (measure, conflict_type, sub_measure). For deaths
    # we also keep a "loose" index that ignores sub_measure — intrastate-variant
    # helpers carry sub_measure="na" but feed into stacks at the parent's
    # `deaths_sub_measure`.
    by_path: dict[tuple, list] = {}
    by_path_loose: dict[tuple, list] = {}
    for v in c.views:
        d = v.dimensions
        inds = list(v.indicators.y or [])
        by_path[(d["measure"], d["conflict_type"], d["sub_measure"])] = inds
        loose_key = (d["measure"], d["conflict_type"])
        if loose_key not in by_path_loose or d["sub_measure"] == "country_and_region_data":
            by_path_loose[loose_key] = inds

    new_views: list[View] = []
    for parent, children in spec.by_sub_type_labels.items():
        for measure in spec.by_sub_type_measures.get(parent, set()):
            for sub_measure in _by_sub_type_output_sub_measures(spec, measure):
                stacked = _stacked_children(measure, sub_measure, children, by_path, by_path_loose)
                if not stacked:
                    continue
                new_views.append(
                    View(
                        dimensions={
                            "measure": measure,
                            "conflict_type": parent,
                            "conflict_sub_type": CST.BY_SUB_TYPE,
                            "sub_measure": sub_measure,
                        },
                        indicators=ViewIndicators(y=stacked),
                        config={},
                    )
                )
    c.views.extend(new_views)


def _by_sub_type_output_sub_measures(spec: SourceSpec, measure: str) -> list[str]:
    """The `sub_measure` values to emit a by_sub_type view for, given a measure."""
    if measure in (M.DEATHS, M.DEATH_RATE):
        return [spec.deaths_sub_measure]
    return ["all_ongoing_conflicts", "only_new_conflicts"]


def _stacked_children(
    measure: str,
    sub_measure: str,
    children: list[tuple[str, str]],
    by_path: dict[tuple, list],
    by_path_loose: dict[tuple, list],
) -> list[Indicator]:
    """Pick one indicator per child (best estimate where available) and label it."""
    out: list[Indicator] = []
    for child_ct, label in children:
        # Lookup the child's view's indicators. For deaths, fall back to the
        # loose index because intrastate-variant helpers carry sub_measure="na".
        inds = by_path.get((measure, child_ct, sub_measure))
        if inds is None and measure in (M.DEATHS, M.DEATH_RATE):
            inds = by_path_loose.get((measure, child_ct), [])
        if not inds:
            continue

        if measure in (M.DEATHS, M.DEATH_RATE) and len(inds) > 1:
            # CI-stacked deaths view → pick the best estimate (or low as
            # fallback for sources like MARS that don't carry a center).
            pick = _pick_best_or_low(inds)
            if pick is None:
                continue
            out.append(Indicator(catalogPath=pick.catalogPath, display={"name": label}))
        else:
            # Single-indicator view → take its only indicator (or each, if multiple).
            for i in inds:
                out.append(Indicator(catalogPath=i.catalogPath, display={"name": label}))
    return out


def _pick_best_or_low(inds: list[Indicator]) -> Indicator | None:
    """Pick the center estimate; fall back to the low estimate if no center exists."""
    center = next(
        (i for i in inds if "_low_" not in i.catalogPath and "_high_" not in i.catalogPath),
        None,
    )
    if center is not None:
        return center
    return next((i for i in inds if "_low_" in i.catalogPath), None)


# ---------------------------------------------------------------------------
# MIC (MIE / COW-MID) post-process — hostility-level stacking.
# ---------------------------------------------------------------------------


def _drop_dim(c, dim_slug: str) -> None:
    """Remove a dimension and its references from every view. No-op if absent."""
    dim = next((d for d in c.dimensions if d.slug == dim_slug), None)
    if dim is None:
        return
    c.dimensions.remove(dim)
    for view in c.views:
        view.dimensions.pop(dim_slug, None)


def _build_mic_post_process(c, spec: SourceSpec) -> None:
    """Collapse the 4 hostility-level helper conflict_sub_type slugs into
    `by_sub_type` (stacked) and `only_wars` (war-only).

    Each (measure, sub_measure) combo carries 5 views after CI collapse:
    `all_sub_types` (aggregate) + 4 helper `_hostlev_*` views. We stack the
    helpers into a single `by_sub_type` view, then rename `_hostlev_war` to
    `only_wars` to surface that one as a standalone view; the other three
    helpers are dropped.
    """
    # 1. Stack the 4 hostility-level helper slugs into `by_sub_type`. Use
    #    replace=False so the helper-bearing views survive — we still need
    #    `_hostlev_war` for the rename, and we explicitly drop the others below.
    #    `drop_dimensions_if_single_choice=False` keeps `conflict_type` (which
    #    has a single choice for MIC sources) so the dim survives for the
    #    combined-explorer merge.
    c.group_views(
        groups=[
            {
                "dimension": "conflict_sub_type",
                "choices": [
                    CST._HOSTLEV_THREAT,
                    CST._HOSTLEV_DISPLAY,
                    CST._HOSTLEV_USE,
                    CST._HOSTLEV_WAR,
                ],
                "choice_new_slug": CST.BY_SUB_TYPE,
                "replace": False,
            }
        ],
        drop_dimensions_if_single_choice=False,
    )
    # 2. Surface the war helper as the canonical `only_wars` view.
    c.rename_choice_slug("conflict_sub_type", CST._HOSTLEV_WAR, CST.ONLY_WARS, dedup_slug="inherit")
    # 3. Drop the remaining helper hostility-level views — only the
    #    by_sub_type stack and only_wars survive.
    c.drop_views(
        [
            {
                "conflict_sub_type": [
                    CST._HOSTLEV_THREAT,
                    CST._HOSTLEV_DISPLAY,
                    CST._HOSTLEV_USE,
                ]
            }
        ]
    )


# ===========================================================================
# Collection-level helpers
# ===========================================================================


# ===========================================================================
# FAUST templates (titles, subtitles, notes, displays)
# ===========================================================================


def _dod_url(link: str) -> str:
    """Extract the URL portion of a DoD markdown link.

    `"[interstate conflicts](#dod:interstate-ucdp)"` → `"#dod:interstate-ucdp"`.
    """
    open_paren = link.rfind("](")
    if open_paren < 0 or not link.endswith(")"):
        return ""
    return link[open_paren + 2 : -1]


def _dod_label(link: str) -> str:
    """Extract the first word of a DoD link's bracket text.

    `"[extrastate wars](#dod:extrastate-war-cow)"` → `"extrastate"`.
    """
    start = link.find("[")
    end = link.find("]")
    if start < 0 or end < 0:
        return ""
    return link[start + 1 : end].split()[0]


def _set_view_config(view, spec: SourceSpec) -> None:
    """Fill in `title` / `subtitle` / `note` / chart-config blocks per view."""
    d = view.dimensions
    measure = d["measure"]
    ctype = d["conflict_type"]
    cst = d["conflict_sub_type"]
    sub_measure = d["sub_measure"]
    cfg = view.config or {}

    if spec.style == "mic":
        _set_view_config_mic(cfg, spec, measure, cst, sub_measure)
    else:
        if measure in (M.DEATHS, M.DEATH_RATE):
            _deaths_text(cfg, spec, measure, ctype, cst)
        elif measure in (M.N_CONFLICTS, M.CONFLICT_RATE):
            _count_text(cfg, spec, measure, ctype, cst, sub_measure)
        elif measure == M.PARTICIPANTS:
            _participants_text(cfg, spec, ctype, sub_measure)
        elif measure == M.LOCATIONS:
            _locations_text(cfg, spec, ctype, sub_measure)

    # Pin the map column to the best estimate on CI-stacked deaths / death_rate
    # maps. Without this, grapher picks the mapped column from the (unstable)
    # indicator order and may land on the low/high estimate — which also drops
    # the colorScale, since only the best indicator carries it.
    if cfg.get("hasMapTab") and measure in (M.DEATHS, M.DEATH_RATE) and view.indicators and view.indicators.y:
        best = _pick_best_or_low(view.indicators.y)
        if best is not None:
            cfg.setdefault("map", {})["columnSlug"] = best.catalogPath

    view.config = cfg


def _deaths_text(cfg: dict[str, Any], spec: SourceSpec, measure: str, ctype: str, cst: str) -> None:
    per_capita = measure == M.DEATH_RATE
    noun = "Death rate" if per_capita else spec.deaths_noun
    name = spec.ct_name[ctype]
    # "based on where they occurred" only when deaths are resolved to the
    # country of occurrence (UCDP's country_and_region_data). Regional-only
    # sources (COW/MARS/PRIO) omit it.
    # "based on where they occurred" marks deaths attributed to the country of
    # occurrence. For atomic / aggregate views it tracks deaths_map_views
    # membership (so UCDP extrastate — regional-only — is excluded). For
    # by_sub_type stacks it tracks `located` (the parent aggregates country-
    # resolved children), which only UCDP (country_and_region_data) is.
    located = spec.deaths_sub_measure == "country_and_region_data"
    has_map = (ctype, cst) in spec.deaths_map_views
    map_suffix = " based on where they occurred" if has_map else ""
    best_note = f"'Best' estimates as identified by {spec.ci_estimate_source}." if spec.deaths_note_best else None

    # One-sided violence: victims are civilians, "from" (not "due to fighting
    # in"), singular "was". Only UCDP carries it; never has by_sub_type.
    if ctype == CT.ONE_SIDED:
        dod = spec.dod[ctype]
        cfg["title"] = f"{noun} from one-sided violence{map_suffix}"
        if per_capita:
            cfg["subtitle"] = f"Deaths of civilians from {dod} that was ongoing that year, per 100,000 people."
        else:
            cfg["subtitle"] = f"Included are deaths of civilians from {dod} that was ongoing that year."
        if best_note:
            cfg["note"] = best_note
        if has_map:
            cfg.update(MAP_CONFIG)
        else:
            cfg["selectedFacetStrategy"] = "entity"
        return

    if cst == CST.BY_SUB_TYPE:
        suffix = " based on where they occurred" if located else ""
        cfg["title"] = f"{noun} in {name}{suffix}"
        cfg["subtitle"] = _deaths_subtitle(spec, spec.dod_by_sub_type.get(ctype, spec.dod[ctype]), per_capita)
        cfg.update(STACKED_CONFIG)
        return

    if cst in (CST.ONLY_NON_INTERNATIONALIZED, CST.ONLY_INTERNATIONALIZED):
        word = "non-internationalized" if cst == CST.ONLY_NON_INTERNATIONALIZED else "internationalized"
        intra_name = spec.ct_name[CT.INTRASTATE]  # "intrastate conflicts" / "intrastate wars"
        intrastate_anchor = _dod_url(spec.dod[CT.INTRASTATE])
        cfg["title"] = f"{noun} in {word} {intra_name}"
        cfg["subtitle"] = _deaths_subtitle(
            spec,
            f"[{word} {intra_name}]({intrastate_anchor})",
            per_capita,
        )
        if best_note:
            cfg["note"] = best_note
        cfg["selectedFacetStrategy"] = "entity"
        return

    # na / all_sub_types — single-indicator or CI-stacked deaths view.
    cfg["title"] = f"{noun} in {name}{map_suffix}"
    cfg["subtitle"] = _deaths_subtitle(spec, spec.dod[ctype], per_capita)
    if best_note:
        cfg["note"] = best_note
    if has_map:
        cfg.update(MAP_CONFIG)
    else:
        cfg["selectedFacetStrategy"] = "entity"


def _deaths_subtitle(spec: SourceSpec, dod_link: str, per_capita: bool) -> str:
    """Build the deaths/death-rate subtitle from a DoD-link fragment."""
    subjects = spec.deaths_subjects
    cause = spec.deaths_cause
    if per_capita:
        return (
            f"Deaths of {subjects} due to {cause}, per 100,000 people. "
            f"Included are {dod_link} that were ongoing that year."
        )
    return f"Included are deaths of {subjects} due to {cause} in {dod_link} that were ongoing that year."


def _count_text(cfg: dict[str, Any], spec: SourceSpec, measure: str, ctype: str, cst: str, sub_measure: str) -> None:
    rate = measure == M.CONFLICT_RATE
    is_new = sub_measure == "only_new_conflicts"
    name = spec.ct_name[ctype]
    new_prefix = "new " if is_new else ""
    lead = "Rate of" if rate else "Number of"
    verb = "started that year" if is_new else "were ongoing that year"
    note_override = spec.count_note_override.get((ctype, measure, cst, sub_measure))

    if cst == CST.BY_SUB_TYPE:
        cfg["title"] = f"{lead} {new_prefix}{name}"
        cfg["subtitle"] = _count_subtitle(spec, spec.dod_by_sub_type.get(ctype, spec.dod[ctype]), rate, verb)
        # An explicit per-view override wins; otherwise the regional note only
        # applies to aggregates across conflict types (all_armed /
        # all_state_based). Intrastate's by_sub_type is the internationalized
        # split of the same conflicts, so PROD omits the regional note there.
        if note_override is not None:
            cfg["note"] = note_override
        elif spec.count_note_regions and ctype in (CT.ALL_ARMED, CT.ALL_STATE_BASED):
            cfg["note"] = (
                f"Some {spec.unit_plural} affect several regions, and do not necessarily start at the same "
                "time across them. The sum across all regions can therefore be higher than the global number."
                if is_new
                else f"Some {spec.unit_plural} affect several regions. The sum across all regions can "
                "therefore be higher than the global number."
            )
        cfg.update(STACKED_CONFIG)
        return

    # One-sided violence: counted as discrete "one-sided conflicts" (you count
    # events, not "violence"), and grammatically singular ("Included is … that
    # was ongoing"). The DoD link text stays "one-sided violence".
    if ctype == CT.ONE_SIDED:
        sing_verb = "started that year" if is_new else "was ongoing that year"
        cfg["title"] = f"{lead} {new_prefix}one-sided conflicts"
        body = f"Included is {spec.dod[ctype]} that {sing_verb}."
        if rate:
            cfg["subtitle"] = (
                f"The number of {spec.unit_plural} divided by the number of all states. This accounts "
                f"for the changing number of states over time. {body}"
            )
        else:
            cfg["subtitle"] = body
        return

    cfg["title"] = f"{lead} {new_prefix}{name}"
    cfg["subtitle"] = _count_subtitle(spec, spec.dod[ctype], rate, verb, interstate=ctype == CT.INTERSTATE)
    if note_override is not None:
        cfg["note"] = note_override


def _count_subtitle(spec: SourceSpec, dod_link: str, rate: bool, verb: str, interstate: bool = False) -> str:
    """Build the number_of_conflicts / conflict_rate subtitle."""
    if not rate:
        return f"Included are {dod_link} that {verb}."
    # Interstate's ongoing rate uses a state-pair denominator (per_country_pair).
    denom = "all state-pairs" if interstate else "all states"
    return (
        f"The number of {spec.unit_plural} divided by the number of {denom}. This accounts for the changing "
        f"number of states over time. Included are {dod_link} that {verb}."
    )


def _participants_text(cfg: dict[str, Any], spec: SourceSpec, ctype: str, sub_measure: str) -> None:
    name = spec.ct_name[ctype]
    country_level = sub_measure == "country_level_data"

    # One-sided violence: "engaging in" / "primary perpetrators of at least one
    # instance of".
    if ctype == CT.ONE_SIDED:
        cfg["title"] = (
            "States engaging in one-sided violence"
            if country_level
            else "Number of states engaging in one-sided violence"
        )
        cfg["subtitle"] = (
            f"Included are states that were [primary perpetrators]({spec.dod_primary_participant}) "
            f"of at least one instance of {spec.dod[ctype]} that year."
        )
        if country_level:
            cfg.update(MAP_CONFIG)
            if spec.map_note_missing_states:
                cfg["note"] = MISSING_STATES_NOTE
        return

    cfg["title"] = f"States involved in {name}" if country_level else f"Number of states involved in {name}"

    # Singular form of the DoD link (UCDP "interstate conflicts" → "interstate
    # conflict", COW "interstate wars" → "interstate war").
    if ctype == CT.ALL_STATE_BASED and spec.participant_all_state_based_single:
        dod_sing = _singularize_dod(spec.dod[ctype])
    elif ctype == CT.ALL_STATE_BASED:
        dod_sing = _all_state_based_dod_singular(spec)
    else:
        dod_sing = _singularize_dod(spec.dod[ctype])

    if spec.participant_style == "plain":
        cfg["subtitle"] = f"Included are states that participated in at least one {dod_sing} that year."
    else:
        cfg["subtitle"] = (
            f"Included are states that were [{spec.participant_role}]({spec.dod_primary_participant}) "
            f"in at least one {dod_sing} that year."
        )
    if country_level:
        cfg.update(MAP_CONFIG)
        if spec.map_note_missing_states:
            cfg["note"] = MISSING_STATES_NOTE


def _singularize_dod(link: str) -> str:
    """ "[interstate wars](url)" → "[interstate war](url)"."""
    return link.replace("conflicts](", "conflict](").replace("wars](", "war](")


def _all_state_based_dod_singular(spec: SourceSpec) -> str:
    """Build the "interstate, intrastate, or extrastate war/conflict" fragment
    using the spec's per-CT DoD anchors, deriving each label from the spec's
    own link text (UCDP "extrasystemic" vs COW "extrastate")."""
    parts = []
    for ctype in (CT.INTERSTATE, CT.INTRASTATE, CT.EXTRASTATE):
        if ctype in spec.dod:
            label = _dod_label(spec.dod[ctype])
            parts.append(f"[{label}]({_dod_url(spec.dod[ctype])})")
    # Detect whether the source talks about "wars" instead of "conflicts".
    noun = "war" if spec.ct_name[CT.INTERSTATE].endswith("wars") else "conflict"
    if len(parts) >= 2:
        return ", ".join(parts[:-1]) + f", or {parts[-1]} {noun}"
    if parts:
        return f"{parts[0]} {noun}"
    return f"{noun}"


def _locations_text(cfg: dict[str, Any], spec: SourceSpec, ctype: str, sub_measure: str) -> None:
    name = spec.ct_name[ctype]
    country_level = sub_measure == "country_level_data"

    if ctype == CT.ONE_SIDED:
        cfg["title"] = (
            "Countries where one-sided violence took place"
            if country_level
            else "Number of countries where one-sided violence took place"
        )
        cfg["subtitle"] = f"Included is {spec.dod[ctype]} that {spec.locations_verb}."
    else:
        loc_name = spec.locations_name.get(ctype, name)
        cfg["title"] = (
            f"Countries where {loc_name} took place"
            if country_level
            else f"Number of countries where {loc_name} took place"
        )
        cfg["subtitle"] = f"Included are {spec.dod[ctype]} that {spec.locations_verb}."
    if country_level:
        cfg.update(MAP_CONFIG)
        if spec.map_note_missing_states:
            cfg["note"] = MISSING_STATES_NOTE


# ---------------------------------------------------------------------------
# FAUST for MIC sources (MIE / COW-MID).
# ---------------------------------------------------------------------------


def _set_view_config_mic(cfg: dict[str, Any], spec: SourceSpec, measure: str, cst: str, sub_measure: str) -> None:
    """Dispatch MIC view-config helpers by measure."""
    if measure in (M.DEATHS, M.DEATH_RATE):
        _mic_deaths_text(cfg, spec, measure)
    elif measure in (M.N_CONFLICTS, M.CONFLICT_RATE):
        _mic_count_text(cfg, spec, measure, cst, sub_measure)
    elif measure == M.PARTICIPANTS:
        _mic_participants_text(cfg, spec, sub_measure)


def _mic_deaths_text(cfg: dict[str, Any], spec: SourceSpec, measure: str) -> None:
    per_capita = measure == M.DEATH_RATE
    if per_capita:
        cfg["title"] = "Death rate in interstate conflicts"
        cfg["subtitle"] = "Deaths of combatants due to fighting between states that year, per 100,000 people."
    else:
        cfg["title"] = "Deaths in interstate conflicts"
        cfg["subtitle"] = "Included are deaths of combatants due to fighting between states that year."
    if spec.mic_deaths_kind == "fatality_stack":
        cfg.update(STACKED_CONFIG)
    else:
        # MIE low/high range plot.
        cfg["yScaleToggle"] = True
        cfg["selectedFacetStrategy"] = "entity"


def _mic_count_text(cfg: dict[str, Any], spec: SourceSpec, measure: str, cst: str, sub_measure: str) -> None:
    rate = measure == M.CONFLICT_RATE
    is_new = sub_measure == "only_new_conflicts"
    noun = "wars" if cst == CST.ONLY_WARS else "conflicts"
    prefix = "new " if is_new else ""
    lead = "Rate of" if rate else "Number of"
    cfg["title"] = f"{lead} {prefix}interstate {noun}"
    cfg["subtitle"] = _mic_count_subtitle(spec, cst, rate=rate, is_new=is_new)
    cfg["note"] = _mic_count_note(cst, is_new=is_new, rate=rate)
    if not cfg["note"]:
        cfg.pop("note", None)
    if cst == CST.BY_SUB_TYPE:
        cfg.update(STACKED_CONFIG)


def _mic_count_subtitle(spec: SourceSpec, cst: str, *, rate: bool, is_new: bool) -> str:
    """Build a MIC subtitle from the spec's force / war DoD URLs."""
    war_verb = "started that year" if is_new else "were ongoing that year"
    if cst == CST.ONLY_WARS:
        body = f"Included are [interstate wars]({spec.mic_dod_war_url}) that {war_verb}."
        if rate:
            return (
                "The number of wars divided by the number of all state-pairs. "
                f"This accounts for the changing number of states over time. {body}"
            )
        return body
    # all_sub_types / by_sub_type — same wording.
    suffix = "that year for the first time" if is_new else "that year"
    body = (
        f"Included are conflicts between states where force was "
        f"[threatened, displayed, used]({spec.mic_dod_force_url}), or escalated to a "
        f"[war]({spec.mic_dod_war_url}) {suffix}."
    )
    if rate:
        return (
            "The number of conflicts divided by the number of all state-pairs. "
            f"This accounts for the changing number of states over time. {body}"
        )
    return body


def _mic_count_note(cst: str, *, is_new: bool, rate: bool) -> str:
    """Build the MIC count-view note (matches the legacy YAML wording)."""
    if cst == CST.ONLY_WARS:
        if is_new or rate:
            return ""
        return (
            "Some wars affect several regions. The sum across all regions can therefore "
            "be higher than the global number."
        )
    if cst == CST.ALL_SUB_TYPES:
        if is_new:
            return ""
        if rate:
            return ""
        return (
            "The number of states has increased a lot over time. Some conflicts affect "
            "several regions. The sum across all regions can therefore be higher than the "
            "global number."
        )
    # by_sub_type
    if is_new:
        return (
            "Some conflicts affect several regions, and do not necessarily start at the same time "
            "across them. The sum across all regions can therefore be higher than the global number."
        )
    if rate:
        return (
            "Some conflicts affect several regions. The sum across all regions can therefore "
            "be higher than the global number."
        )
    return (
        "The number of states has increased a lot over time. Some conflicts affect "
        "several regions. The sum across all regions can therefore be higher than the "
        "global number."
    )


def _mic_participants_text(cfg: dict[str, Any], spec: SourceSpec, sub_measure: str) -> None:
    country_level = sub_measure == "country_level_data"
    cfg["title"] = (
        "States involved in interstate conflicts"
        if country_level
        else "Number of states involved in interstate conflicts"
    )
    cfg["subtitle"] = (
        "Included are states that participated in at least one conflict with another state "
        f"where force was [threatened, displayed, or used]({spec.mic_dod_force_url}) that year."
    )
    if country_level:
        cfg["note"] = MISSING_STATES_NOTE
        cfg.update(MAP_CONFIG)


def _set_view_displays(view, spec: SourceSpec) -> None:
    """Fill in per-indicator display (name / color / colorScale) blocks per view."""
    d = view.dimensions
    measure = d["measure"]
    ctype = d["conflict_type"]
    cst = d["conflict_sub_type"]
    sub_measure = d["sub_measure"]
    if view.indicators is None or view.indicators.y is None:
        return
    ys = view.indicators.y

    if spec.style == "mic":
        _set_view_displays_mic(ys, spec, measure, cst, sub_measure)
        return

    # by_sub_type stacks: labels are set by `_build_by_sub_type_views`.
    if cst == CST.BY_SUB_TYPE:
        return

    if measure in (M.DEATHS, M.DEATH_RATE):
        _set_deaths_displays(ys, spec, measure, ctype, cst)
    elif measure in (M.N_CONFLICTS, M.CONFLICT_RATE):
        _set_count_display(ys, spec, ctype)
    elif measure in (M.LOCATIONS, M.PARTICIPANTS):
        _set_locations_or_participants_displays(ys, measure, sub_measure=d["sub_measure"])


# ---------------------------------------------------------------------------
# MIC display helpers.
# ---------------------------------------------------------------------------

# Identify a hostility level from an indicator's catalogPath suffix.
# MIE columns carry `__hostility_level_<level>`; COW-MID columns carry
# `__hostility_<level>` (no "_level"). Both share the same set of labels.
_HOSTLEV_PATH_LABEL: list[tuple[str, str]] = [
    ("hostility_level_threat_to_use_force", "Threats of force"),
    ("hostility_level_display_of_force", "Displays of force"),
    ("hostility_level_use_of_force", "Uses of force"),
    ("hostility_level_war", "Wars"),
    ("hostility_threat_to_use_force", "Threats of force"),
    ("hostility_display_of_force", "Displays of force"),
    ("hostility_use_of_force", "Uses of force"),
    ("hostility_war", "Wars"),
]
# Identify a fatality bucket from an indicator's catalogPath. COW-MID's
# fatality column slug suffixes match these substrings exactly.
_FATALITY_PATH_LABEL: list[tuple[str, str]] = [
    ("fatality_unknown__", "No deaths data"),
    ("fatality_no_deaths__", "No deaths"),
    ("fatality_1_25_deaths__", "1-25 deaths"),
    ("fatality_26_100_deaths__", "26-100 deaths"),
    ("fatality_101_250_deaths__", "101-250 deaths"),
    ("fatality_251_500_deaths__", "251-500 deaths"),
    ("fatality_501_999_deaths__", "501-999 deaths"),
    ("fatality__gt__999_deaths__", "1000 deaths or more"),
]


def _set_view_displays_mic(
    ys: list[Indicator],
    spec: SourceSpec,
    measure: str,
    cst: str,
    sub_measure: str,
) -> None:
    """Dispatch MIC display helpers."""
    if measure == M.DEATHS:
        if spec.mic_deaths_kind == "fatality_stack":
            _sort_and_label_by_path(ys, _FATALITY_PATH_LABEL)
        else:  # low_high
            _set_mie_low_high(ys, per_capita=False)
    elif measure == M.DEATH_RATE:
        _set_mie_low_high(ys, per_capita=True)
    elif measure in (M.N_CONFLICTS, M.CONFLICT_RATE):
        if cst == CST.BY_SUB_TYPE:
            _sort_and_label_by_path(ys, _HOSTLEV_PATH_LABEL)
        elif cst == CST.ONLY_WARS:
            if ys:
                ys[0].display = {"name": "Wars"}
    elif measure == M.PARTICIPANTS and sub_measure == "country_level_data":
        for ind in ys:
            ind.display = {"colorScaleNumericBins": BOOL_MAP_BINS}


def _set_mie_low_high(ys: list[Indicator], per_capita: bool) -> None:
    """Apply MIE low/high colors (non-standard). The 2 indicators are stacked
    in the order the CI-collapse step gave us; we sort high above low so the
    deaths range plot reads top-down."""

    def kind(path: str) -> str:
        # MIE deaths column suffix: `_high__` / `_low__` (with `_per_capita`
        # inserted before `__hostility_level_all` for rates).
        if "_high__" in path or "_high_per_capita__" in path:
            return "high"
        if "_low__" in path or "_low_per_capita__" in path:
            return "low"
        return "other"

    ys.sort(key=lambda ind: 0 if kind(ind.catalogPath) == "high" else 1)
    for ind in ys:
        k = kind(ind.catalogPath)
        if k == "high":
            ind.display = dict(RANGE_HIGH_DISPLAY)
        elif k == "low":
            ind.display = dict(RANGE_LOW_DISPLAY)


def _sort_and_label_by_path(ys: list[Indicator], path_label_order: list[tuple[str, str]]) -> None:
    """Sort `ys` by the position of the first matching catalogPath substring
    in `path_label_order`, then apply the corresponding display name."""

    def rank(path: str) -> int:
        for i, (needle, _) in enumerate(path_label_order):
            if needle in path:
                return i
        return len(path_label_order)

    ys.sort(key=lambda ind: rank(ind.catalogPath))
    for ind in ys:
        for needle, label in path_label_order:
            if needle in ind.catalogPath:
                ind.display = {"name": label}
                break


def _set_deaths_displays(ys: list[Indicator], spec: SourceSpec, measure: str, ctype: str, cst: str) -> None:
    """Set displays on a deaths / death_rate view.

    Three shapes:
        - CI with center (UCDP-style: best / low / high) → red center + grey CI.
        - Range only (MARS-style: low / high, no center) → red high + blue low.
        - Single indicator (COW) → CT short label, no "Best estimate" tag.
    """
    has_ci_variants = any("_low_" in i.catalogPath or "_high_" in i.catalogPath for i in ys)
    if not has_ci_variants:
        short = spec.ct_short.get(ctype)
        if len(ys) == 1 and short:
            ys[0].display = {"name": short}
        return

    per_capita = measure == M.DEATH_RATE
    with_cs = (ctype, cst) in spec.deaths_map_with_cs
    has_center = any("_low_" not in i.catalogPath and "_high_" not in i.catalogPath for i in ys)
    for ind in ys:
        if "_low_" in ind.catalogPath:
            ind.display = dict(LOW_DISPLAY if has_center else RANGE_LOW_DISPLAY)
        elif "_high_" in ind.catalogPath:
            ind.display = dict(HIGH_DISPLAY if has_center else RANGE_HIGH_DISPLAY)
        else:
            disp = dict(BEST_DISPLAY)
            if with_cs:
                disp["colorScaleScheme"] = "OrRd"
                disp["colorScaleNumericBins"] = RATE_MAP_BINS if per_capita else DEATHS_MAP_BINS
            ind.display = disp


def _set_count_display(ys: list[Indicator], spec: SourceSpec, ctype: str) -> None:
    """Single-indicator number_of_conflicts / conflict_rate view → CT short label."""
    short = spec.ct_short.get(ctype)
    if len(ys) == 1 and short:
        ys[0].display = {"name": short}


def _set_locations_or_participants_displays(ys: list[Indicator], measure: str, sub_measure: str) -> None:
    """Locations always use the boolean colorScale; participants only at country level."""
    needs_cs = measure == M.LOCATIONS or sub_measure == "country_level_data"
    if not needs_cs:
        return
    for ind in ys:
        ind.display = {"colorScaleNumericBins": BOOL_MAP_BINS}


# ===========================================================================
# Universal build pipeline
# ===========================================================================


def build_source_explorer(spec: SourceSpec, sub_config: dict[str, Any]):
    """Build one sub-explorer for `spec` and return it.

    `sub_config` is the shared per-spec create_collection input — the YAML's
    dim definitions (minus `data_source`) plus the explorer's `definitions`
    block. It's identical across sources, so `run()` computes it once and
    passes it in.

    The returned explorer does not carry the `data_source` dim; that's added
    by `_attach_data_source_dim` (or by `combine_collections` in the future,
    once every source is built programmatically).
    """
    tables = _load_and_adjust(spec)

    # Indicator names per table, in the same order as `tables`.
    main_inds = [m for m in (M.DEATHS, M.DEATH_RATE, M.N_CONFLICTS, M.CONFLICT_RATE) if m in spec.measures]
    indicator_names: list[list[str]] = [main_inds]
    if spec.country_table:
        indicator_names.append([M.PARTICIPANTS])
    if spec.locations_table:
        indicator_names.append([M.LOCATIONS])

    c = paths.create_collection(
        config=sub_config,
        tb=tables,
        indicator_names=indicator_names,
        dimensions=["conflict_type", "conflict_sub_type", "sub_measure", "_estimate"],
        indicators_slug="measure",
        short_name=spec.slug,
        explorer=True,
    )

    # 1) CI collapse: merge all `_estimate` values into a single "_ci" stack.
    #    Non-CI columns carry `_estimate="best"` too, so a single group_views
    #    call handles every source — deaths views become CI / range / fatality
    #    stacks, non-CI views become 1-indicator (degenerate) "stacks".
    #    NOTE: we pass the explicit list of existing choices rather than
    #    omitting `choices` because group_views adds "_ci" to the dim *before*
    #    its replace-pass runs, and an omitted `choices` would then include
    #    "_ci" itself — wiping the new views we just created.
    #    We pass `drop_dimensions_if_single_choice=False` and drop `_estimate`
    #    explicitly below so we keep `conflict_type` for MIC sources (where it
    #    has a single choice but still needs to surface in the combined
    #    explorer for `combine_collections` to merge cleanly).
    estimate_in_use = list(c.dimension_choices_in_use().get("_estimate", set()))
    if "best" in estimate_in_use:
        estimate_in_use = ["best"] + [
            e for e in estimate_in_use if e != "best"
        ]  # "best" is the default, not a real choice
    c.group_views(
        groups=[{"dimension": "_estimate", "choices": estimate_in_use, "choice_new_slug": "_ci", "replace": True}],
        drop_dimensions_if_single_choice=False,
    )
    _drop_dim(c, "_estimate")

    # 2-4) Style-specific stacking and helper-slug cleanup.
    if spec.style == "mic":
        # MIC sources stack hostility-level helper slugs on `conflict_sub_type`
        # into a single `by_sub_type` view, then surface `_hostlev_war` as
        # `only_wars` and drop the rest.
        _build_mic_post_process(c, spec)
    else:
        # UCDP-style: manually build by_sub_type stacks across conflict_type
        # children, drop helper intrastate views for non-deaths measures, and
        # consolidate the remaining helper conflict_type slugs into
        # `intrastate_conflicts`.
        _build_by_sub_type_views(c, spec)

        ct_in_use = c.dimension_choices_in_use().get("conflict_type", set())
        helpers_in_use = [s for s in INTRASTATE_VARIANT_REMAP if s in ct_in_use]
        non_deaths_measures = [
            m for m in (M.N_CONFLICTS, M.CONFLICT_RATE, M.LOCATIONS, M.PARTICIPANTS) if m in spec.measures
        ]
        if helpers_in_use and non_deaths_measures:
            c.drop_views([{"conflict_type": helpers_in_use, "measure": non_deaths_measures}])

        declared_cts = set(c.get_dimension("conflict_type").choice_slugs)
        for old_ct, new_ct in INTRASTATE_VARIANT_REMAP.items():
            if old_ct in declared_cts:
                c.rename_choice_slug("conflict_type", old_ct, new_ct, dedup_slug="inherit")

    # 5) Drop any dim choices no longer referenced by a view, then enforce
    #    canonical ordering on the conflict_type / conflict_sub_type dims.
    c.prune_dimension_choices(["conflict_type", "conflict_sub_type"])
    c.sort_choices(
        {
            "conflict_type": CONFLICT_TYPE_ORDER,
            "conflict_sub_type": CONFLICT_SUB_TYPE_ORDER,
        }
    )

    # 6) Fill in titles / subtitles / notes / displays.
    for view in c.views:
        _set_view_config(view, spec)
        _set_view_displays(view, spec)

    return c


# ===========================================================================
# Per-source specs
# ===========================================================================

# ---- UCDP -----------------------------------------------------------------

UCDP_SPEC = SourceSpec(
    slug="ucdp",
    name="Uppsala Conflict Data Program",
    dataset_path="ucdp",
    main_table="ucdp",
    country_table="ucdp_country",
    locations_table="ucdp_locations",
    measures={
        M.DEATHS,
        M.DEATH_RATE,
        M.N_CONFLICTS,
        M.CONFLICT_RATE,
        M.LOCATIONS,
        M.PARTICIPANTS,
    },
    ct_map={
        "all": CT.ALL_ARMED,
        "state-based": CT.ALL_STATE_BASED,
        "interstate": CT.INTERSTATE,
        "intrastate": CT.INTRASTATE,
        "intrastate (internationalized)": CT._INTRA_INT,
        "intrastate (non-internationalized)": CT._INTRA_NON_INT,
        "extrasystemic": CT.EXTRASTATE,
        "non-state conflict": CT.NON_STATE,
        "one-sided violence": CT.ONE_SIDED,
    },
    deaths_sub_measure="country_and_region_data",
    by_sub_type_labels={
        CT.ALL_ARMED: [
            (CT.ONE_SIDED, "One-sided violence"),
            (CT.NON_STATE, "Non-state"),
            (CT.INTRASTATE, "Intrastate"),
            (CT.EXTRASTATE, "Extrasystemic"),
            (CT.INTERSTATE, "Interstate"),
        ],
        CT.ALL_STATE_BASED: [
            (CT._INTRA_INT, "Internationalized intrastate"),
            (CT._INTRA_NON_INT, "Non-internationalized intrastate"),
            (CT.EXTRASTATE, "Extrasystemic"),
            (CT.INTERSTATE, "Interstate"),
        ],
        CT.INTRASTATE: [
            (CT._INTRA_NON_INT, "Non-internationalized intrastate"),
            (CT._INTRA_INT, "Internationalized intrastate"),
        ],
    },
    by_sub_type_measures={
        CT.ALL_ARMED: {M.DEATHS, M.DEATH_RATE, M.N_CONFLICTS, M.CONFLICT_RATE},
        CT.ALL_STATE_BASED: {M.DEATHS, M.DEATH_RATE, M.N_CONFLICTS, M.CONFLICT_RATE},
        CT.INTRASTATE: {M.N_CONFLICTS, M.CONFLICT_RATE},
    },
    dod={
        CT.ALL_ARMED: f"[armed conflicts]({DOD_UCDP.ARMED})",
        CT.ALL_STATE_BASED: (
            f"[interstate]({DOD_UCDP.INTERSTATE}), "
            f"[intrastate]({DOD_UCDP.INTRASTATE}), and "
            f"[extrasystemic]({DOD_UCDP.EXTRASYSTEMIC}) conflicts"
        ),
        CT.INTERSTATE: f"[interstate conflicts]({DOD_UCDP.INTERSTATE})",
        CT.INTRASTATE: f"[intrastate conflicts]({DOD_UCDP.INTRASTATE})",
        CT.EXTRASTATE: f"[extrasystemic conflicts]({DOD_UCDP.EXTRASYSTEMIC})",
        CT.NON_STATE: f"[non-state conflicts]({DOD_UCDP.NONSTATE})",
        CT.ONE_SIDED: f"[one-sided violence]({DOD_UCDP.ONESIDED})",
    },
    dod_by_sub_type={
        CT.ALL_ARMED: (
            f"[interstate]({DOD_UCDP.INTERSTATE}), "
            f"[intrastate]({DOD_UCDP.INTRASTATE}), "
            f"[extrasystemic]({DOD_UCDP.EXTRASYSTEMIC}), "
            f"[non-state]({DOD_UCDP.NONSTATE}) conflicts, and "
            f"[one-sided violence]({DOD_UCDP.ONESIDED})"
        ),
        CT.ALL_STATE_BASED: (
            f"[interstate]({DOD_UCDP.INTERSTATE}), "
            f"[intrastate]({DOD_UCDP.INTRASTATE}), and "
            f"[extrasystemic]({DOD_UCDP.EXTRASYSTEMIC}) conflicts"
        ),
        CT.INTRASTATE: f"[non-internationalized and internationalized intrastate conflicts]({DOD_UCDP.INTRASTATE})",
    },
    deaths_map_views={
        (CT.ALL_ARMED, CST.ALL_SUB_TYPES),
        (CT.ALL_STATE_BASED, CST.ALL_SUB_TYPES),
        (CT.INTERSTATE, CST.NA),
        (CT.INTRASTATE, CST.ALL_SUB_TYPES),
        (CT.NON_STATE, CST.NA),
        (CT.ONE_SIDED, CST.NA),
    },
    # Aggregate (ALL_ARMED / ALL_STATE_BASED) views match the OrRd+bins config
    # the per-CT views already use — matches UCDP 2025-06-13's PROD grapher
    # chart config for these indicators. Our garden dataset is at 2023-09-21
    # (no `map.colorScale` on the indicators), so we inject the bins via the
    # explorer-display path.
    deaths_map_with_cs={
        (CT.ALL_ARMED, CST.ALL_SUB_TYPES),
        (CT.ALL_STATE_BASED, CST.ALL_SUB_TYPES),
        (CT.INTERSTATE, CST.NA),
        (CT.INTRASTATE, CST.ALL_SUB_TYPES),
        (CT.NON_STATE, CST.NA),
        (CT.ONE_SIDED, CST.NA),
    },
)


# ---- UCDP+PRIO ------------------------------------------------------------
# Same definitions as UCDP, deaths-only, no atomic conflict types in the
# explorer (no all_armed_conflicts, no non-state, no one-sided).

UCDP_PRIO_SPEC = SourceSpec(
    slug="ucdp_prio",
    name="UCDP + PRIO",
    dataset_path="ucdp_prio",
    main_table="ucdp_prio",
    measures={M.DEATHS, M.DEATH_RATE},
    ct_map={
        "state-based": CT.ALL_STATE_BASED,
        "interstate": CT.INTERSTATE,
        "intrastate": CT.INTRASTATE,
        "intrastate (internationalized)": CT._INTRA_INT,
        "intrastate (non-internationalized)": CT._INTRA_NON_INT,
        "extrasystemic": CT.EXTRASTATE,
    },
    by_sub_type_labels={
        CT.ALL_STATE_BASED: [
            (CT._INTRA_INT, "Internationalized intrastate"),
            (CT._INTRA_NON_INT, "Non-internationalized intrastate"),
            (CT.EXTRASTATE, "Extrasystemic"),
            (CT.INTERSTATE, "Interstate"),
        ],
    },
    by_sub_type_measures={CT.ALL_STATE_BASED: {M.DEATHS, M.DEATH_RATE}},
    # UCDP+PRIO uses UCDP's DoD anchors since it adopts the UCDP definitions.
    dod={
        CT.ALL_STATE_BASED: (
            f"[interstate]({DOD_UCDP.INTERSTATE}), "
            f"[intrastate]({DOD_UCDP.INTRASTATE}), and "
            f"[extrasystemic]({DOD_UCDP.EXTRASYSTEMIC}) conflicts"
        ),
        CT.INTERSTATE: f"[interstate conflicts]({DOD_UCDP.INTERSTATE})",
        CT.INTRASTATE: f"[intrastate conflicts]({DOD_UCDP.INTRASTATE})",
        CT.EXTRASTATE: f"[extrasystemic conflicts]({DOD_UCDP.EXTRASYSTEMIC})",
    },
    dod_by_sub_type={
        CT.ALL_STATE_BASED: (
            f"[interstate]({DOD_UCDP.INTERSTATE}), "
            f"[intrastate]({DOD_UCDP.INTRASTATE}), and "
            f"[extrasystemic]({DOD_UCDP.EXTRASYSTEMIC}) conflicts"
        ),
    },
)


# ---- MARS -----------------------------------------------------------------
# MARS only carries low/high estimates (no center). Its conflict_type values
# are unique: "civil war" and "others (non-civil)" map to intrastate /
# interstate respectively. No internationalized variants and no map tabs.

MARS_SPEC = SourceSpec(
    slug="mars",
    name="Project Mars",
    dataset_path="mars",
    main_table="mars",
    country_table="mars_country",
    deaths_subjects="combatants",
    unit_plural="wars",
    deaths_note_best=False,
    count_note_regions=False,
    participant_role="major participants",
    dod_primary_participant=DOD_MARS.MAJOR_PARTICIPANT,
    participant_all_state_based_single=True,
    map_note_missing_states=True,
    # MARS labels all state-based as "wars", interstate as "interstate wars",
    # and intrastate (civil war) as "civil wars".
    ct_name={
        CT.ALL_STATE_BASED: "wars",
        CT.INTERSTATE: "interstate wars",
        CT.INTRASTATE: "civil wars",
    },
    measures={
        M.DEATHS,
        M.DEATH_RATE,
        M.N_CONFLICTS,
        M.CONFLICT_RATE,
        M.PARTICIPANTS,
    },
    ct_map={
        "all": CT.ALL_STATE_BASED,
        "civil war": CT.INTRASTATE,
        "others (non-civil)": CT.INTERSTATE,
    },
    by_sub_type_labels={
        CT.ALL_STATE_BASED: [
            (CT.INTRASTATE, "Civil wars"),
            (CT.INTERSTATE, "Interstate wars"),
        ],
    },
    by_sub_type_measures={
        CT.ALL_STATE_BASED: {M.DEATHS, M.DEATH_RATE, M.N_CONFLICTS, M.CONFLICT_RATE},
    },
    dod={
        CT.ALL_STATE_BASED: f"[conventional wars]({DOD_MARS.CONVENTIONAL_WARS})",
        CT.INTERSTATE: f"[interstate wars]({DOD_MARS.INTERSTATE})",
        CT.INTRASTATE: f"[civil wars]({DOD_MARS.CIVIL_WAR})",
    },
    dod_by_sub_type={
        CT.ALL_STATE_BASED: f"[interstate]({DOD_MARS.INTERSTATE}) and [civil]({DOD_MARS.CIVIL_WAR}) wars",
    },
)


# ---- COW (Correlates of War – Wars) ---------------------------------------
# COW uses hyphenated CT slugs ("inter-state", "intra-state") and "wars"
# instead of "conflicts" in user-facing text. No CI on deaths.

COW_SPEC = SourceSpec(
    slug="cow",
    name="Correlates of War – Wars",
    dataset_path="cow",
    main_table="cow",
    country_table="cow_country",
    locations_table="cow_locations",
    deaths_subjects="combatants",
    deaths_cause="fighting, disease, and starvation",
    unit_plural="wars",
    deaths_note_best=False,
    participant_style="plain",
    locations_verb="were ongoing that year",
    locations_name={CT.ALL_STATE_BASED: "interstate or intrastate wars"},
    map_note_missing_states=True,
    # PROD attaches bespoke notes to a few COW count/rate views that don't
    # follow the regional-note rule (matched to PROD; see ai/ diff IDs).
    count_note_override={
        # ID 153 — adds the "states increased" prefix to the regional note.
        (CT.ALL_ARMED, M.CONFLICT_RATE, CST.BY_SUB_TYPE, "all_ongoing_conflicts"): (
            "The number of states has increased a lot over time. Some wars affect several regions. "
            "The sum across all regions can therefore be higher than the global number."
        ),
        # ID 163 — regional note on the (otherwise note-less) interstate count.
        (CT.INTERSTATE, M.N_CONFLICTS, CST.NA, "all_ongoing_conflicts"): (
            "Some wars affect several regions. The sum across all regions can therefore be higher than the global number."
        ),
        # ID 177 — "states increased" note on the intrastate by_sub_type count.
        (CT.INTRASTATE, M.N_CONFLICTS, CST.BY_SUB_TYPE, "all_ongoing_conflicts"): (
            "The number of states has increased a lot over time."
        ),
    },
    measures={
        M.DEATHS,
        M.DEATH_RATE,
        M.N_CONFLICTS,
        M.CONFLICT_RATE,
        M.LOCATIONS,
        M.PARTICIPANTS,
    },
    ct_map={
        "all": CT.ALL_ARMED,
        "state-based": CT.ALL_STATE_BASED,
        "inter-state": CT.INTERSTATE,
        "intra-state": CT.INTRASTATE,
        "intra-state (internationalized)": CT._INTRA_INT,
        "intra-state (non-internationalized)": CT._INTRA_NON_INT,
        "extra-state": CT.EXTRASTATE,
        "non-state": CT.NON_STATE,
    },
    by_sub_type_labels={
        # COW's all_armed by_sub_type expands intrastate into the two
        # internationalized variants. Stacking order matches PROD: internationalized
        # intrastate first (bottom of the stack), then non-internationalized,
        # non-state, extrastate, interstate (top).
        CT.ALL_ARMED: [
            (CT._INTRA_INT, "Internationalized intrastate"),
            (CT._INTRA_NON_INT, "Non-internationalized intrastate"),
            (CT.NON_STATE, "Non-state wars"),
            (CT.EXTRASTATE, "Extrastate wars"),
            (CT.INTERSTATE, "Interstate wars"),
        ],
        CT.INTRASTATE: [
            (CT._INTRA_INT, "Internationalized intrastate"),
            (CT._INTRA_NON_INT, "Non-internationalized intrastate"),
        ],
    },
    by_sub_type_measures={
        CT.ALL_ARMED: {M.DEATHS, M.DEATH_RATE, M.N_CONFLICTS, M.CONFLICT_RATE},
        CT.INTRASTATE: {M.N_CONFLICTS, M.CONFLICT_RATE},
    },
    ct_name={
        CT.ALL_ARMED: "wars",
        CT.ALL_STATE_BASED: "state-based wars",
        CT.INTERSTATE: "interstate wars",
        CT.INTRASTATE: "intrastate wars",
        CT.EXTRASTATE: "extrastate wars",
        CT.NON_STATE: "non-state wars",
    },
    ct_short={
        CT.INTERSTATE: "Interstate wars",
        CT.INTRASTATE: "Intrastate wars",
        CT.EXTRASTATE: "Extrastate wars",
        CT.NON_STATE: "Non-state wars",
    },
    dod={
        CT.ALL_ARMED: f"[wars]({DOD_COW.WAR})",
        CT.ALL_STATE_BASED: f"[interstate]({DOD_COW.INTERSTATE}) and [intrastate]({DOD_COW.INTRASTATE}) wars",
        CT.INTERSTATE: f"[interstate wars]({DOD_COW.INTERSTATE})",
        CT.INTRASTATE: f"[intrastate wars]({DOD_COW.INTRASTATE})",
        CT.EXTRASTATE: f"[extrastate wars]({DOD_COW.EXTRASTATE})",
        CT.NON_STATE: f"[non-state wars]({DOD_COW.NON_STATE})",
    },
    dod_by_sub_type={
        CT.ALL_ARMED: (
            f"[interstate]({DOD_COW.INTERSTATE}), "
            f"[intrastate]({DOD_COW.INTRASTATE}), "
            f"[extrastate]({DOD_COW.EXTRASTATE}), and "
            f"[non-state]({DOD_COW.NON_STATE}) wars"
        ),
        CT.INTRASTATE: f"[non-internationalized and internationalized intrastate wars]({DOD_COW.INTRASTATE})",
    },
)


# ---- PRIO (Peace Research Institute Oslo) ---------------------------------
# PRIO uses battle-deaths columns (`number_deaths_ongoing_conflicts_battle*`).
# Its "all" CT and the country table's "state-based" both represent
# all-state-based aggregates.

PRIO_SPEC = SourceSpec(
    slug="prio",
    name="Peace Research Institute Oslo",
    dataset_path="prio_v31",
    main_table="prio_v31",
    country_table="prio_v31_country",
    measures={
        M.DEATHS,
        M.DEATH_RATE,
        M.N_CONFLICTS,
        M.CONFLICT_RATE,
        M.PARTICIPANTS,
    },
    ct_map={
        "all": CT.ALL_STATE_BASED,
        "state-based": CT.ALL_STATE_BASED,
        "interstate": CT.INTERSTATE,
        "intrastate": CT.INTRASTATE,
        "intrastate (internationalized)": CT._INTRA_INT,
        "intrastate (non-internationalized)": CT._INTRA_NON_INT,
        "extrasystemic": CT.EXTRASTATE,
    },
    deaths_family="number_deaths_ongoing_conflicts_battle",
    by_sub_type_labels={
        CT.ALL_STATE_BASED: [
            (CT._INTRA_INT, "Internationalized intrastate"),
            (CT._INTRA_NON_INT, "Non-internationalized intrastate"),
            (CT.EXTRASTATE, "Extrasystemic"),
            (CT.INTERSTATE, "Interstate"),
        ],
        CT.INTRASTATE: [
            (CT._INTRA_NON_INT, "Non-internationalized intrastate"),
            (CT._INTRA_INT, "Internationalized intrastate"),
        ],
    },
    by_sub_type_measures={
        CT.ALL_STATE_BASED: {M.DEATHS, M.DEATH_RATE, M.N_CONFLICTS, M.CONFLICT_RATE},
        CT.INTRASTATE: {M.N_CONFLICTS, M.CONFLICT_RATE},
    },
    dod={
        CT.ALL_STATE_BASED: f"[interstate]({DOD_UCDP.INTERSTATE}), [intrastate]({DOD_UCDP.INTRASTATE}), and [extrasystemic]({DOD_UCDP.EXTRASYSTEMIC}) conflicts",
        CT.INTERSTATE: f"[interstate conflicts]({DOD_UCDP.INTERSTATE})",
        CT.INTRASTATE: f"[intrastate conflicts]({DOD_UCDP.INTRASTATE})",
        CT.EXTRASTATE: f"[extrasystemic conflicts]({DOD_UCDP.EXTRASYSTEMIC})",
    },
    dod_by_sub_type={
        CT.INTRASTATE: f"[non-internationalized and internationalized intrastate conflicts]({DOD_UCDP.INTRASTATE})",
    },
)


# ---- MIE -----------------------------------------------------------------
# Interstate-only. Hostility-level split on `conflict_sub_type` instead of
# the intrastate split UCDP-family sources use. Deaths is a 2-indicator
# low/high range plot (no center estimate).

MIE_SPEC = SourceSpec(
    slug="mie",
    name="Militarized Interstate Events",
    dataset_path="mie",
    main_table="mie",
    country_table="mie_country",
    measures={
        M.DEATHS,
        M.DEATH_RATE,
        M.N_CONFLICTS,
        M.CONFLICT_RATE,
        M.PARTICIPANTS,
    },
    style="mic",
    mic_deaths_kind="low_high",
    mic_dod_force_url=DOD_MIE.FORCE,
    mic_dod_war_url=DOD_MIE.WAR,
)


# ---- COW-MID -------------------------------------------------------------
# Interstate-only. Same hostility-level split as MIE, but uses "disputes"
# instead of "conflicts" in the source columns. Deaths is an 8-indicator
# stacked-bar by fatality bucket (no CI; no death_rate).

COW_MID_SPEC = SourceSpec(
    slug="cow_mid",
    name="Correlates of War — Militarized Interstate Disputes",
    dataset_path="cow_mid",
    main_table="cow_mid",
    country_table="cow_mid_country",
    measures={
        M.DEATHS,
        M.N_CONFLICTS,
        M.CONFLICT_RATE,
        M.PARTICIPANTS,
    },
    # COW-MID uses "disputes" rather than "conflicts" in column families.
    ongoing_conflicts_family="number_ongoing_disputes",
    new_conflicts_family="number_new_disputes",
    style="mic",
    mic_deaths_kind="fatality_stack",
    mic_dod_force_url=DOD_COW_MID.FORCE,
    mic_dod_war_url=DOD_COW_MID.WAR,
)


# ===========================================================================
# Entry point
# ===========================================================================

PROGRAMMATIC_SPECS: list[SourceSpec] = [
    UCDP_SPEC,
    UCDP_PRIO_SPEC,
    MARS_SPEC,
    MIE_SPEC,
    COW_SPEC,
    COW_MID_SPEC,
    PRIO_SPEC,
]


def _validate_constants_match_yaml(config: dict[str, Any]) -> None:
    """Assert that the `CT`, `M`, and `CST` classes are in sync with the YAML's dim choices.

    Catches drift between this module's slug constants and the YAML's
    `conflict_type` / `measure` / `conflict_sub_type` dimensions — e.g.,
    adding a new measure to `M` but forgetting to declare it in the YAML's
    dropdown (or vice versa) would otherwise surface as a silent
    missing-view or KeyError at save.

    Helper attrs on `CT` (those starting with `_`, like `CT._INTRA_INT`) are
    transient build-time slugs and are intentionally excluded — they never
    appear in the YAML's dim choices.
    """
    dims_by_slug = {d["slug"]: d for d in config.get("dimensions", [])}
    for cls, dim_slug in [(CT, "conflict_type"), (M, "measure"), (CST, "conflict_sub_type")]:
        if dim_slug not in dims_by_slug:
            raise ValueError(f"YAML config is missing the `{dim_slug}` dimension.")
        py_slugs = {v for k, v in vars(cls).items() if isinstance(v, str) and not k.startswith("_")}
        yaml_slugs = {c["slug"] for c in dims_by_slug[dim_slug]["choices"]}
        if py_slugs != yaml_slugs:
            only_py = sorted(py_slugs - yaml_slugs)
            only_yaml = sorted(yaml_slugs - py_slugs)
            details = []
            if only_py:
                details.append(f"in `{cls.__name__}` but missing from YAML `{dim_slug}` choices: {only_py}")
            if only_yaml:
                details.append(f"in YAML `{dim_slug}` choices but missing from `{cls.__name__}`: {only_yaml}")
            raise ValueError(f"`{cls.__name__}` is out of sync with YAML — {'; '.join(details)}.")


def run() -> None:
    yaml_config = paths.load_collection_config()
    _validate_constants_match_yaml(yaml_config)

    # The YAML carries dim definitions + explorer-level config only — every
    # source is built programmatically now.
    yaml_explorer = paths.create_collection(
        config=yaml_config,
        explorer=True,
        short_name="conflict-data-source",
    )

    # Per-source sub_config shared by all programmatic specs: same dim
    # definitions as the YAML (minus `data_source`, which combine_collections
    # re-introduces below), same `definitions` block, no views.
    sub_config = {
        "config": {},
        "definitions": yaml_config.get("definitions", {}),
        "dimensions": [d for d in yaml_config["dimensions"] if d["slug"] != "data_source"],
        "views": [],
    }

    programmatic_subs = [build_source_explorer(spec, sub_config) for spec in PROGRAMMATIC_SPECS]

    final = combine_collections(
        collections=programmatic_subs,
        catalog_path=yaml_explorer.catalog_path,
        config={"config": yaml_config.get("config", {})},
        force_collection_dimension=True,
        collection_dimension_slug="data_source",
        collection_dimension_name="Data source",
        collection_choices_names=[spec.name for spec in PROGRAMMATIC_SPECS],
    )

    # The "related question" link is the same on every view. The YAML's
    # `common_views` can't carry it post-combine (group_views recreates views),
    # so set it globally here.
    final.set_global_config(
        {
            "relatedQuestionText": "How do different approaches measure armed conflicts and their deaths?",
            "relatedQuestionUrl": "https://ourworldindata.org/conflict-data-how-do-researchers-measure-armed-conflicts-and-their-deaths",
        }
    )
    final.save(tolerate_extra_indicators=True)
