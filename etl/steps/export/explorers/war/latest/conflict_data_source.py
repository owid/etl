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
`data_source` dropdown. The YAML carries the dim definitions and the
explorer-level config plus the views for any source that hasn't been migrated
yet.

Sources currently programmatic: ucdp, ucdp_prio, mars, cow, prio.
Still loaded from YAML: mie, cow_mid.

When MIE and COW-MID are migrated, `_attach_data_source_dim` becomes obsolete:
`combine_collections` can introduce the `data_source` dim via
`force_collection_dimension=True` + `collection_dimension_slug="data_source"`,
and the YAML can shrink to dim definitions + explorer-level config only.
"""

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from owid.catalog.tables import Table

from etl.collection.core.combine import combine_collections
from etl.collection.model.dimension import Dimension, DimensionChoice
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
    NON_STATE = "#dod:non-state-war-cow"


class DOD_MARS:
    """MARS DoD anchor URLs."""

    INTERSTATE = "#dod:interstate-war-mars"
    CIVIL_WAR = "#dod:civil-war-mars"
    NON_CIVIL_WAR = "#dod:non-civil-war-mars"
    CONVENTIONAL_WARS = "#dod:conventional-war-mars"


class DOD_PRIO:
    """PRIO DoD anchor URLs."""

    STATE_BASED = "#dod:state-based-conflict-prio"
    INTERSTATE = "#dod:interstate-prio"
    INTRASTATE = "#dod:intrastate-prio"
    EXTRASYSTEMIC = "#dod:extrasystemic-prio"


# Used in every conflict_participants subtitle (override per-source if needed).
DOD_PRIMARY_PARTICIPANT = "#dod:primary-participant-ucdp"


# Per-indicator display blocks for confidence-interval (CI) stacks.
BEST_DISPLAY = {"name": "Best estimate", "color": "#B13507"}
LOW_DISPLAY = {"name": "Low estimate", "color": "#C3AEA6"}
HIGH_DISPLAY = {"name": "High estimate", "color": "#C3AEA6"}

# Map colorScaleNumericBins for deaths / rate / boolean (locations/participants).
DEATHS_MAP_BINS = "0.99, #E8F4EA, 0;10;100;1000;10000;100000"
RATE_MAP_BINS = "0.1; 0.3; 1; 3; 10; 30; 100"
BOOL_MAP_BINS = "0,#92C5DE,No;1,#F4A582,Yes"

# Shared view-config blocks.
STACKED_CONFIG = {
    "type": "StackedBar",
    "baseColorScheme": "stackedAreaDefault",
    "hideRelativeToggle": False,
    "selectedFacetStrategy": "entity",
}
MAP_CONFIG = {"hasMapTab": True, "tab": "map", "selectedFacetStrategy": "entity"}

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
    # CT slug → DoD-link markdown (e.g. "[interstate conflicts](#dod:interstate-ucdp)").
    dod: dict[str, str] = field(default_factory=dict)
    # CT slug (parent) → combined DoD link used in by_sub_type subtitles.
    dod_by_sub_type: dict[str, str] = field(default_factory=dict)
    # DoD anchor (full markdown link, "#dod:...") used in the conflict_participants
    # subtitle ("…states that were [primary participants](…)").
    dod_primary_participant: str = DOD_PRIMARY_PARTICIPANT
    # Source name used in the "'Best' estimates as identified by X." note.
    # Most sources mirror UCDP's wording so the default is fine.
    # TODO: where is this surfaced?
    ci_estimate_source: str = "UCDP"

    # Map-tab + per-best colorScale settings for deaths views, keyed by
    # (conflict_type, conflict_sub_type).
    deaths_map_views: set[tuple[str, str]] = field(default_factory=set)
    deaths_map_with_cs: set[tuple[str, str]] = field(default_factory=set)

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


def _parse_main_col(spec: SourceSpec, short: str, ct_raw: str) -> dict[str, Any] | None:
    """Map a `main_table` column to explorer dim values; return None to drop."""
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


def _parse_country_col(spec: SourceSpec, short: str, ct_raw: str) -> dict[str, Any] | None:
    """Map a `country_table` column (conflict_participants) to explorer dims."""
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


def _parse_locations_col(spec: SourceSpec, short: str, ct_raw: str) -> dict[str, Any] | None:
    """Map a `locations_table` column (conflict_locations) to explorer dims."""
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
        ct_raw = (meta.dimensions or {}).get("conflict_type")
        if not ct_raw or not meta.original_short_name:
            drops.append(col)
            continue
        dims = parse(spec, meta.original_short_name, ct_raw)
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
        for slug in ("conflict_sub_type", "sub_measure", "_estimate"):
            if slug not in existing:
                tb.metadata.dimensions.append({"name": slug, "slug": slug})
    return tb


def _load_and_adjust(spec: SourceSpec) -> list[Table]:
    """Load every table the spec references and apply `_adjust_table` to each."""
    ds = paths.load_dataset(spec.dataset_path)
    tables: list[Table] = [_adjust_table(ds.read(spec.main_table, load_data=False), spec, _parse_main_col)]
    if spec.country_table:
        tables.append(_adjust_table(ds.read(spec.country_table, load_data=False), spec, _parse_country_col))
    if spec.locations_table:
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


def _set_view_config(view, spec: SourceSpec) -> None:
    """Fill in `title` / `subtitle` / `note` / chart-config blocks per view."""
    d = view.dimensions
    measure = d["measure"]
    ctype = d["conflict_type"]
    cst = d["conflict_sub_type"]
    sub_measure = d["sub_measure"]
    cfg = view.config or {}

    if measure in (M.DEATHS, M.DEATH_RATE):
        _deaths_text(cfg, spec, measure, ctype, cst)
    elif measure in (M.N_CONFLICTS, M.CONFLICT_RATE):
        _count_text(cfg, spec, measure, ctype, cst, sub_measure)
    elif measure == M.PARTICIPANTS:
        _participants_text(cfg, spec, ctype, sub_measure)
    elif measure == M.LOCATIONS:
        _locations_text(cfg, spec, ctype, sub_measure)

    view.config = cfg


def _deaths_text(cfg: dict[str, Any], spec: SourceSpec, measure: str, ctype: str, cst: str) -> None:
    per_capita = measure == M.DEATH_RATE
    noun = "Death rate" if per_capita else spec.deaths_noun
    name = spec.ct_name[ctype]

    if cst == CST.BY_SUB_TYPE:
        cfg["title"] = f"{noun} in {name} based on where they occurred"
        cfg["subtitle"] = _deaths_subtitle(spec.dod_by_sub_type.get(ctype, spec.dod[ctype]), per_capita)
        cfg.update(STACKED_CONFIG)
        return

    if cst in (CST.ONLY_NON_INTERNATIONALIZED, CST.ONLY_INTERNATIONALIZED):
        word = "non-internationalized" if cst == CST.ONLY_NON_INTERNATIONALIZED else "internationalized"
        intrastate_anchor = _dod_url(spec.dod[CT.INTRASTATE])
        cfg["title"] = f"{noun} in {word} intrastate conflicts"
        cfg["subtitle"] = _deaths_subtitle(
            f"[{word} intrastate conflicts]({intrastate_anchor})",
            per_capita,
        )
        cfg["note"] = f"'Best' estimates as identified by {spec.ci_estimate_source}."
        cfg["selectedFacetStrategy"] = "entity"
        return

    # na / all_sub_types — single-indicator or CI-stacked deaths view.
    has_map = (ctype, cst) in spec.deaths_map_views
    cfg["title"] = f"{noun} in {name} based on where they occurred" if has_map else f"{noun} in {name}"
    cfg["subtitle"] = _deaths_subtitle(spec.dod[ctype], per_capita)
    cfg["note"] = f"'Best' estimates as identified by {spec.ci_estimate_source}."
    if has_map:
        cfg.update(MAP_CONFIG)
    else:
        cfg["selectedFacetStrategy"] = "entity"


def _deaths_subtitle(dod_link: str, per_capita: bool) -> str:
    """Build the deaths/death-rate subtitle from a DoD-link fragment."""
    if per_capita:
        return (
            "Deaths of combatants and civilians due to fighting, per 100,000 people. "
            f"Included are {dod_link} that were ongoing that year."
        )
    return f"Included are deaths of combatants and civilians due to fighting in {dod_link} that were ongoing that year."


def _count_text(cfg: dict[str, Any], spec: SourceSpec, measure: str, ctype: str, cst: str, sub_measure: str) -> None:
    rate = measure == M.CONFLICT_RATE
    is_new = sub_measure == "only_new_conflicts"
    name = spec.ct_name[ctype]
    new_prefix = "new " if is_new else ""
    lead = "Rate of" if rate else "Number of"
    verb = "started that year" if is_new else "were ongoing that year"

    if cst == CST.BY_SUB_TYPE:
        cfg["title"] = f"{lead} {new_prefix}{name}"
        cfg["subtitle"] = _count_subtitle(spec.dod_by_sub_type.get(ctype, spec.dod[ctype]), rate, verb)
        cfg["note"] = (
            "Some conflicts affect several regions, and do not necessarily start at the same time "
            "across them. The sum across all regions can therefore be higher than the global number."
            if is_new
            else "Some conflicts affect several regions. The sum across all regions can therefore "
            "be higher than the global number."
        )
        cfg.update(STACKED_CONFIG)
        return

    cfg["title"] = f"{lead} {new_prefix}{name}"
    cfg["subtitle"] = _count_subtitle(spec.dod[ctype], rate, verb, interstate=ctype == CT.INTERSTATE)


def _count_subtitle(dod_link: str, rate: bool, verb: str, interstate: bool = False) -> str:
    """Build the number_of_conflicts / conflict_rate subtitle."""
    if not rate:
        return f"Included are {dod_link} that {verb}."
    # Interstate's ongoing rate uses a state-pair denominator (per_country_pair).
    denom = "all state-pairs" if interstate else "all states"
    return (
        f"The number of conflicts divided by the number of {denom}. This accounts for the changing "
        f"number of states over time. Included are {dod_link} that {verb}."
    )


def _participants_text(cfg: dict[str, Any], spec: SourceSpec, ctype: str, sub_measure: str) -> None:
    name = spec.ct_name[ctype]
    country_level = sub_measure == "country_level_data"
    cfg["title"] = f"States involved in {name}" if country_level else f"Number of states involved in {name}"

    # Singular form of the DoD link (UCDP "interstate conflicts" → "interstate
    # conflict", COW "interstate wars" → "interstate war").
    if ctype == CT.ALL_STATE_BASED:
        dod_sing = _all_state_based_dod_singular(spec)
    else:
        dod_sing = spec.dod[ctype].replace("conflicts](", "conflict](").replace("wars](", "war](")

    cfg["subtitle"] = (
        f"Included are states that were [primary participants]({spec.dod_primary_participant}) "
        f"in at least one {dod_sing} that year."
    )
    if country_level:
        cfg.update(MAP_CONFIG)


def _all_state_based_dod_singular(spec: SourceSpec) -> str:
    """Build the "interstate, intrastate, or extrasystemic conflict" fragment using
    the spec's interstate / intrastate / extrastate DoD anchors, in singular form."""
    parts = []
    for ctype, label in (
        (CT.INTERSTATE, "interstate"),
        (CT.INTRASTATE, "intrastate"),
        (CT.EXTRASTATE, "extrasystemic"),
    ):
        if ctype in spec.dod:
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
    cfg["title"] = (
        f"Countries where {name} took place" if country_level else f"Number of countries where {name} took place"
    )
    cfg["subtitle"] = f"Included are {spec.dod[ctype]} that caused at least one death in the country that year."
    if country_level:
        cfg.update(MAP_CONFIG)


def _set_view_displays(view, spec: SourceSpec) -> None:
    """Fill in per-indicator display (name / color / colorScale) blocks per view."""
    d = view.dimensions
    measure = d["measure"]
    ctype = d["conflict_type"]
    cst = d["conflict_sub_type"]
    if view.indicators is None or view.indicators.y is None:
        return
    ys = view.indicators.y

    # by_sub_type stacks: labels are set by `_build_by_sub_type_views`.
    if cst == CST.BY_SUB_TYPE:
        return

    if measure in (M.DEATHS, M.DEATH_RATE):
        _set_deaths_displays(ys, spec, measure, ctype, cst)
    elif measure in (M.N_CONFLICTS, M.CONFLICT_RATE):
        _set_count_display(ys, spec, ctype)
    elif measure in (M.LOCATIONS, M.PARTICIPANTS):
        _set_locations_or_participants_displays(ys, measure, sub_measure=d["sub_measure"])


def _set_deaths_displays(ys: list[Indicator], spec: SourceSpec, measure: str, ctype: str, cst: str) -> None:
    """Set displays on a deaths / death_rate view.

    CI-stacked views (has _low_/_high_ variants) → best/low/high labels.
    Single-indicator views (e.g. COW) → CT short label, no "Best estimate" tag.
    """
    has_ci_variants = any("_low_" in i.catalogPath or "_high_" in i.catalogPath for i in ys)
    if not has_ci_variants:
        short = spec.ct_short.get(ctype)
        if len(ys) == 1 and short:
            ys[0].display = {"name": short}
        return

    per_capita = measure == M.DEATH_RATE
    with_cs = (ctype, cst) in spec.deaths_map_with_cs
    for ind in ys:
        if "_low_" in ind.catalogPath:
            ind.display = dict(LOW_DISPLAY)
        elif "_high_" in ind.catalogPath:
            ind.display = dict(HIGH_DISPLAY)
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
    #    call handles every source — deaths views become 3-indicator CI
    #    stacks, non-CI views become 1-indicator (degenerate) "stacks". Every
    #    view ends up at `_estimate="_ci"`, so `drop_dimensions_if_single_choice`
    #    (default True) auto-drops the now-trivial dim.
    #    NOTE: we pass the explicit list of existing choices rather than
    #    omitting `choices` because group_views adds "_ci" to the dim *before*
    #    its replace-pass runs, and an omitted `choices` would then include
    #    "_ci" itself — wiping the new views we just created.
    estimate_in_use = list(c.dimension_choices_in_use().get("_estimate", set()))
    c.group_views(
        groups=[{"dimension": "_estimate", "choices": estimate_in_use, "choice_new_slug": "_ci", "replace": True}]
    )

    # 2) Build by_sub_type stacks (manual — see comment on `_build_by_sub_type_views`).
    _build_by_sub_type_views(c, spec)

    # 3) Drop the helper intrastate-variant views for non-deaths measures.
    #    They only exist as children of the by_sub_type stacks above; the
    #    legacy explorer doesn't surface them standalone.
    ct_in_use = c.dimension_choices_in_use().get("conflict_type", set())
    helpers_in_use = [s for s in INTRASTATE_VARIANT_REMAP if s in ct_in_use]
    non_deaths_measures = [
        m for m in (M.N_CONFLICTS, M.CONFLICT_RATE, M.LOCATIONS, M.PARTICIPANTS) if m in spec.measures
    ]
    if helpers_in_use and non_deaths_measures:
        c.drop_views([{"conflict_type": helpers_in_use, "measure": non_deaths_measures}])

    # 4) Consolidate the remaining helper conflict_type slugs (deaths
    #    only_*_internationalized views) into `intrastate_conflicts`. The
    #    helper-bearing views differ from canonical intrastate views by
    #    `conflict_sub_type`, so no coordinate collision occurs.
    #    `dedup_slug="inherit"` keeps the canonical `intrastate_conflicts`
    #    choice config that's already on the dim.
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
    deaths_map_with_cs={
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
        CT.ALL_STATE_BASED: f"[state-based wars]({DOD_COW.STATE_BASED})",
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
        CT.ALL_STATE_BASED: f"[state-based conflicts]({DOD_PRIO.STATE_BASED})",
        CT.INTERSTATE: f"[interstate conflicts]({DOD_PRIO.INTERSTATE})",
        CT.INTRASTATE: f"[intrastate conflicts]({DOD_PRIO.INTRASTATE})",
        CT.EXTRASTATE: f"[extrasystemic conflicts]({DOD_PRIO.EXTRASYSTEMIC})",
    },
    dod_by_sub_type={
        CT.ALL_STATE_BASED: (
            f"[interstate]({DOD_PRIO.INTERSTATE}), "
            f"[intrastate]({DOD_PRIO.INTRASTATE}), and "
            f"[extrasystemic]({DOD_PRIO.EXTRASYSTEMIC}) conflicts"
        ),
        CT.INTRASTATE: f"[non-internationalized and internationalized intrastate conflicts]({DOD_PRIO.INTRASTATE})",
    },
)


# ===========================================================================
# Entry point
# ===========================================================================

# Sources built programmatically. The rest still load from the YAML.
# TODO: add MIE + COW-MID once they're migrated; the YAML's `views:` section
# can then be removed entirely.
PROGRAMMATIC_SPECS: list[SourceSpec] = [UCDP_SPEC, UCDP_PRIO_SPEC, MARS_SPEC, COW_SPEC, PRIO_SPEC]


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

    # The YAML carries views only for sources not yet migrated (MIE + COW-MID).
    # It still defines all five dims and the explorer-level config.
    yaml_explorer = paths.create_collection(config=yaml_config, explorer=True)

    # Per-source sub_config shared by all programmatic specs: same dim
    # definitions as the YAML (minus `data_source`, which combine_collections
    # re-introduces), same `definitions` block, no views.
    sub_config = {
        "config": {},
        "definitions": yaml_config.get("definitions", {}),
        "dimensions": [d for d in yaml_config["dimensions"] if d["slug"] != "data_source"],
        "views": [],
    }

    programmatic_subs = [build_source_explorer(spec, sub_config) for spec in PROGRAMMATIC_SPECS]
    for spec, sub in zip(PROGRAMMATIC_SPECS, programmatic_subs):
        _attach_data_source_dim(sub, spec, yaml_explorer)

    final = combine_collections(
        collections=[*programmatic_subs, yaml_explorer],
        catalog_path=yaml_explorer.catalog_path,
        config={"config": yaml_config.get("config", {})},
    )
    final.save(tolerate_extra_indicators=True)


def _attach_data_source_dim(sub_explorer, spec: SourceSpec, yaml_explorer) -> None:
    """Insert a `data_source` dim matching the YAML's definition and tag views.

    Transitional shim: when every source is programmatic this can be deleted
    and `combine_collections` will introduce the dim itself via
    `force_collection_dimension=True` + `collection_dimension_slug="data_source"`.
    """
    ds_yaml = next(d for d in yaml_explorer.dimensions if d.slug == "data_source")
    ds_dim = Dimension(
        slug=ds_yaml.slug,
        name=ds_yaml.name,
        description=ds_yaml.description,
        presentation=deepcopy(ds_yaml.presentation),
        choices=[DimensionChoice(slug=spec.slug, name=spec.name)],
    )
    sub_explorer.dimensions = [ds_dim, *sub_explorer.dimensions]
    for view in sub_explorer.views:
        view.dimensions = {"data_source": spec.slug, **view.dimensions}
