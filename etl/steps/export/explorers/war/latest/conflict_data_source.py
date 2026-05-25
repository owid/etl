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
# can override individual entries via `ct_name_override` / `ct_short_override`.
CT_NAME = {
    "all_armed_conflicts": "armed conflicts",
    "all_state_based_conflicts": "state-based conflicts",
    "interstate_conflicts": "interstate conflicts",
    "intrastate_conflicts": "intrastate conflicts",
    "extrastate_conflicts": "extrasystemic conflicts",
    "non_state_conflicts": "non-state conflicts",
    "one_sided_violence": "one-sided violence",
}
CT_SHORT = {
    "interstate_conflicts": "Interstate",
    "intrastate_conflicts": "Intrastate",
    "extrastate_conflicts": "Extrasystemic",
    "non_state_conflicts": "Non-state",
    "one_sided_violence": "One-sided violence",
}

# "Parent" conflict types: their single-indicator (or CI-stacked) view uses
# `conflict_sub_type=all_sub_types` rather than `na`.
PARENT_CTS = {"all_armed_conflicts", "all_state_based_conflicts", "intrastate_conflicts"}
# Legacy quirk: interstate's counts/rate view also uses `all_sub_types`
# even though interstate is otherwise treated as atomic.
INTERSTATE_USES_ALL_SUB_TYPES_FOR_COUNTS = {"interstate_conflicts"}

# Helper conflict_type slugs used during the build to keep the two intrastate
# sub-types separable (so by_sub_type stacks can pick them up as children).
# They get remapped to `intrastate_conflicts` + a matching `conflict_sub_type`
# in `build_source_explorer`.
INTRASTATE_VARIANT_REMAP = {
    "_intrastate_int": ("intrastate_conflicts", "only_internationalized_conflicts"),
    "_intrastate_non_int": ("intrastate_conflicts", "only_non_internationalized_conflicts"),
}

# Canonical dimension orderings used by `_refresh_dim_choices` after the build.
CONFLICT_TYPE_ORDER = [
    "all_armed_conflicts",
    "all_state_based_conflicts",
    "interstate_conflicts",
    "intrastate_conflicts",
    "extrastate_conflicts",
    "non_state_conflicts",
    "one_sided_violence",
]
CONFLICT_SUB_TYPE_ORDER = [
    "na",
    "by_sub_type",
    "all_sub_types",
    "only_non_internationalized_conflicts",
    "only_internationalized_conflicts",
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

    # Per-CT name overrides used in titles + display labels. Default falls back
    # to CT_NAME / CT_SHORT. COW populates these to swap "conflicts" → "wars".
    ct_name_override: dict[str, str] = field(default_factory=dict)
    ct_short_override: dict[str, str] = field(default_factory=dict)

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
    dod_primary_participant: str = "#dod:primary-participant-ucdp"
    # Source name used in the "'Best' estimates as identified by X." note.
    # Most sources mirror UCDP's wording so the default is fine.
    ci_estimate_source: str = "UCDP"

    # Map-tab + per-best colorScale settings for deaths views, keyed by
    # (conflict_type, conflict_sub_type).
    deaths_map_views: set[tuple[str, str]] = field(default_factory=set)
    deaths_map_with_cs: set[tuple[str, str]] = field(default_factory=set)


# ===========================================================================
# Dimension assignment (column → explorer dims)
# ===========================================================================


def _conflict_sub_type(measure: str, ctype: str) -> str:
    """Return the `conflict_sub_type` value for a non-stacked / non-CI view."""
    if ctype == "_intrastate_int":
        return "only_internationalized_conflicts"
    if ctype == "_intrastate_non_int":
        return "only_non_internationalized_conflicts"
    if measure in ("conflict_deaths", "death_rate"):
        return "all_sub_types" if ctype in PARENT_CTS else "na"
    if measure in ("number_of_conflicts", "conflict_rate"):
        if ctype in PARENT_CTS or ctype in INTERSTATE_USES_ALL_SUB_TYPES_FOR_COUNTS:
            return "all_sub_types"
        return "na"
    # locations / participants: only intrastate uses "all_sub_types".
    return "all_sub_types" if ctype == "intrastate_conflicts" else "na"


def _parse_main_col(spec: SourceSpec, short: str, ct_raw: str) -> dict[str, Any] | None:
    """Map a `main_table` column to explorer dim values; return None to drop."""
    ctype = spec.ct_map.get(ct_raw)
    if ctype is None:
        return None

    # Deaths / death-rate families (CI: best/low/high × _per_capita).
    deaths_variants = {
        spec.deaths_family: ("conflict_deaths", "best"),
        f"{spec.deaths_family}_low": ("conflict_deaths", "low"),
        f"{spec.deaths_family}_high": ("conflict_deaths", "high"),
        f"{spec.deaths_family}_per_capita": ("death_rate", "best"),
        f"{spec.deaths_family}_low_per_capita": ("death_rate", "low"),
        f"{spec.deaths_family}_high_per_capita": ("death_rate", "high"),
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
            return _maybe_dim_dict(spec, "number_of_conflicts", ctype, sub_measure)

        if short == f"{family}_per_country_pair":
            # Only kept for interstate-ongoing rates; drop the rest.
            if not (is_ongoing and ctype == "interstate_conflicts"):
                return None
            return _maybe_dim_dict(spec, "conflict_rate", ctype, sub_measure)

        if short == f"{family}_per_country":
            # Interstate-ongoing rate uses the per_country_pair variant instead.
            if ctype == "interstate_conflicts" and is_ongoing:
                return None
            return _maybe_dim_dict(spec, "conflict_rate", ctype, sub_measure)

    return None  # any other column family (civilians/combatants/unknown, etc.)


def _parse_country_col(spec: SourceSpec, short: str, ct_raw: str) -> dict[str, Any] | None:
    """Map a `country_table` column (conflict_participants) to explorer dims."""
    ctype = spec.ct_map.get(ct_raw)
    if ctype is None or ctype == "non_state_conflicts":
        return None  # non-state has no "primary state participant" semantics
    if "conflict_participants" not in spec.measures:
        return None

    if short == "number_participants":
        return _maybe_dim_dict(spec, "conflict_participants", ctype, "regional_data")
    if short == "participated_in_conflict":
        return _maybe_dim_dict(spec, "conflict_participants", ctype, "country_level_data")
    return None


def _parse_locations_col(spec: SourceSpec, short: str, ct_raw: str) -> dict[str, Any] | None:
    """Map a `locations_table` column (conflict_locations) to explorer dims."""
    ctype = spec.ct_map.get(ct_raw)
    if ctype is None:
        return None
    if "conflict_locations" not in spec.measures:
        return None

    if short == "number_locations":
        return _maybe_dim_dict(spec, "conflict_locations", ctype, "regional_data")
    if short == "is_location_of_conflict":
        return _maybe_dim_dict(spec, "conflict_locations", ctype, "country_level_data")
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
    """Same as `_dim_dict` for non-CI measures, gated on the spec carrying this measure."""
    if measure not in spec.measures:
        return None
    return _dim_dict(measure, ctype, sub_measure, "_na")


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
                            "conflict_sub_type": "by_sub_type",
                            "sub_measure": sub_measure,
                        },
                        indicators=ViewIndicators(y=stacked),
                        config={},
                    )
                )
    c.views.extend(new_views)


def _by_sub_type_output_sub_measures(spec: SourceSpec, measure: str) -> list[str]:
    """The `sub_measure` values to emit a by_sub_type view for, given a measure."""
    if measure in ("conflict_deaths", "death_rate"):
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
        if inds is None and measure in ("conflict_deaths", "death_rate"):
            inds = by_path_loose.get((measure, child_ct), [])
        if not inds:
            continue

        if measure in ("conflict_deaths", "death_rate") and len(inds) > 1:
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


def _drop_dim(c, dim_slug: str) -> None:
    """Remove a dimension from the collection and from every view's dimensions dict."""
    c.dimensions = [d for d in c.dimensions if d.slug != dim_slug]
    for v in c.views:
        v.dimensions.pop(dim_slug, None)


def _refresh_dim_choices(c, dim_slug: str, order: list[str]) -> None:
    """Re-list a dim's choices in `order`, dropping unused slugs."""
    dim = next(d for d in c.dimensions if d.slug == dim_slug)
    by_slug = {ch.slug: ch for ch in dim.choices}
    used = {v.dimensions.get(dim_slug) for v in c.views}
    used.discard(None)
    new_choices = [
        by_slug.get(slug) or DimensionChoice(slug=slug, name=slug)
        for slug in order
        if slug in used
    ]
    # Append any used-but-unlisted slugs at the end (defensive — shouldn't happen).
    for slug in sorted(used - set(order)):
        new_choices.append(by_slug.get(slug) or DimensionChoice(slug=slug, name=slug))
    dim.choices = new_choices


# ===========================================================================
# FAUST templates (titles, subtitles, notes, displays)
# ===========================================================================


def _ct_name(spec: SourceSpec, ctype: str) -> str:
    return spec.ct_name_override.get(ctype) or CT_NAME[ctype]


def _ct_short(spec: SourceSpec, ctype: str) -> str | None:
    return spec.ct_short_override.get(ctype) or CT_SHORT.get(ctype)


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

    if measure in ("conflict_deaths", "death_rate"):
        _deaths_text(cfg, spec, measure, ctype, cst)
    elif measure in ("number_of_conflicts", "conflict_rate"):
        _count_text(cfg, spec, measure, ctype, cst, sub_measure)
    elif measure == "conflict_participants":
        _participants_text(cfg, spec, ctype, sub_measure)
    elif measure == "conflict_locations":
        _locations_text(cfg, spec, ctype, sub_measure)

    view.config = cfg


def _deaths_text(cfg: dict[str, Any], spec: SourceSpec, measure: str, ctype: str, cst: str) -> None:
    per_capita = measure == "death_rate"
    noun = "Death rate" if per_capita else spec.deaths_noun
    name = _ct_name(spec, ctype)

    if cst == "by_sub_type":
        cfg["title"] = f"{noun} in {name} based on where they occurred"
        cfg["subtitle"] = _deaths_subtitle(spec.dod_by_sub_type.get(ctype, spec.dod[ctype]), per_capita)
        cfg.update(STACKED_CONFIG)
        return

    if cst in ("only_non_internationalized_conflicts", "only_internationalized_conflicts"):
        word = "non-internationalized" if cst.startswith("only_non") else "internationalized"
        intrastate_anchor = _dod_url(spec.dod["intrastate_conflicts"])
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
    return (
        "Included are deaths of combatants and civilians due to fighting in "
        f"{dod_link} that were ongoing that year."
    )


def _count_text(cfg: dict[str, Any], spec: SourceSpec, measure: str, ctype: str, cst: str, sub_measure: str) -> None:
    rate = measure == "conflict_rate"
    is_new = sub_measure == "only_new_conflicts"
    name = _ct_name(spec, ctype)
    new_prefix = "new " if is_new else ""
    lead = "Rate of" if rate else "Number of"
    verb = "started that year" if is_new else "were ongoing that year"

    if cst == "by_sub_type":
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
    cfg["subtitle"] = _count_subtitle(spec.dod[ctype], rate, verb, interstate=ctype == "interstate_conflicts")


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
    name = _ct_name(spec, ctype)
    country_level = sub_measure == "country_level_data"
    cfg["title"] = f"States involved in {name}" if country_level else f"Number of states involved in {name}"

    # Singular form of the DoD link (UCDP "interstate conflicts" → "interstate
    # conflict", COW "interstate wars" → "interstate war").
    if ctype == "all_state_based_conflicts":
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
        ("interstate_conflicts", "interstate"),
        ("intrastate_conflicts", "intrastate"),
        ("extrastate_conflicts", "extrasystemic"),
    ):
        if ctype in spec.dod:
            parts.append(f"[{label}]({_dod_url(spec.dod[ctype])})")
    # Detect whether the source talks about "wars" instead of "conflicts".
    noun = "war" if spec.ct_name_override.get("interstate_conflicts", "").endswith("wars") else "conflict"
    if len(parts) >= 2:
        return ", ".join(parts[:-1]) + f", or {parts[-1]} {noun}"
    if parts:
        return f"{parts[0]} {noun}"
    return f"{noun}"


def _locations_text(cfg: dict[str, Any], spec: SourceSpec, ctype: str, sub_measure: str) -> None:
    name = _ct_name(spec, ctype)
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
    if cst == "by_sub_type":
        return

    if measure in ("conflict_deaths", "death_rate"):
        _set_deaths_displays(ys, spec, measure, ctype, cst)
    elif measure in ("number_of_conflicts", "conflict_rate"):
        _set_count_display(ys, spec, ctype)
    elif measure in ("conflict_locations", "conflict_participants"):
        _set_locations_or_participants_displays(ys, measure, sub_measure=d["sub_measure"])


def _set_deaths_displays(ys: list[Indicator], spec: SourceSpec, measure: str, ctype: str, cst: str) -> None:
    """Set displays on a deaths / death_rate view.

    CI-stacked views (has _low_/_high_ variants) → best/low/high labels.
    Single-indicator views (e.g. COW) → CT short label, no "Best estimate" tag.
    """
    has_ci_variants = any("_low_" in i.catalogPath or "_high_" in i.catalogPath for i in ys)
    if not has_ci_variants:
        short = _ct_short(spec, ctype)
        if len(ys) == 1 and short:
            ys[0].display = {"name": short}
        return

    per_capita = measure == "death_rate"
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
    short = _ct_short(spec, ctype)
    if len(ys) == 1 and short:
        ys[0].display = {"name": short}


def _set_locations_or_participants_displays(ys: list[Indicator], measure: str, sub_measure: str) -> None:
    """Locations always use the boolean colorScale; participants only at country level."""
    needs_cs = measure == "conflict_locations" or sub_measure == "country_level_data"
    if not needs_cs:
        return
    for ind in ys:
        ind.display = {"colorScaleNumericBins": BOOL_MAP_BINS}


# ===========================================================================
# Universal build pipeline
# ===========================================================================


def build_source_explorer(spec: SourceSpec, yaml_config: dict[str, Any]):
    """Build one sub-explorer for `spec` and return it.

    The returned explorer does not carry the `data_source` dim; that's added
    by `_attach_data_source_dim` (or by `combine_collections` in the future,
    once every source is built programmatically).
    """
    tables = _load_and_adjust(spec)

    # Indicator names per table, in the same order as `tables`.
    main_inds = [
        m for m in ("conflict_deaths", "death_rate", "number_of_conflicts", "conflict_rate") if m in spec.measures
    ]
    indicator_names: list[list[str]] = [main_inds]
    if spec.country_table:
        indicator_names.append(["conflict_participants"])
    if spec.locations_table:
        indicator_names.append(["conflict_locations"])

    # Reuse the YAML's dim definitions (minus data_source) so this sub-explorer
    # shares the same dim metadata as the YAML-driven one — required by
    # `combine_collections`'s structural-equality check.
    sub_config = {
        "config": {},
        "definitions": yaml_config.get("definitions", {}),
        "dimensions": [d for d in yaml_config["dimensions"] if d["slug"] != "data_source"],
        "views": [],
    }
    c = paths.create_collection(
        config=sub_config,
        tb=tables,
        indicator_names=indicator_names,
        dimensions=["conflict_type", "conflict_sub_type", "sub_measure", "_estimate"],
        indicators_slug="measure",
        short_name=spec.slug,
        explorer=True,
    )

    # 1) CI collapse: merge (best, low, high) → "_ci". Some sources (e.g. MARS)
    #    don't carry every CI variant — intersect against what's actually there.
    est_dim = next(d for d in c.dimensions if d.slug == "_estimate")
    ci_choices = [v for v in ("best", "low", "high") if v in est_dim.choice_slugs]
    if len(ci_choices) >= 2:
        c.group_views(
            groups=[{"dimension": "_estimate", "choices": ci_choices, "choice_new_slug": "_ci", "replace": True}]
        )
    _drop_dim(c, "_estimate")

    # 2) Build by_sub_type stacks (manual — see comment on `_build_by_sub_type_views`).
    _build_by_sub_type_views(c, spec)

    # 3) Drop the helper intrastate-variant views for non-deaths measures.
    #    They only exist as children of the by_sub_type stacks above; the
    #    legacy explorer doesn't surface them standalone.
    ct_dim = next(d for d in c.dimensions if d.slug == "conflict_type")
    helpers_in_use = [s for s in INTRASTATE_VARIANT_REMAP if s in ct_dim.choice_slugs]
    non_deaths_measures = [
        m
        for m in ("number_of_conflicts", "conflict_rate", "conflict_locations", "conflict_participants")
        if m in spec.measures
    ]
    if helpers_in_use and non_deaths_measures:
        c.drop_views([{"conflict_type": helpers_in_use, "measure": non_deaths_measures}])

    # 4) Rename the remaining helper conflict_type slugs (deaths only_*_internationalized
    #    views) to use `intrastate_conflicts`. `conflict_sub_type` and `sub_measure`
    #    were already set correctly in `_parse_main_col`.
    for view in c.views:
        ct = view.dimensions.get("conflict_type")
        if ct in INTRASTATE_VARIANT_REMAP:
            new_ct, _ = INTRASTATE_VARIANT_REMAP[ct]
            view.dimensions["conflict_type"] = new_ct

    # 5) Canonical dim choice ordering.
    _refresh_dim_choices(c, "conflict_type", CONFLICT_TYPE_ORDER)
    _refresh_dim_choices(c, "conflict_sub_type", CONFLICT_SUB_TYPE_ORDER)

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
        "conflict_deaths",
        "death_rate",
        "number_of_conflicts",
        "conflict_rate",
        "conflict_locations",
        "conflict_participants",
    },
    ct_map={
        "all": "all_armed_conflicts",
        "state-based": "all_state_based_conflicts",
        "interstate": "interstate_conflicts",
        "intrastate": "intrastate_conflicts",
        "intrastate (internationalized)": "_intrastate_int",
        "intrastate (non-internationalized)": "_intrastate_non_int",
        "extrasystemic": "extrastate_conflicts",
        "non-state conflict": "non_state_conflicts",
        "one-sided violence": "one_sided_violence",
    },
    deaths_sub_measure="country_and_region_data",
    by_sub_type_labels={
        "all_armed_conflicts": [
            ("one_sided_violence", "One-sided violence"),
            ("non_state_conflicts", "Non-state"),
            ("intrastate_conflicts", "Intrastate"),
            ("extrastate_conflicts", "Extrasystemic"),
            ("interstate_conflicts", "Interstate"),
        ],
        "all_state_based_conflicts": [
            ("_intrastate_int", "Internationalized intrastate"),
            ("_intrastate_non_int", "Non-internationalized intrastate"),
            ("extrastate_conflicts", "Extrasystemic"),
            ("interstate_conflicts", "Interstate"),
        ],
        "intrastate_conflicts": [
            ("_intrastate_non_int", "Non-internationalized intrastate"),
            ("_intrastate_int", "Internationalized intrastate"),
        ],
    },
    by_sub_type_measures={
        "all_armed_conflicts": {"conflict_deaths", "death_rate", "number_of_conflicts", "conflict_rate"},
        "all_state_based_conflicts": {"conflict_deaths", "death_rate", "number_of_conflicts", "conflict_rate"},
        "intrastate_conflicts": {"number_of_conflicts", "conflict_rate"},
    },
    dod={
        "all_armed_conflicts": "[armed conflicts](#dod:armed-conflict-ucdp)",
        "all_state_based_conflicts": "[interstate](#dod:interstate-ucdp), [intrastate](#dod:intrastate-ucdp), and [extrasystemic](#dod:extrasystemic-ucdp) conflicts",
        "interstate_conflicts": "[interstate conflicts](#dod:interstate-ucdp)",
        "intrastate_conflicts": "[intrastate conflicts](#dod:intrastate-ucdp)",
        "extrastate_conflicts": "[extrasystemic conflicts](#dod:extrasystemic-ucdp)",
        "non_state_conflicts": "[non-state conflicts](#dod:nonstate-ucdp)",
        "one_sided_violence": "[one-sided violence](#dod:onesided-ucdp)",
    },
    dod_by_sub_type={
        "all_armed_conflicts": "[interstate](#dod:interstate-ucdp), [intrastate](#dod:intrastate-ucdp), [extrasystemic](#dod:extrasystemic-ucdp), [non-state](#dod:nonstate-ucdp) conflicts, and [one-sided violence](#dod:onesided-ucdp)",
        "all_state_based_conflicts": "[interstate](#dod:interstate-ucdp), [intrastate](#dod:intrastate-ucdp), and [extrasystemic](#dod:extrasystemic-ucdp) conflicts",
        "intrastate_conflicts": "[non-internationalized and internationalized intrastate conflicts](#dod:intrastate-ucdp)",
    },
    deaths_map_views={
        ("all_armed_conflicts", "all_sub_types"),
        ("all_state_based_conflicts", "all_sub_types"),
        ("interstate_conflicts", "na"),
        ("intrastate_conflicts", "all_sub_types"),
        ("non_state_conflicts", "na"),
        ("one_sided_violence", "na"),
    },
    deaths_map_with_cs={
        ("interstate_conflicts", "na"),
        ("intrastate_conflicts", "all_sub_types"),
        ("non_state_conflicts", "na"),
        ("one_sided_violence", "na"),
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
    measures={"conflict_deaths", "death_rate"},
    ct_map={
        "state-based": "all_state_based_conflicts",
        "interstate": "interstate_conflicts",
        "intrastate": "intrastate_conflicts",
        "intrastate (internationalized)": "_intrastate_int",
        "intrastate (non-internationalized)": "_intrastate_non_int",
        "extrasystemic": "extrastate_conflicts",
    },
    by_sub_type_labels={
        "all_state_based_conflicts": [
            ("_intrastate_int", "Internationalized intrastate"),
            ("_intrastate_non_int", "Non-internationalized intrastate"),
            ("extrastate_conflicts", "Extrasystemic"),
            ("interstate_conflicts", "Interstate"),
        ],
    },
    by_sub_type_measures={"all_state_based_conflicts": {"conflict_deaths", "death_rate"}},
    dod={
        "all_state_based_conflicts": "[interstate](#dod:interstate-ucdp), [intrastate](#dod:intrastate-ucdp), and [extrasystemic](#dod:extrasystemic-ucdp) conflicts",
        "interstate_conflicts": "[interstate conflicts](#dod:interstate-ucdp)",
        "intrastate_conflicts": "[intrastate conflicts](#dod:intrastate-ucdp)",
        "extrastate_conflicts": "[extrasystemic conflicts](#dod:extrasystemic-ucdp)",
    },
    dod_by_sub_type={
        "all_state_based_conflicts": "[interstate](#dod:interstate-ucdp), [intrastate](#dod:intrastate-ucdp), and [extrasystemic](#dod:extrasystemic-ucdp) conflicts",
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
        "conflict_deaths",
        "death_rate",
        "number_of_conflicts",
        "conflict_rate",
        "conflict_participants",
    },
    ct_map={
        "all": "all_state_based_conflicts",
        "civil war": "intrastate_conflicts",
        "others (non-civil)": "interstate_conflicts",
    },
    by_sub_type_labels={
        "all_state_based_conflicts": [
            ("intrastate_conflicts", "Civil wars"),
            ("interstate_conflicts", "Interstate wars"),
        ],
    },
    by_sub_type_measures={
        "all_state_based_conflicts": {"conflict_deaths", "death_rate", "number_of_conflicts", "conflict_rate"},
    },
    dod={
        "all_state_based_conflicts": "[interstate](#dod:interstate-cow) and [civil](#dod:intrastate-cow) wars",
        "interstate_conflicts": "[non-civil wars](#dod:non-civil-war-mars)",
        "intrastate_conflicts": "[civil wars](#dod:civil-war-mars)",
    },
    dod_by_sub_type={
        "all_state_based_conflicts": "[civil](#dod:civil-war-mars) and [non-civil](#dod:non-civil-war-mars) wars",
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
        "conflict_deaths",
        "death_rate",
        "number_of_conflicts",
        "conflict_rate",
        "conflict_locations",
        "conflict_participants",
    },
    ct_map={
        "all": "all_armed_conflicts",
        "state-based": "all_state_based_conflicts",
        "inter-state": "interstate_conflicts",
        "intra-state": "intrastate_conflicts",
        "intra-state (internationalized)": "_intrastate_int",
        "intra-state (non-internationalized)": "_intrastate_non_int",
        "extra-state": "extrastate_conflicts",
        "non-state": "non_state_conflicts",
    },
    by_sub_type_labels={
        "all_armed_conflicts": [
            ("non_state_conflicts", "Non-state wars"),
            ("intrastate_conflicts", "Intrastate wars"),
            ("extrastate_conflicts", "Extrastate wars"),
            ("interstate_conflicts", "Interstate wars"),
        ],
        "intrastate_conflicts": [
            ("_intrastate_non_int", "Non-internationalized intrastate"),
            ("_intrastate_int", "Internationalized intrastate"),
        ],
    },
    by_sub_type_measures={
        "all_armed_conflicts": {"conflict_deaths", "death_rate", "number_of_conflicts", "conflict_rate"},
        "intrastate_conflicts": {"number_of_conflicts", "conflict_rate"},
    },
    ct_name_override={
        "all_armed_conflicts": "wars",
        "all_state_based_conflicts": "state-based wars",
        "interstate_conflicts": "interstate wars",
        "intrastate_conflicts": "intrastate wars",
        "extrastate_conflicts": "extrastate wars",
        "non_state_conflicts": "non-state wars",
    },
    ct_short_override={
        "interstate_conflicts": "Interstate wars",
        "intrastate_conflicts": "Intrastate wars",
        "extrastate_conflicts": "Extrastate wars",
        "non_state_conflicts": "Non-state wars",
    },
    dod={
        "all_armed_conflicts": "[wars](#dod:war-cow)",
        "all_state_based_conflicts": "[state-based wars](#dod:state-based-war-cow)",
        "interstate_conflicts": "[interstate wars](#dod:interstate-war-cow)",
        "intrastate_conflicts": "[intrastate wars](#dod:intrastate-war-cow)",
        "extrastate_conflicts": "[extrastate wars](#dod:extrastate-war-cow)",
        "non_state_conflicts": "[non-state wars](#dod:non-state-war-cow)",
    },
    dod_by_sub_type={
        "all_armed_conflicts": "[interstate](#dod:interstate-war-cow), [intrastate](#dod:intrastate-war-cow), [extrastate](#dod:extrastate-war-cow), and [non-state](#dod:non-state-war-cow) wars",
        "intrastate_conflicts": "[non-internationalized and internationalized intrastate wars](#dod:intrastate-war-cow)",
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
        "conflict_deaths",
        "death_rate",
        "number_of_conflicts",
        "conflict_rate",
        "conflict_participants",
    },
    ct_map={
        "all": "all_state_based_conflicts",
        "state-based": "all_state_based_conflicts",
        "interstate": "interstate_conflicts",
        "intrastate": "intrastate_conflicts",
        "intrastate (internationalized)": "_intrastate_int",
        "intrastate (non-internationalized)": "_intrastate_non_int",
        "extrasystemic": "extrastate_conflicts",
    },
    deaths_family="number_deaths_ongoing_conflicts_battle",
    by_sub_type_labels={
        "all_state_based_conflicts": [
            ("_intrastate_int", "Internationalized intrastate"),
            ("_intrastate_non_int", "Non-internationalized intrastate"),
            ("extrastate_conflicts", "Extrasystemic"),
            ("interstate_conflicts", "Interstate"),
        ],
        "intrastate_conflicts": [
            ("_intrastate_non_int", "Non-internationalized intrastate"),
            ("_intrastate_int", "Internationalized intrastate"),
        ],
    },
    by_sub_type_measures={
        "all_state_based_conflicts": {"conflict_deaths", "death_rate", "number_of_conflicts", "conflict_rate"},
        "intrastate_conflicts": {"number_of_conflicts", "conflict_rate"},
    },
    dod={
        "all_state_based_conflicts": "[state-based conflicts](#dod:state-based-conflict-prio)",
        "interstate_conflicts": "[interstate conflicts](#dod:interstate-prio)",
        "intrastate_conflicts": "[intrastate conflicts](#dod:intrastate-prio)",
        "extrastate_conflicts": "[extrasystemic conflicts](#dod:extrasystemic-prio)",
    },
    dod_by_sub_type={
        "all_state_based_conflicts": "[interstate](#dod:interstate-prio), [intrastate](#dod:intrastate-prio), and [extrasystemic](#dod:extrasystemic-prio) conflicts",
        "intrastate_conflicts": "[non-internationalized and internationalized intrastate conflicts](#dod:intrastate-prio)",
    },
)


# ===========================================================================
# Entry point
# ===========================================================================

# Sources built programmatically. The rest still load from the YAML.
# TODO: add MIE + COW-MID once they're migrated; the YAML's `views:` section
# can then be removed entirely.
PROGRAMMATIC_SPECS: list[SourceSpec] = [UCDP_SPEC, UCDP_PRIO_SPEC, MARS_SPEC, COW_SPEC, PRIO_SPEC]


def run() -> None:
    yaml_config = paths.load_collection_config()

    # The YAML carries views only for sources not yet migrated (MIE + COW-MID).
    # It still defines all five dims and the explorer-level config.
    yaml_explorer = paths.create_collection(config=yaml_config, explorer=True)

    programmatic_subs = [build_source_explorer(spec, yaml_config) for spec in PROGRAMMATIC_SPECS]
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
