"""Conflict Data Source explorer.

Each data source is described by a small `SourceSpec` and built programmatically
from its grapher table(s). The build is a universal pipeline (see
`build_source_explorer`) parameterised by the spec:

  1. Load the source's table(s) and apply `_adjust_source_dimensions` — a
     generic adjuster that sets the explorer's dims (`measure`,
     `conflict_type`, `conflict_sub_type`, `sub_measure`, plus a helper
     `_estimate` for CI-bearing measures) on every column's metadata.
  2. `paths.create_collection(tb=…)` auto-expands one view per column.
  3. `group_views` on the helper `_estimate` dim merges the (best, low, high)
     deaths triplets into single CI-stacked views.
  4. `_build_by_sub_type_views` constructs the stacked-across-children views
     using the spec's `by_sub_type_labels`.
  5. Helper conflict_type slugs are renamed (e.g. `_intrastate_int` →
     `intrastate_conflicts` with `conflict_sub_type=only_internationalized_conflicts`).
  6. Each view's title / subtitle / note / display blocks are filled in by
     template (parameterised by the spec's DoD anchors and `deaths_noun`).

All seven sub-explorers are joined under a leading `data_source` dropdown by
`combine_collections`. The YAML carries the dim definitions and the
explorer-level config but no view content.

Sources currently programmatic: ucdp, ucdp_prio, mars. The other four
(cow, prio, mie, cow_mid) still load their views from the YAML until they get
their `SourceSpec`.
"""

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from owid.catalog.tables import Table

from etl.collection.core.combine import combine_collections
from etl.helpers import PathFinder

paths = PathFinder(__file__)


# ===========================================================================
# SourceSpec — everything source-specific lives here
# ===========================================================================


@dataclass
class SourceSpec:
    """Declarative description of one data source.

    Most fields default to UCDP/COW/MARS-style naming; override per source.
    """

    # Identity
    slug: str  # data_source dim choice (e.g. "ucdp")
    name: str  # display name
    dataset_path: str  # path to load_dataset()

    # Tables
    main_table: str  # e.g. "ucdp"
    country_table: str | None = None  # for conflict_participants
    locations_table: str | None = None  # for conflict_locations

    # Column family base names. Override only when the source deviates from
    # the UCDP-style naming.
    deaths_family: str = "number_deaths_ongoing_conflicts"
    new_conflicts_family: str = "number_new_conflicts"
    ongoing_conflicts_family: str = "number_ongoing_conflicts"

    # Measures this source contributes (subset of the explorer's six measures)
    measures: set[str] = field(default_factory=set)

    # Table conflict_type value → explorer slug. Use the helpers
    # "_intrastate_int" / "_intrastate_non_int" for the internationalized
    # sub-types; they get renamed back to intrastate_conflicts in post-process.
    ct_map: dict[str, str] = field(default_factory=dict)

    # by_sub_type structure: parent CT → ordered list of (child_slug, label).
    by_sub_type_labels: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    # Which measures support by_sub_type for each parent.
    by_sub_type_measures: dict[str, set[str]] = field(default_factory=dict)

    # Conflict types that count as parents (their single view uses
    # `conflict_sub_type=all_sub_types` instead of `na`).
    parent_cts: set[str] = field(
        default_factory=lambda: {"all_armed_conflicts", "all_state_based_conflicts", "intrastate_conflicts"}
    )
    # Conflict types that label their counts view as `all_sub_types` even when
    # they're atomic (legacy quirk for interstate counts/rate).
    counts_use_all_sub_types: set[str] = field(default_factory=lambda: {"interstate_conflicts"})

    # Sub-measure for deaths views. UCDP uses "country_and_region_data" because
    # its deaths data is split across the main + locations tables; everyone else
    # uses just "regional_data".
    deaths_sub_measure: str = "regional_data"

    # Per-CT name overrides (used in titles + display labels). Falls back to the
    # global CT_NAME / CT_SHORT if not set. Sources like COW that say "interstate
    # wars" instead of "interstate conflicts" populate these.
    ct_name_override: dict[str, str] = field(default_factory=dict)
    ct_short_override: dict[str, str] = field(default_factory=dict)

    # Per-(measure, conflict_type) title overrides — last resort for cases where
    # the templated title doesn't fit. Falls back to template if not set.
    title_overrides: dict[tuple[str, str], str] = field(default_factory=dict)

    # FAUST parameters
    deaths_noun: str = "Deaths"  # PRIO: "Battle deaths"
    dod: dict[str, str] = field(default_factory=dict)  # CT slug → DoD link
    dod_by_sub_type: dict[str, str] = field(default_factory=dict)
    related_question_text: str = "How do different approaches measure armed conflicts and their deaths?"
    related_question_url: str = (
        "https://ourworldindata.org/conflict-data-how-do-researchers-measure-armed-conflicts-and-their-deaths"
    )

    # Map-tab + colorScale settings for deaths views (per (conflict_type, conflict_sub_type))
    deaths_map_views: set[tuple[str, str]] = field(default_factory=set)
    deaths_map_with_cs: set[tuple[str, str]] = field(default_factory=set)


# ---------------------------------------------------------------------------
# Display & chart-config constants (shared across sources)
# ---------------------------------------------------------------------------

BEST_DISPLAY = {"name": "Best estimate", "color": "#B13507"}
LOW_DISPLAY = {"name": "Low estimate", "color": "#C3AEA6"}
HIGH_DISPLAY = {"name": "High estimate", "color": "#C3AEA6"}

DEATHS_MAP_BINS = "0.99, #E8F4EA, 0;10;100;1000;10000;100000"
RATE_MAP_BINS = "0.1; 0.3; 1; 3; 10; 30; 100"
BOOL_MAP_BINS = "0,#92C5DE,No;1,#F4A582,Yes"

STACKED_CONFIG = {
    "type": "StackedBar",
    "baseColorScheme": "stackedAreaDefault",
    "hideRelativeToggle": False,
    "selectedFacetStrategy": "entity",
}
MAP_CONFIG = {"hasMapTab": True, "tab": "map", "selectedFacetStrategy": "entity"}


# Human-readable CT names shared across sources. Sources can override entries by
# providing extra keys, but the defaults match the standard explorer slugs.
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


# ===========================================================================
# Generic dimension adjustment
# ===========================================================================


def _conflict_sub_type(spec: SourceSpec, measure: str, ctype: str) -> str:
    """`conflict_sub_type` value for a single-indicator (or CI-stacked deaths) view."""
    if ctype == "_intrastate_int":
        return "only_internationalized_conflicts"
    if ctype == "_intrastate_non_int":
        return "only_non_internationalized_conflicts"
    if measure in ("conflict_deaths", "death_rate"):
        return "all_sub_types" if ctype in spec.parent_cts else "na"
    if measure in ("number_of_conflicts", "conflict_rate"):
        if ctype in spec.parent_cts or ctype in spec.counts_use_all_sub_types:
            return "all_sub_types"
        return "na"
    # locations / participants
    return "all_sub_types" if ctype == "intrastate_conflicts" else "na"


def _parse_main_col(spec: SourceSpec, short: str, ct_raw: str) -> dict[str, Any] | None:
    """Parse a column from the source's main table. Returns dim assignments or None to drop."""
    ctype = spec.ct_map.get(ct_raw)
    if ctype is None:
        return None

    # Deaths / death-rate families (CI: best/low/high × _per_capita)
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
        sub_measure = "na" if ctype in ("_intrastate_int", "_intrastate_non_int") else spec.deaths_sub_measure
        return {
            "measure": measure,
            "conflict_type": ctype,
            "conflict_sub_type": _conflict_sub_type(spec, measure, ctype),
            "sub_measure": sub_measure,
            "_estimate": estimate,
        }

    # Count / rate families. Only interstate-ongoing uses per_country_pair; drop
    # the other per_country_pair variants and the per_country variant for
    # interstate ongoing.
    for ongoing in (True, False):
        family = spec.ongoing_conflicts_family if ongoing else spec.new_conflicts_family
        sub_measure = "all_ongoing_conflicts" if ongoing else "only_new_conflicts"
        if short == family:
            measure = "number_of_conflicts"
            if measure not in spec.measures:
                return None
            return {
                "measure": measure,
                "conflict_type": ctype,
                "conflict_sub_type": _conflict_sub_type(spec, measure, ctype),
                "sub_measure": sub_measure,
                "_estimate": "_na",
            }
        if short == f"{family}_per_country_pair":
            if not (ongoing and ctype == "interstate_conflicts"):
                return None  # drop
            measure = "conflict_rate"
            if measure not in spec.measures:
                return None
            return {
                "measure": measure,
                "conflict_type": ctype,
                "conflict_sub_type": _conflict_sub_type(spec, measure, ctype),
                "sub_measure": sub_measure,
                "_estimate": "_na",
            }
        if short == f"{family}_per_country":
            if ctype == "interstate_conflicts" and ongoing:
                return None  # interstate ongoing uses per_country_pair
            measure = "conflict_rate"
            if measure not in spec.measures:
                return None
            return {
                "measure": measure,
                "conflict_type": ctype,
                "conflict_sub_type": _conflict_sub_type(spec, measure, ctype),
                "sub_measure": sub_measure,
                "_estimate": "_na",
            }
    return None  # any other column family (civilians/combatants/unknown) → drop


def _parse_country_col(spec: SourceSpec, short: str, ct_raw: str) -> dict[str, Any] | None:
    ctype = spec.ct_map.get(ct_raw)
    if ctype is None or ctype == "non_state_conflicts":
        return None
    if "conflict_participants" not in spec.measures:
        return None
    if short == "number_participants":
        return {
            "measure": "conflict_participants",
            "conflict_type": ctype,
            "conflict_sub_type": _conflict_sub_type(spec, "conflict_participants", ctype),
            "sub_measure": "regional_data",
            "_estimate": "_na",
        }
    if short == "participated_in_conflict":
        return {
            "measure": "conflict_participants",
            "conflict_type": ctype,
            "conflict_sub_type": _conflict_sub_type(spec, "conflict_participants", ctype),
            "sub_measure": "country_level_data",
            "_estimate": "_na",
        }
    return None


def _parse_locations_col(spec: SourceSpec, short: str, ct_raw: str) -> dict[str, Any] | None:
    ctype = spec.ct_map.get(ct_raw)
    if ctype is None:
        return None
    if "conflict_locations" not in spec.measures:
        return None
    if short == "number_locations":
        return {
            "measure": "conflict_locations",
            "conflict_type": ctype,
            "conflict_sub_type": _conflict_sub_type(spec, "conflict_locations", ctype),
            "sub_measure": "regional_data",
            "_estimate": "_na",
        }
    if short == "is_location_of_conflict":
        return {
            "measure": "conflict_locations",
            "conflict_type": ctype,
            "conflict_sub_type": _conflict_sub_type(spec, "conflict_locations", ctype),
            "sub_measure": "country_level_data",
            "_estimate": "_na",
        }
    return None


def _adjust_table(tb: Table, spec: SourceSpec, parse: Callable) -> Table:
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
    if isinstance(tb.metadata.dimensions, list):
        existing = {d.get("slug") for d in tb.metadata.dimensions}
        for slug in ("conflict_sub_type", "sub_measure", "_estimate"):
            if slug not in existing:
                tb.metadata.dimensions.append({"name": slug, "slug": slug})
    return tb


def _load_and_adjust(spec: SourceSpec) -> list[Table]:
    """Load every table the spec references and apply `_adjust_table`."""
    ds = paths.load_dataset(spec.dataset_path)
    tables: list[Table] = [_adjust_table(ds.read(spec.main_table, load_data=False), spec, _parse_main_col)]
    if spec.country_table:
        tables.append(_adjust_table(ds.read(spec.country_table, load_data=False), spec, _parse_country_col))
    if spec.locations_table:
        tables.append(_adjust_table(ds.read(spec.locations_table, load_data=False), spec, _parse_locations_col))
    return tables


# ===========================================================================
# By-sub-type stacks (generic)
# ===========================================================================


def _build_by_sub_type_views(c, spec: SourceSpec) -> None:
    """Construct by_sub_type stacked views by combining child indicators."""
    from etl.collection.model.view import Indicator, View, ViewIndicators

    # Indexes by (measure, conflict_type, sub_measure)
    by_path: dict[tuple, list] = {}
    by_path_loose: dict[tuple, list] = {}  # (measure, ct) → indicators (any sub_measure)
    for v in c.views:
        d = v.dimensions
        inds = list(v.indicators.y or [])
        by_path[(d["measure"], d["conflict_type"], d["sub_measure"])] = inds
        loose_key = (d["measure"], d["conflict_type"])
        if loose_key not in by_path_loose or d["sub_measure"] == "country_and_region_data":
            by_path_loose[loose_key] = inds

    def output_sub_measures(measure: str) -> set[str]:
        if measure in ("conflict_deaths", "death_rate"):
            return {spec.deaths_sub_measure}
        return {"all_ongoing_conflicts", "only_new_conflicts"}

    def child_indicators(measure: str, child_ct: str, sub_measure: str) -> list:
        inds = by_path.get((measure, child_ct, sub_measure))
        if inds is not None:
            return inds
        if measure in ("conflict_deaths", "death_rate"):
            return by_path_loose.get((measure, child_ct), [])
        return []

    new_views = []
    for parent, children in spec.by_sub_type_labels.items():
        for measure in spec.by_sub_type_measures.get(parent, set()):
            for sub_measure in output_sub_measures(measure):
                stacked_y = []
                for child_ct, label in children:
                    inds = child_indicators(measure, child_ct, sub_measure)
                    if not inds:
                        continue
                    if measure in ("conflict_deaths", "death_rate") and len(inds) > 1:
                        # Prefer "best" (center) estimate; fall back to "low" if a
                        # source (e.g. MARS) doesn't carry a center.
                        pick = next(
                            (i for i in inds if "_low_" not in i.catalogPath and "_high_" not in i.catalogPath),
                            None,
                        )
                        if pick is None:
                            pick = next((i for i in inds if "_low_" in i.catalogPath), None)
                        if pick is None:
                            continue
                        stacked_y.append(Indicator(catalogPath=pick.catalogPath, display={"name": label}))
                    else:
                        for i in inds:
                            stacked_y.append(Indicator(catalogPath=i.catalogPath, display={"name": label}))
                if not stacked_y:
                    continue
                new_views.append(
                    View(
                        dimensions={
                            "measure": measure,
                            "conflict_type": parent,
                            "conflict_sub_type": "by_sub_type",
                            "sub_measure": sub_measure,
                        },
                        indicators=ViewIndicators(y=stacked_y),
                        config={},
                    )
                )
    c.views.extend(new_views)


# ===========================================================================
# Dim cleanup helpers (generic)
# ===========================================================================


def _drop_dim(c, dim_slug: str) -> None:
    c.dimensions = [d for d in c.dimensions if d.slug != dim_slug]
    for v in c.views:
        v.dimensions.pop(dim_slug, None)


def _refresh_dim_choices(c, dim_slug: str, order: list[str]) -> None:
    from etl.collection.model.dimension import DimensionChoice

    dim = next(d for d in c.dimensions if d.slug == dim_slug)
    by_slug = {ch.slug: ch for ch in dim.choices}
    used = {v.dimensions.get(dim_slug) for v in c.views}
    used.discard(None)
    new_choices = []
    for slug in order:
        if slug in used:
            new_choices.append(by_slug.get(slug) or DimensionChoice(slug=slug, name=slug))
    for slug in sorted(used - set(order)):
        new_choices.append(by_slug.get(slug) or DimensionChoice(slug=slug, name=slug))
    dim.choices = new_choices


# ===========================================================================
# FAUST + display templates (parameterised by spec)
# ===========================================================================


def _ct_name(spec: SourceSpec, ctype: str) -> str:
    return spec.ct_name_override.get(ctype) or CT_NAME[ctype]


def _ct_short(spec: SourceSpec, ctype: str) -> str | None:
    if ctype in spec.ct_short_override:
        return spec.ct_short_override[ctype]
    return CT_SHORT.get(ctype)


def _set_view_config(view, spec: SourceSpec) -> None:
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

    # Apply per-source title overrides as a last step.
    override = spec.title_overrides.get((measure, ctype))
    if override:
        cfg["title"] = override

    view.config = cfg


def _deaths_text(cfg: dict[str, Any], spec: SourceSpec, measure: str, ctype: str, cst: str) -> None:
    per_capita = measure == "death_rate"
    noun = "Death rate" if per_capita else spec.deaths_noun
    name = _ct_name(spec, ctype)
    if cst == "by_sub_type":
        cfg["title"] = f"{noun} in {name} based on where they occurred"
        cfg["subtitle"] = (
            f"Deaths of combatants and civilians due to fighting, per 100,000 people. "
            f"Included are {spec.dod_by_sub_type.get(ctype, spec.dod[ctype])} that were ongoing that year."
            if per_capita
            else f"Included are deaths of combatants and civilians due to fighting in "
            f"{spec.dod_by_sub_type.get(ctype, spec.dod[ctype])} that were ongoing that year."
        )
        cfg.update(STACKED_CONFIG)
    elif cst in ("only_non_internationalized_conflicts", "only_internationalized_conflicts"):
        word = "non-internationalized" if "non" in cst else "internationalized"
        cfg["title"] = f"{noun} in {word} intrastate conflicts"
        cfg["subtitle"] = (
            f"Deaths of combatants and civilians due to fighting, per 100,000 people. "
            f"Included are [{word} intrastate conflicts](#dod:intrastate-{spec.slug.split('_')[0]}) "
            f"that were ongoing that year."
            if per_capita
            else f"Included are deaths of combatants and civilians due to fighting in "
            f"[{word} intrastate conflicts](#dod:intrastate-{spec.slug.split('_')[0]}) that were ongoing that year."
        )
        cfg["note"] = "'Best' estimates as identified by UCDP."
        cfg["selectedFacetStrategy"] = "entity"
    else:  # na / all_sub_types — CI stack
        has_map = (ctype, cst) in spec.deaths_map_views
        cfg["title"] = f"{noun} in {name} based on where they occurred" if has_map else f"{noun} in {name}"
        cfg["subtitle"] = (
            f"Deaths of combatants and civilians due to fighting, per 100,000 people. "
            f"Included are {spec.dod[ctype]} that were ongoing that year."
            if per_capita
            else f"Included are deaths of combatants and civilians due to fighting in "
            f"{spec.dod[ctype]} that were ongoing that year."
        )
        cfg["note"] = "'Best' estimates as identified by UCDP."
        if has_map:
            cfg.update(MAP_CONFIG)
        else:
            cfg["selectedFacetStrategy"] = "entity"


def _count_text(cfg: dict[str, Any], spec: SourceSpec, measure: str, ctype: str, cst: str, sub_measure: str) -> None:
    rate = measure == "conflict_rate"
    is_new = sub_measure == "only_new_conflicts"
    name = _ct_name(spec, ctype)
    new_prefix = "new " if is_new else ""
    lead = "Rate of" if rate else "Number of"
    verb = "started that year" if is_new else "were ongoing that year"
    if cst == "by_sub_type":
        cfg["title"] = f"{lead} {new_prefix}{name}"
        cfg["subtitle"] = (
            f"The number of conflicts divided by the number of all states. This accounts for "
            f"the changing number of states over time. Included are {spec.dod_by_sub_type.get(ctype, spec.dod[ctype])} that {verb}."
            if rate
            else f"Included are {spec.dod_by_sub_type.get(ctype, spec.dod[ctype])} that {verb}."
        )
        cfg["note"] = (
            "Some conflicts affect several regions, and do not necessarily start at the same time "
            "across them. The sum across all regions can therefore be higher than the global number."
            if is_new
            else "Some conflicts affect several regions. The sum across all regions can therefore be higher than the global number."
        )
        cfg.update(STACKED_CONFIG)
    else:
        cfg["title"] = f"{lead} {new_prefix}{name}"
        if rate and ctype == "interstate_conflicts":
            cfg["subtitle"] = (
                "The number of conflicts divided by the number of all state-pairs. This accounts "
                f"for the changing number of states over time. Included are {spec.dod[ctype]} that {verb}."
            )
        elif rate:
            cfg["subtitle"] = (
                "The number of conflicts divided by the number of all states. This accounts for "
                f"the changing number of states over time. Included are {spec.dod[ctype]} that {verb}."
            )
        else:
            cfg["subtitle"] = f"Included are {spec.dod[ctype]} that {verb}."


def _participants_text(cfg: dict[str, Any], spec: SourceSpec, ctype: str, sub_measure: str) -> None:
    name = _ct_name(spec, ctype)
    country_level = sub_measure == "country_level_data"
    cfg["title"] = f"States involved in {name}" if country_level else f"Number of states involved in {name}"
    if ctype == "all_state_based_conflicts":
        dod_sing = (
            "[interstate](#dod:interstate-ucdp), [intrastate](#dod:intrastate-ucdp), "
            "or [extrasystemic](#dod:extrasystemic-ucdp) conflict"
        )
    else:
        dod_sing = spec.dod[ctype].replace("conflicts](", "conflict](")
    cfg["subtitle"] = (
        f"Included are states that were [primary participants](#dod:primary-participant-ucdp) "
        f"in at least one {dod_sing} that year."
    )
    if country_level:
        cfg.update(MAP_CONFIG)


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
    d = view.dimensions
    measure = d["measure"]
    ctype = d["conflict_type"]
    cst = d["conflict_sub_type"]
    if view.indicators is None or view.indicators.y is None:
        return
    ys = view.indicators.y

    if measure in ("conflict_deaths", "death_rate"):
        per_capita = measure == "death_rate"
        if cst == "by_sub_type":
            return
        # Distinguish a CI stack (has _low_/_high_ variants) from a single-indicator
        # view (e.g. COW deaths). Single views get a CT label, not "Best estimate".
        has_ci_variants = any(("_low_" in i.catalogPath or "_high_" in i.catalogPath) for i in ys)
        if not has_ci_variants:
            short = _ct_short(spec, ctype)
            if len(ys) == 1 and short:
                ys[0].display = {"name": short}
            return
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
    elif measure in ("number_of_conflicts", "conflict_rate"):
        if cst == "by_sub_type":
            return
        short = _ct_short(spec, ctype)
        if len(ys) == 1 and short:
            ys[0].display = {"name": short}
    elif measure in ("conflict_locations", "conflict_participants"):
        country_level = d["sub_measure"] == "country_level_data"
        for ind in ys:
            if country_level:
                ind.display = {"colorScaleNumericBins": BOOL_MAP_BINS}
            elif measure == "conflict_locations":
                ind.display = {"colorScaleNumericBins": BOOL_MAP_BINS}


# ===========================================================================
# Universal build pipeline
# ===========================================================================


def build_source_explorer(spec: SourceSpec, yaml_config: dict[str, Any]):
    """Run the universal pipeline for a single source."""
    tables = _load_and_adjust(spec)

    # Per-table indicator_names lists in the same order as `tables`:
    #   main: deaths/death_rate/number_of_conflicts/conflict_rate (filtered by spec.measures)
    #   country: conflict_participants
    #   locations: conflict_locations
    main_inds = [
        m for m in ("conflict_deaths", "death_rate", "number_of_conflicts", "conflict_rate") if m in spec.measures
    ]
    indicator_names: list[list[str]] = [main_inds]
    if spec.country_table:
        indicator_names.append(["conflict_participants"])
    if spec.locations_table:
        indicator_names.append(["conflict_locations"])

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

    # CI collapse: best+low+high → "_ci"; replace=True drops the originals.
    # Some sources (e.g., MARS) don't carry a "best" column — intersect against
    # what actually exists on the table.
    est_dim = next(d for d in c.dimensions if d.slug == "_estimate")
    ci_choices = [c_ for c_ in ("best", "low", "high") if c_ in est_dim.choice_slugs]
    if len(ci_choices) >= 2:
        c.group_views(
            groups=[{"dimension": "_estimate", "choices": ci_choices, "choice_new_slug": "_ci", "replace": True}]
        )
    _drop_dim(c, "_estimate")

    # By-sub-type stacks (manual; group_views can't combine views with
    # heterogeneous conflict_sub_type values across atomic vs parent types).
    _build_by_sub_type_views(c, spec)

    # Drop the helper intrastate-variant views for non-deaths measures — they
    # only exist as children of the by_sub_type stacks above. Only applies if
    # the source actually has these helper conflict_type choices.
    ct_dim = next(d for d in c.dimensions if d.slug == "conflict_type")
    helpers_in_use = [s for s in ("_intrastate_int", "_intrastate_non_int") if s in ct_dim.choice_slugs]
    drops_for_measures = [
        m
        for m in ("number_of_conflicts", "conflict_rate", "conflict_locations", "conflict_participants")
        if m in spec.measures
    ]
    if helpers_in_use and drops_for_measures:
        c.drop_views(
            [
                {"conflict_type": helpers_in_use, "measure": drops_for_measures},
            ]
        )

    # Rename remaining `_intrastate_int/non_int` views (deaths only) to use
    # `conflict_type=intrastate_conflicts`. The conflict_sub_type and
    # sub_measure were already set correctly in adjust_dimensions.
    for view in c.views:
        ct = view.dimensions.get("conflict_type")
        if ct in ("_intrastate_int", "_intrastate_non_int"):
            view.dimensions["conflict_type"] = "intrastate_conflicts"

    _refresh_dim_choices(
        c,
        "conflict_type",
        order=[
            "all_armed_conflicts",
            "all_state_based_conflicts",
            "interstate_conflicts",
            "intrastate_conflicts",
            "extrastate_conflicts",
            "non_state_conflicts",
            "one_sided_violence",
        ],
    )
    _refresh_dim_choices(
        c,
        "conflict_sub_type",
        order=[
            "na",
            "by_sub_type",
            "all_sub_types",
            "only_non_internationalized_conflicts",
            "only_internationalized_conflicts",
        ],
    )

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
    # Standard ct_map shared by sources that use UCDP-style conflict_type slugs
    # (UCDP, UCDP+PRIO; PRIO uses a subset).
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
    deaths_noun="Deaths",
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

UCDP_PRIO_SPEC = SourceSpec(
    slug="ucdp_prio",
    name="UCDP + PRIO",
    dataset_path="ucdp_prio",
    main_table="ucdp_prio",
    # No country or locations table.
    measures={"conflict_deaths", "death_rate"},
    # The table has "all", "non-state conflict", "one-sided violence" too but
    # the explorer doesn't use them — omit them so the columns get dropped.
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
    deaths_noun="Deaths",
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
        # MARS uses "civil war" / "others (non-civil)" rather than the UCDP names.
        # Civil wars are the intrastate-shaped wars; others (non-civil) are interstate.
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
    # MARS only carries low/high estimates (no center) — so its CI views show
    # just 2 indicators per view. Map config and CS are off everywhere for MARS.
    deaths_noun="Deaths",
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
    # COW uses hyphenated forms (inter-state, intra-state, extra-state, non-state).
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
    # COW uses "wars" instead of "conflicts" in user-facing text. Per-CT overrides
    # replace the global names.
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

PRIO_SPEC = SourceSpec(
    slug="prio",
    name="Peace Research Institute Oslo",
    dataset_path="prio_v31",
    main_table="prio_v31",
    country_table="prio_v31_country",
    # No locations table.
    measures={
        "conflict_deaths",
        "death_rate",
        "number_of_conflicts",
        "conflict_rate",
        "conflict_participants",
    },
    # PRIO's "all" represents state-based conflicts (no all_armed_conflicts).
    # The country table uses "state-based" instead of "all" — both map to the same slug.
    ct_map={
        "all": "all_state_based_conflicts",
        "state-based": "all_state_based_conflicts",
        "interstate": "interstate_conflicts",
        "intrastate": "intrastate_conflicts",
        "intrastate (internationalized)": "_intrastate_int",
        "intrastate (non-internationalized)": "_intrastate_non_int",
        "extrasystemic": "extrastate_conflicts",
    },
    # PRIO uses battle-deaths columns: `number_deaths_ongoing_conflicts_battle*`.
    deaths_family="number_deaths_ongoing_conflicts_battle",
    deaths_noun="Deaths",  # legacy title still says "Deaths in ..." (not "Battle deaths")
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
PROGRAMMATIC_SPECS: list[SourceSpec] = [
    UCDP_SPEC,
    UCDP_PRIO_SPEC,
    MARS_SPEC,
    COW_SPEC,
    PRIO_SPEC,
]


def run() -> None:
    yaml_config = paths.load_collection_config()

    # The YAML carries views only for sources we haven't migrated yet (MIE +
    # COW-MID). It still defines all five dims and the explorer-level config.
    yaml_explorer = paths.create_collection(config=yaml_config, explorer=True)

    programmatic_subs = []
    for spec in PROGRAMMATIC_SPECS:
        sub = build_source_explorer(spec, yaml_config)
        _attach_data_source_dim(sub, spec, yaml_explorer)
        programmatic_subs.append(sub)

    final = combine_collections(
        collections=[*programmatic_subs, yaml_explorer],
        catalog_path=yaml_explorer.catalog_path,
        config={"config": yaml_config.get("config", {})},
    )
    final.save(tolerate_extra_indicators=True)


def _attach_data_source_dim(sub_explorer, spec: SourceSpec, yaml_explorer) -> None:
    """Insert a data_source dim that mirrors the YAML's definition + tag views."""
    from etl.collection.model.dimension import Dimension, DimensionChoice

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
