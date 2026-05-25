"""Conflict Data Source explorer.

UCDP is built programmatically from its three tables using the same pattern as
`etl/steps/export/multidim/war/latest/ucdp.py`:

  1. `adjust_ucdp_dimensions` sets the explorer's dims (measure, conflict_type,
     conflict_sub_type, sub_measure) on each column's metadata. Deaths columns
     additionally carry a helper `_estimate` dim so the CI variants can be
     collapsed in step 3.
  2. `paths.create_collection(tb=[3 tables])` auto-expands one view per column.
  3. `group_views` on the helper `_estimate` dim merges the (best, low, high)
     deaths triplets into single CI-stacked views.
  4. `_build_by_sub_type_views` constructs the stacked-across-conflict-type
     views by combining child indicators.
  5. A short post-process step renames the helper `_intrastate_int` /
     `_intrastate_non_int` conflict types into `intrastate_conflicts` with the
     appropriate `conflict_sub_type`.
  6. Each view's title / subtitle / note / display blocks are filled in by
     iterating `c.views`.

The other six data sources stay declared in `conflict_data_source.config.yml`;
`combine_collections` joins all seven sub-explorers under the leading
`data_source` dropdown. The same skeleton can be lifted to those sources once
their FAUST has been reviewed.
"""

from copy import deepcopy
from typing import Any

from owid.catalog.tables import Table

from etl.collection.core.combine import combine_collections
from etl.helpers import PathFinder

paths = PathFinder(__file__)


# ---------------------------------------------------------------------------
# UCDP — column-to-dim mapping
# ---------------------------------------------------------------------------

# Table `conflict_type` value → explorer slug. The two intrastate sub-types
# get helper slugs that we rename back to `intrastate_conflicts` after building
# the by-sub-type stacks.
UCDP_CT_MAP = {
    "all": "all_armed_conflicts",
    "state-based": "all_state_based_conflicts",
    "interstate": "interstate_conflicts",
    "intrastate": "intrastate_conflicts",
    "intrastate (internationalized)": "_intrastate_int",
    "intrastate (non-internationalized)": "_intrastate_non_int",
    "extrasystemic": "extrastate_conflicts",
    "non-state conflict": "non_state_conflicts",
    "one-sided violence": "one_sided_violence",
}

# The "parent" conflict types: their single view labels `conflict_sub_type` as
# "all_sub_types" (vs "na" for atomic types). Matches the legacy explorer.
PARENT_CT = {"all_armed_conflicts", "all_state_based_conflicts", "intrastate_conflicts"}

# `by_sub_type` stack children, in stack order (bottom→top), per parent.
BY_SUBTYPE_LABELS = {
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
}

# Which (measure, parent_ct) combos have a by_sub_type view in the legacy
# explorer. Intrastate by_sub_type isn't shown for deaths/death_rate.
BY_SUBTYPE_MEASURES = {
    "all_armed_conflicts": {"conflict_deaths", "death_rate", "number_of_conflicts", "conflict_rate"},
    "all_state_based_conflicts": {"conflict_deaths", "death_rate", "number_of_conflicts", "conflict_rate"},
    "intrastate_conflicts": {"number_of_conflicts", "conflict_rate"},
}


def _conflict_sub_type(measure: str, ctype: str) -> str:
    """`conflict_sub_type` value for a single-indicator (or CI-stacked deaths) view."""
    if ctype == "_intrastate_int":
        return "only_internationalized_conflicts"
    if ctype == "_intrastate_non_int":
        return "only_non_internationalized_conflicts"
    if measure in ("conflict_deaths", "death_rate"):
        return "all_sub_types" if ctype in PARENT_CT else "na"
    if measure in ("number_of_conflicts", "conflict_rate"):
        return "all_sub_types" if (ctype in PARENT_CT or ctype == "interstate_conflicts") else "na"
    # locations / participants: only intrastate uses "all_sub_types"
    return "all_sub_types" if ctype == "intrastate_conflicts" else "na"


def _sub_measure_for_intrastate_variant(measure: str) -> str:
    """Intrastate-only_* views use sub_measure=na regardless of which table the column
    actually came from — that's how the legacy explorer labels them."""
    return "na"


def _parse_main_col(short: str, ct_raw: str) -> dict[str, Any] | None:
    """Return dim assignments for a column from the `ucdp` table, or None to drop it."""
    ctype = UCDP_CT_MAP.get(ct_raw)
    if ctype is None:
        return None

    # Deaths / death rate (CI: best/low/high)
    deaths_map = {
        "number_deaths_ongoing_conflicts": ("conflict_deaths", "best"),
        "number_deaths_ongoing_conflicts_low": ("conflict_deaths", "low"),
        "number_deaths_ongoing_conflicts_high": ("conflict_deaths", "high"),
        "number_deaths_ongoing_conflicts_per_capita": ("death_rate", "best"),
        "number_deaths_ongoing_conflicts_low_per_capita": ("death_rate", "low"),
        "number_deaths_ongoing_conflicts_high_per_capita": ("death_rate", "high"),
    }
    if short in deaths_map:
        measure, estimate = deaths_map[short]
        sub_measure = (
            _sub_measure_for_intrastate_variant(measure)
            if ctype in ("_intrastate_int", "_intrastate_non_int")
            else "country_and_region_data"
        )
        return {
            "measure": measure,
            "conflict_type": ctype,
            "conflict_sub_type": _conflict_sub_type(measure, ctype),
            "sub_measure": sub_measure,
            "_estimate": estimate,
        }

    # Number / rate of conflicts. The legacy picks per_country_pair *only* for
    # interstate ongoing rate; drop the other unused variants.
    is_ongoing = short.startswith("number_ongoing_conflicts")
    is_new = short.startswith("number_new_conflicts")
    if not (is_ongoing or is_new):
        return None
    sub_measure = "all_ongoing_conflicts" if is_ongoing else "only_new_conflicts"
    if short in ("number_ongoing_conflicts", "number_new_conflicts"):
        measure = "number_of_conflicts"
    elif short == "number_ongoing_conflicts_per_country_pair":
        if not (is_ongoing and ctype == "interstate_conflicts"):
            return None
        measure = "conflict_rate"
    elif short == "number_ongoing_conflicts_per_country":
        if ctype == "interstate_conflicts":
            return None  # use per_country_pair for interstate ongoing
        measure = "conflict_rate"
    elif short == "number_new_conflicts_per_country":
        measure = "conflict_rate"
    else:
        return None
    return {
        "measure": measure,
        "conflict_type": ctype,
        "conflict_sub_type": _conflict_sub_type(measure, ctype),
        "sub_measure": sub_measure,
        "_estimate": "_na",
    }


def _parse_country_col(short: str, ct_raw: str) -> dict[str, Any] | None:
    """Return dim assignments for a column from the `ucdp_country` table, or None."""
    ctype = UCDP_CT_MAP.get(ct_raw)
    if ctype is None or ctype == "non_state_conflicts":
        return None
    if short == "number_participants":
        return {
            "measure": "conflict_participants",
            "conflict_type": ctype,
            "conflict_sub_type": _conflict_sub_type("conflict_participants", ctype),
            "sub_measure": "regional_data",
            "_estimate": "_na",
        }
    if short == "participated_in_conflict":
        return {
            "measure": "conflict_participants",
            "conflict_type": ctype,
            "conflict_sub_type": _conflict_sub_type("conflict_participants", ctype),
            "sub_measure": "country_level_data",
            "_estimate": "_na",
        }
    return None


def _parse_locations_col(short: str, ct_raw: str) -> dict[str, Any] | None:
    """Return dim assignments for a column from the `ucdp_locations` table, or None."""
    ctype = UCDP_CT_MAP.get(ct_raw)
    if ctype is None:
        return None
    if short == "number_locations":
        return {
            "measure": "conflict_locations",
            "conflict_type": ctype,
            "conflict_sub_type": _conflict_sub_type("conflict_locations", ctype),
            "sub_measure": "regional_data",
            "_estimate": "_na",
        }
    if short == "is_location_of_conflict":
        return {
            "measure": "conflict_locations",
            "conflict_type": ctype,
            "conflict_sub_type": _conflict_sub_type("conflict_locations", ctype),
            "sub_measure": "country_level_data",
            "_estimate": "_na",
        }
    return None


def _adjust_table(tb: Table, parse) -> Table:
    drops: list[str] = []
    for col in list(tb.columns):
        if col in ("year", "country"):
            continue
        meta = tb[col].metadata
        ct_raw = (meta.dimensions or {}).get("conflict_type")
        if not ct_raw or not meta.original_short_name:
            drops.append(col)
            continue
        dims = parse(meta.original_short_name, ct_raw)
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


def adjust_ucdp_dimensions(tb_main: Table, tb_country: Table, tb_locations: Table) -> tuple[Table, Table, Table]:
    """Set explorer-aligned dims on each UCDP column."""
    return (
        _adjust_table(tb_main, _parse_main_col),
        _adjust_table(tb_country, _parse_country_col),
        _adjust_table(tb_locations, _parse_locations_col),
    )


# ---------------------------------------------------------------------------
# Display + chart-config constants
# ---------------------------------------------------------------------------

BEST_DISPLAY = {"name": "Best estimate", "color": "#B13507"}
LOW_DISPLAY = {"name": "Low estimate", "color": "#C3AEA6"}
HIGH_DISPLAY = {"name": "High estimate", "color": "#C3AEA6"}

DEATHS_MAP_BINS = "0.99, #E8F4EA, 0;10;100;1000;10000;100000"
RATE_MAP_BINS = "0.1; 0.3; 1; 3; 10; 30; 100"
BOOL_MAP_BINS = "0,#92C5DE,No;1,#F4A582,Yes"

DEATHS_MAP_VIEWS: set[tuple[str, str]] = {
    ("all_armed_conflicts", "all_sub_types"),
    ("all_state_based_conflicts", "all_sub_types"),
    ("interstate_conflicts", "na"),
    ("intrastate_conflicts", "all_sub_types"),
    ("non_state_conflicts", "na"),
    ("one_sided_violence", "na"),
}
DEATHS_MAP_VIEWS_WITH_CS: set[tuple[str, str]] = {
    ("interstate_conflicts", "na"),
    ("intrastate_conflicts", "all_sub_types"),
    ("non_state_conflicts", "na"),
    ("one_sided_violence", "na"),
}

STACKED_CONFIG = {
    "type": "StackedBar",
    "baseColorScheme": "stackedAreaDefault",
    "hideRelativeToggle": False,
    "selectedFacetStrategy": "entity",
}
MAP_CONFIG = {"hasMapTab": True, "tab": "map", "selectedFacetStrategy": "entity"}

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
DOD = {
    "all_armed_conflicts": "[armed conflicts](#dod:armed-conflict-ucdp)",
    "all_state_based_conflicts": "[interstate](#dod:interstate-ucdp), [intrastate](#dod:intrastate-ucdp), and [extrasystemic](#dod:extrasystemic-ucdp) conflicts",
    "interstate_conflicts": "[interstate conflicts](#dod:interstate-ucdp)",
    "intrastate_conflicts": "[intrastate conflicts](#dod:intrastate-ucdp)",
    "extrastate_conflicts": "[extrasystemic conflicts](#dod:extrasystemic-ucdp)",
    "non_state_conflicts": "[non-state conflicts](#dod:nonstate-ucdp)",
    "one_sided_violence": "[one-sided violence](#dod:onesided-ucdp)",
}
DOD_BY_SUBTYPE = {
    "all_armed_conflicts": "[interstate](#dod:interstate-ucdp), [intrastate](#dod:intrastate-ucdp), [extrasystemic](#dod:extrasystemic-ucdp), [non-state](#dod:nonstate-ucdp) conflicts, and [one-sided violence](#dod:onesided-ucdp)",
    "all_state_based_conflicts": "[interstate](#dod:interstate-ucdp), [intrastate](#dod:intrastate-ucdp), and [extrasystemic](#dod:extrasystemic-ucdp) conflicts",
    "intrastate_conflicts": "[non-internationalized and internationalized intrastate conflicts](#dod:intrastate-ucdp)",
}


# ---------------------------------------------------------------------------
# UCDP explorer builder
# ---------------------------------------------------------------------------


def build_ucdp_explorer(yaml_config: dict[str, Any]):
    """Build the UCDP sub-explorer from the three UCDP tables.

    Uses the YAML config's dim definitions so this sub-explorer's dims match
    the structure expected by `combine_collections` (same slug order, names,
    presentation). Only the choice sets differ.
    """
    ds = paths.load_dataset("ucdp")
    tb_main, tb_country, tb_locations = adjust_ucdp_dimensions(
        ds.read("ucdp", load_data=False),
        ds.read("ucdp_country", load_data=False),
        ds.read("ucdp_locations", load_data=False),
    )

    # Use the YAML's dim definitions (drops data_source for now — we re-attach
    # it after building so each UCDP view has data_source="ucdp").
    sub_config = {
        "config": {},
        "definitions": yaml_config.get("definitions", {}),
        "dimensions": [d for d in yaml_config["dimensions"] if d["slug"] != "data_source"],
        "views": [],
    }

    c = paths.create_collection(
        config=sub_config,
        tb=[tb_main, tb_country, tb_locations],
        indicator_names=[
            ["conflict_deaths", "death_rate", "number_of_conflicts", "conflict_rate"],
            ["conflict_participants"],
            ["conflict_locations"],
        ],
        dimensions=["conflict_type", "conflict_sub_type", "sub_measure", "_estimate"],
        indicators_slug="measure",
        short_name="ucdp",
        explorer=True,
    )

    # Collapse the CI helper dim: for deaths/death_rate, merge best+low+high
    # into a single stacked view; replace=True drops the originals.
    c.group_views(
        groups=[
            {
                "dimension": "_estimate",
                "choices": ["best", "low", "high"],
                "choice_new_slug": "_ci",
                "replace": True,
            }
        ]
    )
    # The `_estimate` dim now only has "_ci" and "_na" left; drop_dimensions_if_single_choice
    # ran during group_views, but since both values exist, the dim is still around.
    # Drop it manually: it carries no useful information by now.
    _drop_dim(c, "_estimate")

    # Build by_sub_type stacked views (across child conflict_types). Done
    # manually because group_views can't combine views with heterogeneous
    # conflict_sub_type values (interstate uses "all_sub_types" for counts but
    # "na" for deaths, etc.).
    _build_by_sub_type_views(c)

    # Drop helper intrastate variant views for non-deaths measures — those
    # variants are only used as children of by_sub_type stacks.
    c.drop_views(
        [
            {
                "conflict_type": ["_intrastate_int", "_intrastate_non_int"],
                "measure": ["number_of_conflicts", "conflict_rate", "conflict_locations", "conflict_participants"],
            },
        ]
    )

    # Rename the remaining `_intrastate_int` / `_intrastate_non_int` views (deaths only)
    # to use `conflict_type=intrastate_conflicts`. The conflict_sub_type and
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

    # Fill in the per-view title / subtitle / note / display fields.
    for view in c.views:
        _set_view_config(view)
        _set_view_displays(view)

    return c


def _drop_dim(c, dim_slug: str) -> None:
    """Drop a dimension from a Collection and from each view's dimensions dict."""
    c.dimensions = [d for d in c.dimensions if d.slug != dim_slug]
    for v in c.views:
        v.dimensions.pop(dim_slug, None)


def _refresh_dim_choices(c, dim_slug: str, order: list[str]) -> None:
    """Re-list a dimension's choices in `order`, dropping unused, keeping any unexpected."""
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


def _build_by_sub_type_views(c) -> None:
    """Construct by_sub_type stacked views by combining child views' indicators.

    For deaths/death_rate, the output `sub_measure` is fixed to
    `country_and_region_data` and we look up child views by (measure, conflict_type)
    only, ignoring the child's `sub_measure` (intrastate variants carry "na").
    For count/rate measures the child's `sub_measure` matches the output's.
    """
    from etl.collection.model.view import Indicator, View, ViewIndicators

    by_path: dict[tuple, list] = {}  # (measure, conflict_type, sub_measure) → indicators
    by_path_loose: dict[tuple, list] = {}  # (measure, conflict_type) → indicators (any sub_measure)
    for v in c.views:
        d = v.dimensions
        inds = list(v.indicators.y or [])
        by_path[(d["measure"], d["conflict_type"], d["sub_measure"])] = inds
        # For loose lookup, prefer the country_and_region_data entry if there are multiple.
        loose_key = (d["measure"], d["conflict_type"])
        if loose_key not in by_path_loose or d["sub_measure"] == "country_and_region_data":
            by_path_loose[loose_key] = inds

    def output_sub_measures(parent: str, measure: str) -> set[str]:
        if measure in ("conflict_deaths", "death_rate"):
            return {"country_and_region_data"}
        # number_of_conflicts / conflict_rate
        return {"all_ongoing_conflicts", "only_new_conflicts"}

    def child_indicators(measure: str, child_ct: str, sub_measure: str) -> list:
        """Look up the child view's indicators. For deaths, fall back to a loose match
        because intrastate variants carry sub_measure="na"."""
        inds = by_path.get((measure, child_ct, sub_measure))
        if inds is not None:
            return inds
        if measure in ("conflict_deaths", "death_rate"):
            return by_path_loose.get((measure, child_ct), [])
        return []

    new_views = []
    for parent, children in BY_SUBTYPE_LABELS.items():
        for measure in BY_SUBTYPE_MEASURES.get(parent, set()):
            for sub_measure in output_sub_measures(parent, measure):
                stacked_y = []
                for child_ct, label in children:
                    inds = child_indicators(measure, child_ct, sub_measure)
                    if not inds:
                        continue
                    # Deaths CI views carry 3 indicators (best/low/high); we only want best.
                    if measure in ("conflict_deaths", "death_rate") and len(inds) > 1:
                        best = next(
                            (i for i in inds if "_low_" not in i.catalogPath and "_high_" not in i.catalogPath),
                            None,
                        )
                        if best is None:
                            continue
                        stacked_y.append(Indicator(catalogPath=best.catalogPath, display={"name": label}))
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


# ---------------------------------------------------------------------------
# FAUST + display per view
# ---------------------------------------------------------------------------


def _set_view_config(view) -> None:
    d = view.dimensions
    measure = d["measure"]
    ctype = d["conflict_type"]
    cst = d["conflict_sub_type"]
    sub_measure = d["sub_measure"]
    cfg = view.config or {}

    if measure in ("conflict_deaths", "death_rate"):
        _deaths_text(cfg, measure, ctype, cst)
    elif measure in ("number_of_conflicts", "conflict_rate"):
        _count_text(cfg, measure, ctype, cst, sub_measure)
    elif measure == "conflict_participants":
        _participants_text(cfg, ctype, sub_measure)
    elif measure == "conflict_locations":
        _locations_text(cfg, ctype, sub_measure)

    view.config = cfg


def _deaths_text(cfg: dict[str, Any], measure: str, ctype: str, cst: str) -> None:
    per_capita = measure == "death_rate"
    lead = "Death rate" if per_capita else "Deaths"
    name = CT_NAME[ctype]
    if cst == "by_sub_type":
        cfg["title"] = f"{lead} in {name} based on where they occurred"
        cfg["subtitle"] = (
            f"Deaths of combatants and civilians due to fighting, per 100,000 people. "
            f"Included are {DOD_BY_SUBTYPE[ctype]} that were ongoing that year."
            if per_capita
            else f"Included are deaths of combatants and civilians due to fighting in "
            f"{DOD_BY_SUBTYPE[ctype]} that were ongoing that year."
        )
        cfg.update(STACKED_CONFIG)
    elif cst in ("only_non_internationalized_conflicts", "only_internationalized_conflicts"):
        word = "non-internationalized" if "non" in cst else "internationalized"
        cfg["title"] = f"{lead} in {word} intrastate conflicts"
        cfg["subtitle"] = (
            f"Deaths of combatants and civilians due to fighting, per 100,000 people. "
            f"Included are [{word} intrastate conflicts](#dod:intrastate-ucdp) that were ongoing that year."
            if per_capita
            else f"Included are deaths of combatants and civilians due to fighting in "
            f"[{word} intrastate conflicts](#dod:intrastate-ucdp) that were ongoing that year."
        )
        cfg["note"] = "'Best' estimates as identified by UCDP."
        cfg["selectedFacetStrategy"] = "entity"
    else:  # na / all_sub_types — CI stack
        has_map = (ctype, cst) in DEATHS_MAP_VIEWS
        cfg["title"] = f"{lead} in {name} based on where they occurred" if has_map else f"{lead} in {name}"
        cfg["subtitle"] = (
            f"Deaths of combatants and civilians due to fighting, per 100,000 people. "
            f"Included are {DOD[ctype]} that were ongoing that year."
            if per_capita
            else f"Included are deaths of combatants and civilians due to fighting in "
            f"{DOD[ctype]} that were ongoing that year."
        )
        cfg["note"] = "'Best' estimates as identified by UCDP."
        if has_map:
            cfg.update(MAP_CONFIG)
        else:
            cfg["selectedFacetStrategy"] = "entity"


def _count_text(cfg: dict[str, Any], measure: str, ctype: str, cst: str, sub_measure: str) -> None:
    rate = measure == "conflict_rate"
    is_new = sub_measure == "only_new_conflicts"
    name = CT_NAME[ctype]
    new_prefix = "new " if is_new else ""
    lead = "Rate of" if rate else "Number of"
    verb = "started that year" if is_new else "were ongoing that year"

    if cst == "by_sub_type":
        cfg["title"] = f"{lead} {new_prefix}{name}"
        cfg["subtitle"] = (
            f"The number of conflicts divided by the number of all states. This accounts for "
            f"the changing number of states over time. Included are {DOD_BY_SUBTYPE[ctype]} that {verb}."
            if rate
            else f"Included are {DOD_BY_SUBTYPE[ctype]} that {verb}."
        )
        cfg["note"] = (
            "Some conflicts affect several regions, and do not necessarily start at the same time "
            "across them. The sum across all regions can therefore be higher than the global number."
            if is_new
            else "Some conflicts affect several regions. The sum across all regions can therefore be "
            "higher than the global number."
        )
        cfg.update(STACKED_CONFIG)
    else:
        cfg["title"] = f"{lead} {new_prefix}{name}"
        if rate and ctype == "interstate_conflicts":
            cfg["subtitle"] = (
                "The number of conflicts divided by the number of all state-pairs. This accounts "
                f"for the changing number of states over time. Included are {DOD[ctype]} that {verb}."
            )
        elif rate:
            cfg["subtitle"] = (
                "The number of conflicts divided by the number of all states. This accounts for "
                f"the changing number of states over time. Included are {DOD[ctype]} that {verb}."
            )
        else:
            cfg["subtitle"] = f"Included are {DOD[ctype]} that {verb}."


def _participants_text(cfg: dict[str, Any], ctype: str, sub_measure: str) -> None:
    name = CT_NAME[ctype]
    country_level = sub_measure == "country_level_data"
    cfg["title"] = f"States involved in {name}" if country_level else f"Number of states involved in {name}"
    if ctype == "all_state_based_conflicts":
        dod_sing = (
            "[interstate](#dod:interstate-ucdp), [intrastate](#dod:intrastate-ucdp), "
            "or [extrasystemic](#dod:extrasystemic-ucdp) conflict"
        )
    else:
        dod_sing = DOD[ctype].replace("conflicts](", "conflict](")
    cfg["subtitle"] = (
        f"Included are states that were [primary participants](#dod:primary-participant-ucdp) "
        f"in at least one {dod_sing} that year."
    )
    if country_level:
        cfg.update(MAP_CONFIG)


def _locations_text(cfg: dict[str, Any], ctype: str, sub_measure: str) -> None:
    name = CT_NAME[ctype]
    country_level = sub_measure == "country_level_data"
    cfg["title"] = (
        f"Countries where {name} took place" if country_level else f"Number of countries where {name} took place"
    )
    cfg["subtitle"] = f"Included are {DOD[ctype]} that caused at least one death in the country that year."
    if country_level:
        cfg.update(MAP_CONFIG)


def _set_view_displays(view) -> None:
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
            # Labels are already on the indicators (set in _build_by_sub_type_views).
            return
        # CI triplet: best/low/high
        with_cs = (ctype, cst) in DEATHS_MAP_VIEWS_WITH_CS
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
            return  # already labeled
        if len(ys) == 1 and ctype in CT_SHORT:
            ys[0].display = {"name": CT_SHORT[ctype]}
    elif measure in ("conflict_locations", "conflict_participants"):
        country_level = d["sub_measure"] == "country_level_data"
        for ind in ys:
            if country_level:
                ind.display = {"colorScaleNumericBins": BOOL_MAP_BINS}
            elif measure == "conflict_locations":
                ind.display = {"colorScaleNumericBins": BOOL_MAP_BINS}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run() -> None:
    yaml_config = paths.load_collection_config()

    # The YAML carries the static views for the six non-UCDP data sources. We
    # load it as one sub-explorer and build UCDP as another, then merge them
    # with combine_collections.
    yaml_explorer = paths.create_collection(config=yaml_config, explorer=True)

    ucdp_explorer = build_ucdp_explorer(yaml_config)
    _attach_data_source_dim(ucdp_explorer, yaml_explorer)

    final = combine_collections(
        collections=[ucdp_explorer, yaml_explorer],
        catalog_path=yaml_explorer.catalog_path,
        config={"config": yaml_config.get("config", {})},
    )
    final.save(tolerate_extra_indicators=True)


def _attach_data_source_dim(ucdp_explorer, yaml_explorer) -> None:
    """Insert a `data_source` dim into ucdp_explorer that mirrors the YAML's
    definition, and tag each UCDP view with `data_source="ucdp"`. This is what
    `combine_collections` needs for the two sub-explorers to have matching dim
    structures."""
    from etl.collection.model.dimension import Dimension, DimensionChoice

    ds_yaml = next(d for d in yaml_explorer.dimensions if d.slug == "data_source")
    ucdp_ds_dim = Dimension(
        slug=ds_yaml.slug,
        name=ds_yaml.name,
        description=ds_yaml.description,
        presentation=deepcopy(ds_yaml.presentation),
        choices=[DimensionChoice(slug="ucdp", name="Uppsala Conflict Data Program")],
    )
    ucdp_explorer.dimensions = [ucdp_ds_dim, *ucdp_explorer.dimensions]
    for view in ucdp_explorer.views:
        view.dimensions = {"data_source": "ucdp", **view.dimensions}
