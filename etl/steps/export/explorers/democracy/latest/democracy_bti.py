"""BTI mini-explorer — programmatic alternative to `democracy.bti.config.yml`.

This file demonstrates building a mini-explorer in Python rather than YAML.
The trade-offs (vs. YAML in this directory):
- `BTI_VIEWS` below is a flat list of (metric, sub_metric, indicator, ...) tuples
  that read like a spreadsheet — easy to edit and grep.
- Single-y views are one-liners; multi-y views carry their stacked-area config inline.
- Dimension `choices` are auto-derived from the views (no manual list to maintain).
- Trade-off: per-view chart text (the rare overrides) is harder to localize than
  in YAML, since it lives inside Python data structures rather than a hand-edited .yml.

Used by `democracy.py` via `build_bti(paths, dim_meta, top_config)`.
"""

from etl.collection.explorer.core import Explorer

DATASET = "bertelsmann_transformation_index"

# (metric_slug, sub_metric_slug, indicator_short, view_config_overrides)
# Single-y if 3rd element is a str; multi-y if it's a list of (path, name, color) tuples.
BTI_VIEWS = [
    ("democratic_features", "main_index", "democracy_bti", None),
    ("democratic_features", "stateness_index", "state_bti", None),
    ("democratic_features", "political_participation_index", "political_participation_bti", None),
    ("democratic_features", "rule_of_law_index", "rule_of_law_bti", None),
    ("democratic_features", "stability_of_democratic_institutions_index", "stability_dem_inst_bti", None),
    ("democratic_features", "political_and_social_integration_index", "pol_soc_integr_bti", None),
    ("political_regime", "main_classification", "regime_bti", None),
    ("political_regime", "basic_state_functions_score", "state_basic_bti", None),
    ("political_regime", "free_and_fair_elections_score", "electfreefair_bti", None),
    ("political_regime", "effective_power_to_govern_score", "effective_power_bti", None),
    ("political_regime", "freedom_of_association_score", "freeassoc_bti", None),
    ("political_regime", "freedom_of_expression_score", "freeexpr_bti", None),
    ("political_regime", "separation_of_powers_score", "sep_power_bti", None),
    ("political_regime", "civil_rights_score", "civ_rights_bti", None),
    ("political_regime", "democratic_features", "democracy_bti", None),
    # Stacked-area: number of countries per BTI regime category.
    (
        "political_regime", "number_and_share_of_democracies",
        [
            ("num_countries#num_regime_bti__category_hard_line_autocracy", "Hard-line autocracies", "#8a148c"),
            ("num_countries#num_regime_bti__category_moderate_autocracy", "Moderate autocracies", "#d979d2"),
            ("num_countries#num_regime_bti__category_highly_defective_democracy", "Highly defective democracies", "#1ac6d9"),
            ("num_countries#num_regime_bti__category_defective_democracy", "Defective democracies", "#0487d9"),
            ("num_countries#num_regime_bti__category_consolidating_democracy", "Consolidating democracies", "#034c8c"),
        ],
        {
            "title": "Countries that are democracies and autocracies",
            "subtitle": "[Data](#dod:regimes-bertelsmann) by the [Bertelsmann Transformation Index](#dod:bertelsmann-transformation-index).",
            "note": "Data excludes long-term members of the OECD because they are considered consolidated democracies.",
            "stackMode": "relative",
            "yAxis": {"min": 0, "facetDomain": "independent"},
        },
    ),
    # Stacked-area: people living per BTI regime category.
    (
        "political_regime", "people_living_in_democracies",
        [
            ("num_people#pop_regime_bti__category__1", "No regime data", "#555555"),
            ("num_people#pop_regime_bti__category_hard_line_autocracy", "People living in hard-line autocracies", "#8a148c"),
            ("num_people#pop_regime_bti__category_moderate_autocracy", "People living in moderate autocracies", "#d979d2"),
            ("num_people#pop_regime_bti__category_highly_defective_democracy", "People living in highly defective democracies", "#1ac6d9"),
            ("num_people#pop_regime_bti__category_defective_democracy", "People living in defective democracies", "#0487d9"),
            ("num_people#pop_regime_bti__category_consolidating_democracy", "People living in consolidating democracies", "#034c8c"),
        ],
        {
            "title": "People living in democracies and autocracies",
            "subtitle": "[Data](#dod:regimes-bertelsmann) by the [Bertelsmann Transformation Index](#dod:bertelsmann-transformation-index).",
            "note": "Data excludes long-term members of the OECD because they are considered consolidated democracies.",
        },
    ),
]


def _bti_indicator(short: str) -> str:
    """Return the shortest-unambiguous catalog path for a BTI indicator."""
    # `bti` table is unique to the bti dataset → short form OK.
    # `num_countries`/`num_people` collide with polity/eiu → already include `bti/` prefix.
    return f"bti#{short}" if not short.startswith("num_") else f"bti/{short}"


def _build_view(metric, sub_metric, ind, cfg):
    view = {
        "dimensions": {"dataset": DATASET, "metric": metric, "sub_metric": sub_metric},
    }
    if isinstance(ind, str):
        view["indicators"] = {"y": [{"catalogPath": _bti_indicator(ind)}]}
    else:
        view["indicators"] = {
            "y": [
                {"catalogPath": _bti_indicator(path), "display": {"name": name, "color": color}}
                for path, name, color in ind
            ]
        }
    if cfg:
        view["config"] = cfg
    return view


def build_bti(paths, dim_meta: dict, top_config: dict) -> Explorer:
    """Construct the BTI mini-explorer programmatically."""
    views = [_build_view(*row) for row in BTI_VIEWS]

    # Collect the dim-choice subset used by these views, preserving the
    # canonical order from `dim_meta`.
    used: dict[str, set[str]] = {k: set() for k in dim_meta}
    for v in views:
        for k, val in v["dimensions"].items():
            used[k].add(val)

    dimensions = []
    for slug, meta in dim_meta.items():
        ordered = [c for c in meta["order"] if c in used[slug]]
        dimensions.append({
            "slug": slug,
            "name": meta["name"],
            "presentation": {"type": meta["type"]},
            "choices": [{"slug": c, "name": meta["choices"][c]} for c in ordered],
        })

    config = {
        "config": {
            "explorerTitle": top_config["explorerTitle"],
            "explorerSubtitle": top_config["explorerSubtitle"],
            "isPublished": True,
        },
        "dimensions": dimensions,
        "views": views,
    }

    return paths.create_collection(
        config=config,
        short_name="democracy_bti",
        explorer=True,
    )
