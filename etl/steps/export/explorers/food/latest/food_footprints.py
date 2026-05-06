"""Build the Environmental Impacts of Food explorer.

Two upstream tables back the explorer:
- Poore & Nemecek (2018) "environmental_impacts_of_food" (15 cols) — the **Commodity** half
  (5 impacts × 3 units: per_kg, per_100g_protein, per_1000kcal)
- Clark et al. (2022) "environmental_impacts_of_food_clark_et_al_2022" (24 cols, 20 used)
  — the **Specific food products** half (5 impacts × 4 units, including per_100g_fat;
  biodiversity columns ignored)

Each column is tagged with `m.dimensions = {view_type, impact, unit, by_stage}` and a unifying
`m.original_short_name = "footprint"`, so `paths.create_collection(tb=[poore, clark], ...)`
auto-expands 35 single-indicator views. `c.group_views(...)` then adds 4 facet views
(`impact=all_impacts`, one per unit) and 5 compare-units views (`unit=compare_units`, one per
impact), all on the Specific-food-products side. `c.drop_views(...)` removes the equivalent
auto-grouped views on the Commodity side, since the legacy explorer doesn't surface them there.

Per-view chart text (title, subtitle, type, sourceDesc) is templated by `c.set_global_config`
lambdas — no per-view text in YAML. Only one view is hand-listed in the YAML: the lifecycle-
stage view (`by_stage=stages`, an 8-indicator stacked bar from a third dataset).
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


# ---------------------------------------------------------------------------
# Column → dimension tagging
# ---------------------------------------------------------------------------

POORE_IMPACT = {
    "ghg_emissions_": "carbon",
    "land_use_": "land_use",
    "freshwater_withdrawals_": "water",
    "scarcity_weighted_water_use_": "water_scarcity",
    "eutrophying_emissions_": "eutrophication",
}
POORE_UNIT = {
    "_per_kilogram__poore__and__nemecek__2018": "per_kg",
    "_per_100g_protein__poore__and__nemecek__2018": "per_100g_protein",
    "_per_1000kcal__poore__and__nemecek__2018": "per_1000kcal",
}

# Order matters: water_scarcity must be checked before water (longer prefix wins).
CLARK_IMPACT = {
    "ghg_": "carbon",
    "land_use_": "land_use",
    "water_scarcity_": "water_scarcity",
    "water_": "water",
    "eutrophication_": "eutrophication",
}
CLARK_UNIT = {
    "_kg": "per_kg",
    "_100gprotein": "per_100g_protein",
    "_1000kcal": "per_1000kcal",
    "_100gfat": "per_100g_fat",
}


def _classify(col: str, impact_map: dict, unit_map: dict) -> tuple[str | None, str | None]:
    impact = next((v for k, v in impact_map.items() if col.startswith(k)), None)
    unit = next((v for k, v in unit_map.items() if col.endswith(k)), None)
    return impact, unit


def tag_dimensions(tb, view_type: str, impact_map: dict, unit_map: dict):
    """Set `m.dimensions` and `m.original_short_name` on every footprint column.

    Untagged columns (e.g. Clark's biodiversity_*, which the explorer doesn't show) are left
    alone and ignored by `create_collection`'s expander.
    """
    # Declare dimension slots on the table-level metadata so the expander can find them.
    if isinstance(tb.metadata.dimensions, list):
        existing = {d.get("slug") for d in tb.metadata.dimensions}
        for slug, name in [
            ("view_type", "View type"),
            ("impact", "Impact"),
            ("unit", "Unit"),
            ("by_stage", "By stage"),
        ]:
            if slug not in existing:
                tb.metadata.dimensions.append({"slug": slug, "name": name})

    for col in tb.columns:
        if col in {"country", "year"}:
            continue
        impact, unit = _classify(col, impact_map, unit_map)
        if impact is None or unit is None:
            continue
        tb[col].metadata.original_short_name = "footprint"
        tb[col].metadata.dimensions = {
            "view_type": view_type,
            "impact": impact,
            "unit": unit,
            "by_stage": "combined",
        }
    return tb


# ---------------------------------------------------------------------------
# FAUST templates
# ---------------------------------------------------------------------------

IMPACT_TITLE = {
    "carbon": "Greenhouse gas emissions",
    "land_use": "Land use",
    "water": "Freshwater withdrawals",
    "water_scarcity": "Scarcity-weighted water use",
    "eutrophication": "Eutrophication",
}

IMPACT_SUBTITLE = {
    "carbon": (
        "[Greenhouse gas emissions](#dod:ghgemissions) are measured in kilograms of "
        "[carbon dioxide-equivalents](#dod:carbondioxideequivalents). Non-CO₂ gases are "
        "weighted by the warming they cause over 100 years."
    ),
    "land_use": "Land use is measured in square meters (m²) per year.",
    "water": "Freshwater withdrawals are measured in liters.",
    "water_scarcity": (
        "Scarcity-weighted water use represents freshwater use weighted by local water scarcity. Measured in liters."
    ),
    "eutrophication": (
        "Eutrophying emissions represent runoff of excess nutrients into ecosystems. "
        "Measured in grams of phosphate equivalents (PO₄eq)."
    ),
}

UNIT_PHRASE = {
    "per_kg": "per kilogram of food",
    "per_100g_protein": "per 100 grams of protein",
    "per_1000kcal": "per 1000 kilocalories",
    "per_100g_fat": "per 100 grams of fat",
}

POORE_CITATION = (
    "Joseph Poore and Thomas Nemecek (2018). "
    "Reducing food's environmental impacts through producers and consumers. Science."
)
CLARK_CITATION = "Michael Clark et al (2022). Estimating the environmental impacts of 57,000 food products. PNAS."


def _dim(view, key):
    """Safe accessor — `view.d.<key>` raises AttributeError on missing keys, which can
    happen for views emitted by `combine_collections` (e.g. the auto-added `collection__slug`
    dim) or for any future grouped view that drops a dimension. Reading from
    `view.dimensions` directly with `.get()` keeps the lambdas robust."""
    return view.dimensions.get(key)


def _title(view) -> str:
    by_stage = _dim(view, "by_stage")
    impact = _dim(view, "impact")
    unit = _dim(view, "unit")
    if by_stage == "stages":
        return "Food: greenhouse gas emissions across the supply chain"
    if impact == "all_impacts":
        return f"Environmental impacts of food {UNIT_PHRASE[unit]}"
    if unit == "compare_units":
        return f"{IMPACT_TITLE[impact]} of food products"
    return f"{IMPACT_TITLE[impact]} {UNIT_PHRASE[unit]}"


def _subtitle(view) -> str | None:
    by_stage = _dim(view, "by_stage")
    impact = _dim(view, "impact")
    if by_stage == "stages":
        return (
            "[Greenhouse gas emissions](#dod:ghgemissions) are measured in kilograms of "
            "[carbon dioxide-equivalents (CO₂eq)](#dod:carbondioxideequivalents) per kilogram of food."
        )
    if impact == "all_impacts":
        return None
    return IMPACT_SUBTITLE.get(impact)


def _chart_type(view) -> str:
    return "StackedDiscreteBar" if _dim(view, "by_stage") == "stages" else "DiscreteBar"


def _source_desc(view) -> str:
    if _dim(view, "by_stage") == "stages":
        return POORE_CITATION
    return CLARK_CITATION if _dim(view, "view_type") == "specific_food_product" else POORE_CITATION


# ---------------------------------------------------------------------------
# Step
# ---------------------------------------------------------------------------

IMPACTS = ["carbon", "land_use", "water", "water_scarcity", "eutrophication"]
ALL_UNITS = ["per_kg", "per_100g_protein", "per_1000kcal", "per_100g_fat"]
POORE_UNITS = ["per_kg", "per_100g_protein", "per_1000kcal"]  # no per_100g_fat


def run() -> None:
    config = paths.load_collection_config()

    ds_poore = paths.load_dataset("environmental_impacts_of_food__poore__and__nemecek__2018")
    tb_poore = ds_poore.read("environmental_impacts_of_food__poore__and__nemecek__2018", load_data=False)
    tag_dimensions(tb_poore, "commodity", POORE_IMPACT, POORE_UNIT)

    ds_clark = paths.load_dataset("environmental_impacts_of_food_clark_et_al_2022")
    tb_clark = ds_clark.read("environmental_impacts_of_food_clark_et_al_2022", load_data=False)
    tag_dimensions(tb_clark, "specific_food_product", CLARK_IMPACT, CLARK_UNIT)

    # Per-table dimensions: each table only carries values for its own view_type and the
    # units it actually has columns for (Poore has no per_100g_fat).
    c = paths.create_collection(
        config=config,
        tb=[tb_poore, tb_clark],
        indicator_names="footprint",
        dimensions=[
            {
                "view_type": ["commodity"],
                "impact": IMPACTS,
                "unit": POORE_UNITS,
                "by_stage": ["combined"],
            },
            {
                "view_type": ["specific_food_product"],
                "impact": IMPACTS,
                "unit": ALL_UNITS,
                "by_stage": ["combined"],
            },
        ],
        short_name="food-footprints",
        explorer=True,
    )

    # Group views:
    # - All-impacts facet: collapse the 5 impacts into one multi-indicator view per unit.
    # - Compare-units facet: collapse the 4 units into one multi-indicator view per impact.
    c.group_views(
        groups=[
            {
                "dimension": "impact",
                "choices": IMPACTS,
                "choice_new_slug": "all_impacts",
                "view_config": {
                    "selectedFacetStrategy": "metric",
                    "facetYDomain": "independent",
                },
            },
            {
                "dimension": "unit",
                "choices": ALL_UNITS,
                "choice_new_slug": "compare_units",
                "view_config": {
                    "selectedFacetStrategy": "metric",
                    "facetYDomain": "independent",
                },
            },
        ],
        drop_dimensions_if_single_choice=False,
    )

    # c.group_views(
    #     groups=[
    #         {
    #             "dimension": "unit",
    #             "choices": ALL_UNITS,
    #             "choice_new_slug": "compare_units",
    #             "view_config": {
    #                 "selectedFacetStrategy": "metric",
    #                 "facetYDomain": "independent",
    #             },
    #         }
    #     ],
    #     drop_dimensions_if_single_choice=False,
    # )

    # Drop unwanted views:
    # - commodity × all_impacts / compare_units: the legacy explorer only surfaces these
    #   for specific food products.
    # - all_impacts × compare_units: created when the second group_views runs over the
    #   views the first one had already produced. The legacy explorer doesn't show 5
    #   impacts × 4 units in one chart.
    c.drop_views(
        [
            {"view_type": "commodity", "impact": "all_impacts"},
            {"view_type": "commodity", "unit": "compare_units"},
            {"impact": "all_impacts", "unit": "compare_units"},
        ]
    )

    # Templated chart text via lambdas. `set_global_config` sets these on every view (auto-
    # expanded + grouped + the YAML stage view); the lambdas branch on `view.d` to handle
    # each shape.
    c.set_global_config(
        {
            "type": _chart_type,
            "baseColorScheme": "owid-distinct",
            "title": _title,
            "subtitle": _subtitle,
            "sourceDesc": _source_desc,
        }
    )

    c.save(tolerate_extra_indicators=True)
