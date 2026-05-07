"""Build the Air Pollution explorer.

Single upstream table backs the explorer:
- CEDS air pollutants (`grapher/emissions/2025-02-12/ceds_air_pollutants`) — 198 columns
  covering 9 pollutants × 11 sectors × 2 metrics (absolute, per capita).

The grapher step already tags each column with `m.dimensions = {pollutant, sector}` using
display values ("BC" / "Agriculture"). This step rewrites those to URL-friendly slugs and
adds a `per_capita` slot derived from `original_short_name` (`emissions` vs
`emissions_per_capita`). `paths.create_collection(...)` then auto-expands 198 single-
indicator views.

`c.group_views(...)` adds:
- 2 "All pollutants" facet views (sector=all_sectors × per_capita ∈ {total, per_capita}).
- 18 "Breakdown by sector" facet views (one per pollutant × per_capita).

`c.drop_views(...)` removes the cross-product all_pollutants × <real sector> views (which
the legacy explorer doesn't surface) and the all_pollutants × breakdown_by_sector view
(emitted when the second `group_views` runs over views the first had already produced).

Single-indicator views inherit title/subtitle from the indicator's stored grapher_config;
multi-indicator (grouped) views set them explicitly via `group_views` view_config callables.
`c.set_global_config(...)` applies type/yAxisMin/hasMapTab/defaultView across all views,
using lambdas for the dimension-aware fields.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


# ---------------------------------------------------------------------------
# Slug mappings (upstream display values → URL-friendly slugs)
# ---------------------------------------------------------------------------

POLLUTANT_SLUG = {
    "NH₃": "nh3",
    "BC": "bc",
    "CO": "co",
    "CH₄": "ch4",
    "NOₓ": "nox",
    "N₂O": "n2o",
    "NMVOC": "nmvoc",
    "OC": "oc",
    "SO₂": "so2",
}

SECTOR_SLUG = {
    "All sectors": "all_sectors",
    "Agriculture": "agriculture",
    "Buildings": "buildings",
    "Domestic aviation": "domestic_aviation",
    "Energy": "energy",
    "Industry": "industry",
    "International aviation": "international_aviation",
    "International shipping": "international_shipping",
    "Solvents": "solvents",
    "Transport": "transport",
    "Waste": "waste",
}

POLLUTANTS = list(POLLUTANT_SLUG.values())
SECTORS_REAL = [v for v in SECTOR_SLUG.values() if v != "all_sectors"]


# ---------------------------------------------------------------------------
# FAUST templates
# ---------------------------------------------------------------------------


def _dim(view, key):
    """Safe accessor — robust to missing keys (e.g. the auto-added `collection__slug`)."""
    return view.dimensions.get(key)


def _is_per_capita(view) -> bool:
    return _dim(view, "per_capita") == "per_capita"


def _all_pollutants_title(view):
    return (
        "Per capita emissions of air pollutants from all sectors"
        if _is_per_capita(view)
        else "Emissions of air pollutants from all sectors"
    )


def _all_pollutants_subtitle(view):
    return (
        "Measured in kilograms and split by major pollutant."
        if _is_per_capita(view)
        else "Measured in tonnes and split by major pollutant."
    )


def _build_breakdown_title(pollutant_name: dict[str, str]):
    """Build the breakdown-by-sector title callable, closing over the YAML's slug→name map.

    The second `group_views` call also produces an `all_pollutants × breakdown_by_sector`
    view that's dropped immediately after, but its title callable runs first — so guard.
    """

    def _breakdown_title(view):
        name = pollutant_name.get(_dim(view, "pollutant"))
        if name is None:
            return None
        if _is_per_capita(view):
            return f"Per capita {name.lower()} emissions by sector"
        return f"{name} emissions by sector"

    return _breakdown_title


def _has_map_tab(view) -> bool:
    return _dim(view, "pollutant") != "all_pollutants" and _dim(view, "sector") != "breakdown_by_sector"


def _default_view(view) -> bool:
    return (
        _dim(view, "pollutant") == "all_pollutants"
        and _dim(view, "sector") == "all_sectors"
        and _dim(view, "per_capita") == "total"
    )


# ---------------------------------------------------------------------------
# Step
# ---------------------------------------------------------------------------


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("ceds_air_pollutants")
    tb = ds.read("ceds_air_pollutants", load_data=False)

    # Translate upstream dimension display values to URL-friendly slugs and add a
    # `per_capita` slot derived from each column's `original_short_name`.
    for col in tb.columns:
        if col in {"country", "year"}:
            continue
        d = tb[col].metadata.dimensions
        if d is None:
            continue
        is_per_capita = tb[col].metadata.original_short_name == "emissions_per_capita"
        tb[col].metadata.original_short_name = "emissions"
        tb[col].metadata.dimensions = {
            "pollutant": POLLUTANT_SLUG[d["pollutant"]],
            "sector": SECTOR_SLUG[d["sector"]],
            "per_capita": "per_capita" if is_per_capita else "total",
        }

    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names="emissions",
        dimensions={
            "pollutant": POLLUTANTS,
            "sector": list(SECTOR_SLUG.values()),
            "per_capita": ["total", "per_capita"],
        },
        short_name="air-pollution",
        explorer=True,
    )

    # Pull slug→display-name from the collection so titles stay in sync with the
    # dropdown labels users see in the explorer.
    breakdown_title = _build_breakdown_title(c.get_choice_names("pollutant"))

    c.group_views(
        groups=[
            # All-pollutants facet: collapse the 9 pollutants into one multi-indicator view per
            # (sector, per_capita). The cross-product against real sectors is dropped below.
            {
                "dimension": "pollutant",
                "choices": POLLUTANTS,
                "choice_new_slug": "all_pollutants",
                "view_config": {
                    "selectedFacetStrategy": "metric",
                    "facetYDomain": "independent",
                    "title": _all_pollutants_title,
                    "subtitle": _all_pollutants_subtitle,
                },
            },
            # Breakdown-by-sector facet: collapse the real sectors (excluding `all_sectors`) into
            # one multi-indicator view per (pollutant, per_capita).
            {
                "dimension": "sector",
                "choices": SECTORS_REAL,
                "choice_new_slug": "breakdown_by_sector",
                "view_config": {
                    "selectedFacetStrategy": "entity",
                    "facetYDomain": "independent",
                    "title": breakdown_title,
                },
            },
        ],
        drop_dimensions_if_single_choice=False,
    )

    # Drop unwanted views:
    # - all_pollutants × <real sector>: the legacy explorer only shows the all-pollutants
    #   facet for sector=all_sectors.
    # - all_pollutants × breakdown_by_sector: created when the second `group_views` runs
    #   over the views the first one had already produced.
    c.drop_views(
        [
            {"pollutant": "all_pollutants", "sector": "breakdown_by_sector"},
            *[{"pollutant": "all_pollutants", "sector": s} for s in SECTORS_REAL],
        ]
    )

    # Per-view config applied to every view. Lambdas branch on dimensions; single-
    # indicator title/subtitle remain unset and inherit from the indicator metadata.
    c.set_global_config(
        {
            "type": "LineChart",
            "yAxisMin": 0,
            "hasMapTab": _has_map_tab,
            "defaultView": _default_view,
        }
    )

    c.save(tolerate_extra_indicators=True)
