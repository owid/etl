"""Build the Minerals explorer.

Single upstream table — `grapher/minerals/.../minerals` — with one column per
(metric, mineral, type, unit) combination. Each column's metadata title encodes
those four pieces as `metric|commodity|sub_commodity|unit`. This step parses
that title, lifts the four pieces onto `m.dimensions`, and lets
`paths.create_collection(tb=tb, ...)` auto-expand one view per column.

The explorer is the YAML-driven sibling of `export://multidim/minerals/latest/minerals`
(see `etl/steps/export/multidim/minerals/latest/minerals.py`); the construction
logic is intentionally near-identical, with two differences:
- Top-level `config:` carries the legacy explorer settings (explorerTitle,
  explorerSubtitle, selection, …) instead of the multidim's title block.
- `measure` is presented as a checkbox in the explorer (`Share of global`)
  rather than a radio.

Sparse-data handling: a small set of columns either has very few data points
(<5 years range — show as a bar by setting `minTime` to the last year) or covers
too few entities to make the map tab useful (`COLUMNS_WITHOUT_MAP_TAB`). Both
are applied per-view via `set_global_config` lambdas. The `Unit value` metric
also drops the map tab (single global price, not country-level).
"""

from owid.catalog.utils import underscore
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

paths = PathFinder(__file__)

# Prefix used by the garden step to mark "share of global" indicators.
SHARE_OF_GLOBAL_PREFIX = "share of global "

# Columns for which the map tab is hidden (sparse geographic data not worth a map).
COLUMNS_WITHOUT_MAP_TAB = {
    "production_bismuth_mine_tonnes",
    "production_boron_mine_tonnes",
    "production_diamond_mine_and_synthetic__industrial_tonnes",
    "production_gallium_refinery_tonnes",
    "production_mica_mine__sheet_tonnes",
    "production_sand_and_gravel_mine__construction_tonnes",
    "share_of_global_production_bismuth_mine_tonnes",
    "share_of_global_production_boron_mine_tonnes",
    "share_of_global_production_diamond_mine_and_synthetic__industrial_tonnes",
    "share_of_global_production_gallium_refinery_tonnes",
    "share_of_global_production_mica_mine__sheet_tonnes",
    "share_of_global_production_sand_and_gravel_mine__construction_tonnes",
}


def _column_from_view(view) -> str | None:
    """Return the indicator's bare column name from a view's catalog path."""
    if view.indicators.y:
        path = view.indicators.y[0].catalogPath
        if "#" in path:
            return path.split("#")[-1]
    return None


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("minerals")
    tb = ds.read("minerals")

    # Tag each column with its (mineral, metric, type, measure) dimensions.
    sparse_min_year: dict[str, int] = {}
    mineral_names: dict[str, str] = {}
    type_names: dict[str, str] = {}

    for column in tb.drop(columns=["country", "year"]).columns:
        years = tb["year"][tb[column].notnull()]
        if len(years) == 0:
            continue

        metric, commodity, sub_commodity, unit = tb[column].metadata.title.split("|")

        if metric.startswith(SHARE_OF_GLOBAL_PREFIX):
            metric = metric.replace(SHARE_OF_GLOBAL_PREFIX, "")
            measure = "share_of_global"
        else:
            measure = "absolute"
        metric = metric.replace("_", " ").lower()

        mineral_slug = underscore(commodity)
        type_slug = underscore(sub_commodity)
        mineral_names[mineral_slug] = commodity
        type_names[type_slug] = sub_commodity

        tb[column].m.dimensions = {
            "mineral": mineral_slug,
            "metric": underscore(metric),
            "type": type_slug,
            "measure": measure,
        }
        tb[column].m.original_short_name = "value"

        if (years.max() - years.min()) < 5:
            sparse_min_year[column] = int(years.max())

    missing = COLUMNS_WITHOUT_MAP_TAB - set(tb.columns)
    assert not missing, f"COLUMNS_WITHOUT_MAP_TAB references unknown columns: {missing}"

    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["value"],
        dimensions=["mineral", "metric", "type", "measure"],
        common_view_config={"yAxis": {"min": 0}},
        choice_renames={
            "mineral": mineral_names,
            "type": type_names,
        },
        short_name="minerals",
        explorer=True,
    )

    def _has_map_tab(view) -> bool:
        col = _column_from_view(view)
        if col in COLUMNS_WITHOUT_MAP_TAB:
            return False
        if view.dimensions.get("metric") == "unit_value":
            return False
        return True

    def _min_time(view):
        col = _column_from_view(view)
        return sparse_min_year.get(col)

    def _default_view(view) -> bool:
        d = view.dimensions
        return (
            d.get("mineral") == "copper"
            and d.get("metric") == "production"
            and d.get("type") == "mine"
            and d.get("measure") == "absolute"
        )

    c.set_global_config(
        {
            "hasMapTab": _has_map_tab,
            "minTime": _min_time,
            "defaultView": _default_view,
        }
    )

    c.save(tolerate_extra_indicators=True)
