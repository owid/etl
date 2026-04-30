"""Multi-dim chart on deaths from natural disasters."""

from owid.catalog import Table

from etl.collection.model.view import Indicator
from etl.helpers import PathFinder

paths = PathFinder(__file__)

DISASTER_TYPES = [
    "drought",
    "earthquake",
    "volcanic_activity",
    "flood",
    "dry_mass_movement",
    "extreme_weather",
    "wildfire",
    "extreme_temperature",
]

# Same as DISASTER_TYPES; kept as a separate name to make group_views readable.
INDIVIDUAL_DISASTER_TYPES = list(DISASTER_TYPES)

# Disaster types stacked together for the "All disasters (excluding extreme temperatures)" view.
DISASTER_TYPES_EXCLUDING_EXTREME_TEMPERATURE = [t for t in INDIVIDUAL_DISASTER_TYPES if t != "extreme_temperature"]

# Aggregate type slugs whose views show a stacked breakdown plus a map tab pointing
# at the matching precomputed total indicator.
AGGREGATE_STACKED_TYPES = ("all_stacked", "all_disasters_excluding_extreme_temperature")

# Map aggregate stacked type → underlying garden disaster_type slug, used to build
# the catalog path of the precomputed total indicator shown on the map.
AGGREGATE_TO_TOTAL_TYPE = {
    "all_stacked": "all_disasters",
    "all_disasters_excluding_extreme_temperature": "all_disasters_excluding_extreme_temperature",
}

# Map metric -> garden indicator prefix.
INDICATOR_BY_METRIC = {
    "total_number": "total_dead",
    "per_capita": "total_dead_per_100k_people",
}

# Stable color per disaster type so the same disaster appears in the same colour
# across views (single-series, stacked-by-type, and excluding-extreme-temperatures).
DISASTER_COLORS = {
    "drought": "#bc8e5a",
    "earthquake": "#883039",
    "volcanic_activity": "#a2559c",
    "flood": "#286BBB",
    "dry_mass_movement": "#8b5a2b",
    "extreme_weather": "#5b9460",
    "wildfire": "#e94e1b",
    "extreme_temperature": "#d4a017",
}

# Human-readable phrase used in chart titles for each disaster-type choice.
DISASTER_PHRASES = {
    "all_disasters_excluding_extreme_temperature": "all disasters excluding extreme temperatures",
    "all_stacked": "disasters",
    "drought": "droughts",
    "earthquake": "earthquakes",
    "volcanic_activity": "volcanic activity",
    "flood": "floods",
    "dry_mass_movement": "dry mass movements",
    "extreme_weather": "storms",
    "wildfire": "wildfires",
    "extreme_temperature": "extreme temperatures",
}

COMMON_VIEW_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
    "chartTypes": ["StackedBar"],
    "hasMapTab": True,
    "tab": "chart",
    "yAxis": {"min": 0},
    "originUrl": "https://ourworldindata.org/natural-disasters",
    # Default chart timeline starts at 2000, when reporting coverage becomes more complete.
    "minTime": 2000,
    # Pin the map to a single year so it doesn't fall back to a "first year vs last year"
    # comparison view when minTime/maxTime defines a range on the chart.
    "map": {"time": "latest"},
}

STACKED_VIEW_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
    "chartTypes": ["StackedBar"],
    # Map tab is enabled and shows the all-disasters total via map.columnSlug;
    # see _add_total_indicator_for_map below for how the total indicator is
    # exposed to the map without being plotted on the stacked bar chart.
    "hasMapTab": True,
    "tab": "chart",
    "yAxis": {"min": 0},
    "originUrl": "https://ourworldindata.org/natural-disasters",
    "minTime": 2000,
    # Pin map to a single year so it doesn't fall back to a "first vs last year"
    # comparison view when minTime/maxTime defines a range on the chart.
    "map": {"time": "latest"},
    # Without this, a year drops out of the stack as soon as one disaster type has
    # no reported data — even if other types had a non-zero value.
    "missingDataStrategy": "show",
}

# Footnote shown on every chart, flagging the limited reporting coverage in earlier decades.
NOTE = (
    "Figures are based on reported data, and coverage is significantly limited before around 2000. "
    "Historical trends may partly reflect reporting improvements."
)


def _prepare_table(tb: Table, garden_timespan: str, mdim_timespan: str) -> Table:
    """Filter the wide grapher table to the columns we need and tag them with
    dimension metadata so the multidim explorer can pick the right indicator."""
    columns_to_keep = ["country", "year"]
    for metric, indicator in INDICATOR_BY_METRIC.items():
        for type_slug in DISASTER_TYPES:
            col = f"{indicator}_{type_slug}_{garden_timespan}"
            assert col in tb.columns, f"Column {col} not found in {tb.metadata.short_name}"
            tb[col].metadata.original_short_name = "value"
            tb[col].metadata.dimensions = {
                "type": type_slug,
                "timespan": mdim_timespan,
                "metric": metric,
            }
            columns_to_keep.append(col)

    tb = tb[columns_to_keep]
    tb.metadata.dimensions = [
        {"name": "country", "slug": "country"},
        {"name": "year", "slug": "year"},
        {"name": "Disaster type", "slug": "type"},
        {"name": "Timespan", "slug": "timespan"},
        {"name": "Metric", "slug": "metric"},
    ]
    return tb


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("natural_disasters")
    tb_yearly = _prepare_table(
        ds.read("natural_disasters_yearly", load_data=False),
        garden_timespan="yearly",
        mdim_timespan="annual",
    )
    tb_decadal = _prepare_table(
        ds.read("natural_disasters_decadal", load_data=False),
        garden_timespan="decadal",
        mdim_timespan="decadal",
    )

    c = paths.create_collection(
        config=config,
        tb=[tb_yearly, tb_decadal],
        indicator_names="value",
        common_view_config=COMMON_VIEW_CONFIG,
    )

    c.group_views(
        groups=[
            {
                "dimension": "type",
                "choice_new_slug": "all_stacked",
                "choices": INDIVIDUAL_DISASTER_TYPES,
                "view_config": STACKED_VIEW_CONFIG,
            },
            {
                "dimension": "type",
                "choice_new_slug": "all_disasters_excluding_extreme_temperature",
                "choices": DISASTER_TYPES_EXCLUDING_EXTREME_TEMPERATURE,
                "view_config": STACKED_VIEW_CONFIG,
            },
        ],
    )

    c.set_global_config(
        {
            "title": _title,
            "subtitle": _subtitle,
            "note": NOTE,
        }
    )

    # Expose the all-disasters total on the map tab of the stacked-by-type views.
    _add_total_indicator_for_map(c)

    # Pin a stable colour to each y-indicator based on its disaster type.
    _apply_disaster_colors(c)

    c.save()


def _apply_disaster_colors(c) -> None:
    """Set display.color on each y-indicator so the same disaster type renders in
    the same colour across all views (single-series, stacked, etc.)."""
    for view in c.views:
        if view.indicators.y is None:
            continue
        for indicator in view.indicators.y:
            disaster_type = _disaster_type_from_path(indicator.catalogPath)
            if disaster_type is None:
                continue
            color = DISASTER_COLORS.get(disaster_type)
            if color is None:
                continue
            display = indicator.display or {}
            display.setdefault("color", color)
            indicator.display = display


def _disaster_type_from_path(catalog_path: str) -> str | None:
    """Extract the disaster type slug from an indicator's catalog path. The column
    name follows the pattern ``<prefix>_<type>_<garden_timespan>``."""
    column = catalog_path.rsplit("#", 1)[-1]
    padded = f"_{column}_"
    # Sort longest first so e.g. ``extreme_temperature`` is matched before any
    # shorter overlapping slug ever introduced.
    for type_slug in sorted(DISASTER_COLORS, key=len, reverse=True):
        if f"_{type_slug}_" in padded:
            return type_slug
    return None


def _add_total_indicator_for_map(c) -> None:
    """Attach the all-disasters total indicator as a non-rendered "color" dimension
    on each ``all_stacked`` view, and point ``map.columnSlug`` at it.

    Why ``color``: Grapher requires ``map.columnSlug`` to reference a column that's
    in the chart's ``dimensions`` array. For a ``StackedBar`` only ``y`` is a supported
    rendering slot, so a dimension with ``property: color`` is silently filtered out
    of plotting (validDimensions in GrapherState.tsx) but still satisfies the
    columnSlug check (which uses the raw ``dimensions`` array). The data column
    therefore loads, the bar chart still shows the 8 type breakdown, and the map
    renders the total. A future Grapher addition of a proper ``map`` dimension
    property would make this trick unnecessary.

    The catalog path is given in short form (``table#column``); the framework's
    save-time ``expand_paths`` resolves it against the DAG dependencies, so the
    dataset version is never hardcoded here.
    """
    for view in c.views:
        type_slug = view.dimensions.get("type")
        if type_slug not in AGGREGATE_STACKED_TYPES:
            continue
        metric = view.dimensions["metric"]
        garden_timespan = "yearly" if view.dimensions["timespan"] == "annual" else "decadal"
        total_type = AGGREGATE_TO_TOTAL_TYPE[type_slug]
        column = f"{INDICATOR_BY_METRIC[metric]}_{total_type}_{garden_timespan}"
        catalog_path = f"natural_disasters_{garden_timespan}#{column}"

        view.indicators.color = Indicator(catalogPath=catalog_path)

        view.config = view.config or {}
        view.config["map"] = {**(view.config.get("map") or {}), "columnSlug": catalog_path}


def _title(view) -> str:
    type_phrase = DISASTER_PHRASES[view.dimensions["type"]]
    if view.dimensions["metric"] == "per_capita":
        body = f"Annual death rate from {type_phrase}"
    else:
        body = f"Annual deaths from {type_phrase}"
    if view.dimensions["timespan"] == "decadal":
        return f"{body} (10-year average)"
    return body


def _subtitle(view) -> str:
    parts = []
    if view.dimensions["metric"] == "per_capita":
        parts.append("Death rates are measured as the number of deaths per 100,000 people.")
    if view.dimensions["timespan"] == "decadal":
        parts.append("Decadal figures are measured as the annual average over the subsequent ten-year period.")
    parts.append(
        "Disasters include all geophysical, meteorological, and climate events such as earthquakes, "
        "volcanic activity, drought, wildfires, storms, and flooding."
    )
    return " ".join(parts)
