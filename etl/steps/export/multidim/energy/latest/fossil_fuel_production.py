"""Multidim for fossil fuel production (fuel x metric)."""

from copy import deepcopy

from etl.collection.model.view import View, ViewIndicators
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# OWID and Energy-Institute aggregate entities, excluded when sizing the map color scale (grapher
# leaves them off the map, and their values would otherwise push the top bin above every country).
# Entities with an "(EI)" suffix are the Statistical Review's own regions and are excluded by suffix.
AGGREGATE_ENTITIES = {
    "World",
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "South America",
    "Oceania",
    "European Union (27)",
    "High-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "Low-income countries",
}

# Map each grapher column to its (fuel, metric) dimensions.
COLUMN_DIMENSIONS = {
    **{f"{fuel}_production_twh": {"fuel": fuel, "metric": "production"} for fuel in ["coal", "oil", "gas"]},
    **{f"{fuel}_production_per_capita_kwh": {"fuel": fuel, "metric": "per_capita"} for fuel in ["coal", "oil", "gas"]},
    **{
        f"{fuel}_reserves_to_production_ratio": {"fuel": fuel, "metric": "reserves_ratio"}
        for fuel in ["coal", "oil", "gas"]
    },
    # Total fossil fuel production (the aggregate that the "by fuel" breakdown decomposes).
    "total_production_twh": {"fuel": "total", "metric": "production"},
    "total_production_per_capita_kwh": {"fuel": "total", "metric": "per_capita"},
}


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("fossil_fuel_production")
    tb = ds.read("fossil_fuel_production", reset_index=False)

    #
    # Process data.
    #
    # Keep only the columns that map to a (fuel, metric) view, and set their dimensions.
    tb = tb[list(COLUMN_DIMENSIONS)]
    # Reference magnitude per (fuel, metric) for sizing the map's log bins: the 99th percentile
    # across countries only (aggregates excluded, single outliers ignored). See set_view_titles.
    country_level = tb.index.get_level_values("country")
    is_country = ~(country_level.isin(AGGREGATE_ENTITIES) | country_level.str.contains("(EI)", regex=False))
    tb_countries = tb[is_country]
    dims_max = {
        (dims["fuel"], dims["metric"]): float(tb_countries[column].astype("float64").quantile(0.99))
        for column, dims in COLUMN_DIMENSIONS.items()
    }
    for column, dims in COLUMN_DIMENSIONS.items():
        tb[column].m.dimensions = dims
        tb[column].m.original_short_name = "fossil_fuel_production"

    common_view_config = {
        "hasMapTab": True,
        "tab": "map",
        # Line + bar tabs (grapher's default), so single-fuel views keep the bar tab the
        # original charts had.
        "chartTypes": ["LineChart", "DiscreteBar"],
        "yAxis": {"min": 0},
    }

    c = paths.create_collection(
        config=paths.load_collection_config(),
        tb=tb,
        indicator_names=["fossil_fuel_production"],
        dimensions=["fuel", "metric"],
        common_view_config=common_view_config,
    )

    # Set an explicit title on every single-fuel view, so grapher does not fall back to the
    # indicator display name.
    set_view_titles(c, dims_max)

    # Add "by fuel" stacked views that decompose total fossil fuel production into coal, oil and gas.
    add_decomposition_views(c)

    #
    # Save outputs.
    #
    c.save()


# Single-fuel views (coal/oil/gas) plus the "total" aggregate get an explicit title/subtitle/map.
FUEL_TITLE_NAMES = {"total": "fossil fuels", "coal": "coal", "oil": "oil", "gas": "gas"}

# Unit phrase per metric, used for both single-fuel and stacked views.
METRIC_UNIT_PHRASE = {
    "production": "Measured in [terawatt-hours](#dod:watt-hours).",
    "per_capita": "Measured in [kilowatt-hours](#dod:watt-hours) per person.",
    "reserves_ratio": "Number of years of production left at current reserves and production rates.",
}


def _view_title(fuel: str, metric: str) -> str:
    if fuel == "total":
        return {"production": "Fossil fuel production", "per_capita": "Fossil fuel production per person"}[metric]
    name = FUEL_TITLE_NAMES[fuel]
    return {
        "production": f"{name.capitalize()} production",
        "per_capita": f"{name.capitalize()} production per person",
        "reserves_ratio": f"Reserves-to-production ratio for {name}",
    }[metric]


# Composition note appended to the total (all fossil fuels) views, so the subtitle spells out what
# "fossil fuels" covers.
FOSSIL_FUELS_NOTE = "Fossil fuels include coal, oil, and gas."


def _view_subtitle(fuel: str, metric: str) -> str:
    unit = METRIC_UNIT_PHRASE[metric]
    return f"{unit} {FOSSIL_FUELS_NOTE}" if fuel == "total" else unit


# Aggregates that can be decomposed "by fuel" (only the total), mapped to their constituent fuels
# (listed top-to-bottom for the stacked chart, so coal sits at the bottom).
AGGREGATE_DECOMPOSITION = {"total": ["gas", "oil", "coal"]}
# Base metric each decomposition is built from -> the metric slug it becomes.
_DECOMPOSITION_METRICS = {"production": "by_fuel", "per_capita": "by_fuel_per_capita"}


def _decomposition_title(base_metric: str) -> str:
    return (
        "Fossil fuel production per person, by fuel"
        if base_metric == "per_capita"
        else "Fossil fuel production by fuel"
    )


def add_decomposition_views(c) -> None:
    """Add stacked "by fuel" views that break total fossil fuel production into coal, oil and gas."""
    base_config = {"chartTypes": ["StackedArea"], "tab": "chart", "hasMapTab": False, "hideRelativeToggle": False}
    single_views = {(v.dimensions.get("fuel"), v.dimensions.get("metric")): v for v in c.views}
    for source, constituents in AGGREGATE_DECOMPOSITION.items():
        for base_metric, new_metric in _DECOMPOSITION_METRICS.items():
            indicators = []
            for constituent in constituents:
                view = single_views.get((constituent, base_metric))
                if view is not None and view.indicators.y:
                    indicators.extend(deepcopy(view.indicators.y))
            if not indicators:
                continue
            config = {
                **base_config,
                "title": _decomposition_title(base_metric),
                "subtitle": METRIC_UNIT_PHRASE[base_metric],
            }
            new_view = View(
                dimensions={"fuel": source, "metric": new_metric},
                indicators=ViewIndicators(y=indicators),
                config=config,
            )
            new_view.mark_as_grouped()
            c.views.append(new_view)


# Map color scheme per (fuel, metric), copied from the original production charts each view replaces
# (fetched from their chart configs) so the new maps look like the ones users already know. Views with
# no pre-existing chart fall back to a per-fuel family below.
ORIGINAL_MAP_SCHEMES = {
    ("coal", "per_capita"): {"baseColorScheme": "YlOrBr"},
    ("coal", "production"): {"baseColorScheme": "OrRd"},
    ("gas", "per_capita"): {"baseColorScheme": "BuPu"},
    ("gas", "production"): {"baseColorScheme": "Purples"},
    ("oil", "per_capita"): {"baseColorScheme": "YlOrRd"},
    ("oil", "production"): {"baseColorScheme": "YlOrRd"},
}

# Per-fuel fallback for views without a pre-existing chart (e.g. total, and reserves-to-production).
FUEL_FALLBACK_SCHEME = {"total": "YlOrBr", "coal": "OrRd", "oil": "YlOrRd", "gas": "Purples"}

# All fossil-fuel metrics are unbounded positive magnitudes, so every map uses log-spaced bins with
# an open-ended top bracket (">X").
LOG_METRICS = {"production", "per_capita", "reserves_ratio"}


def _log_thresholds(vmax: float | None, max_bins: int = 7) -> list[float] | None:
    """1-2-5 log-spaced bin edges from 0 up to the largest ladder value strictly below vmax.

    Keeping the top edge below the data max makes grapher render an open-ended top bin
    (isOpenRight = last edge < data max), matching the original charts' brackets.
    """
    if vmax is None or not (vmax > 0):
        return None
    ladder = [m * 10**p for p in range(0, 13) for m in (1, 2, 5)]
    below = [v for v in ladder if v < vmax]
    if len(below) < 2:
        return None
    return [0] + below[-(max_bins - 1) :]


def _map_config(fuel: str, metric: str, vmax: float | None = None) -> dict:
    scheme = ORIGINAL_MAP_SCHEMES.get((fuel, metric)) or {"baseColorScheme": FUEL_FALLBACK_SCHEME[fuel]}
    color_scale = dict(scheme)
    if metric in LOG_METRICS:
        edges = _log_thresholds(vmax)
        if edges:
            color_scale["binningStrategy"] = "manual"
            # Trailing sentinel (smaller than the top edge) forces grapher to render an open-ended
            # top bracket (">X"), independent of where the top edge sits relative to the data max.
            color_scale["customNumericValues"] = edges + [1]
    return {"colorScale": color_scale, "timeTolerance": 3}


def set_view_titles(c, dims_max: dict) -> None:
    for v in c.views:
        fuel = v.dimensions["fuel"]
        if fuel not in FUEL_TITLE_NAMES:
            # Grouped/stacked view already has a title from group_views.
            continue
        metric = v.dimensions["metric"]
        config = dict(v.config or {})
        config["title"] = _view_title(fuel, metric)
        config["subtitle"] = _view_subtitle(fuel, metric)
        config["map"] = _map_config(fuel, metric, dims_max.get((fuel, metric)))
        v.config = config
