"""Multidim for the electricity mix (source x metric, plus total-only metrics)."""

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

# Map each source dimension slug to its column prefix in the electricity_mix grapher table.
SOURCE_COLUMN_PREFIX = {
    "total": "total",
    "coal": "coal",
    "oil": "oil",
    "gas": "gas",
    "fossil": "fossil",
    "nuclear": "nuclear",
    "hydro": "hydro",
    "solar": "solar",
    "wind": "wind",
    "solar_and_wind": "solar_and_wind",
    "renewables": "renewable",
    "other_renewables": "other_renewables_including_bioenergy",
    "bioenergy": "bioenergy",
    "low_carbon": "low_carbon",
}

# Total-only metrics: (metric slug, column).
TOTAL_ONLY_METRICS = {
    "demand": "total_demand__twh",
    "demand_per_capita": "per_capita_total_demand__kwh",
    "net_imports": "total_net_imports__twh",
    "imports_share": "net_imports_share_of_demand__pct",
    "carbon_intensity": "co2_intensity__gco2_kwh",
}


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("electricity_mix")
    tb = ds.read("electricity_mix", reset_index=False)

    #
    # Process data.
    #
    column_dimensions = {}
    # Source x metric grid (generation, per capita, share of generation).
    for source_slug, prefix in SOURCE_COLUMN_PREFIX.items():
        candidates = {
            "generation": f"{prefix}_generation__twh",
            "per_capita": f"per_capita_{prefix}_generation__kwh",
            "share_of_generation": f"{prefix}_share_of_electricity__pct",
        }
        for metric_slug, column in candidates.items():
            if column in tb.columns:
                column_dimensions[column] = {"source": source_slug, "metric": metric_slug}

    # Total-only metrics (demand, imports, carbon intensity).
    for metric_slug, column in TOTAL_ONLY_METRICS.items():
        if column in tb.columns:
            column_dimensions[column] = {"source": "total", "metric": metric_slug}

    tb = tb[list(column_dimensions)]
    # Reference magnitude per (source, metric) for sizing the map's log bins: the 99th percentile
    # across countries only (aggregates excluded, single outliers ignored). See set_view_titles.
    country_level = tb.index.get_level_values("country")
    is_country = ~(country_level.isin(AGGREGATE_ENTITIES) | country_level.str.contains("(EI)", regex=False))
    tb_countries = tb[is_country]
    dims_max = {
        (dims["source"], dims["metric"]): float(tb_countries[column].astype("float64").quantile(0.99))
        for column, dims in column_dimensions.items()
    }
    for column, dims in column_dimensions.items():
        tb[column].m.dimensions = dims
        tb[column].m.original_short_name = "electricity"

    common_view_config = {
        "hasMapTab": True,
        "tab": "map",
        # Line + bar tabs (grapher's default), so single-source views keep the bar tab the
        # original charts had.
        "chartTypes": ["LineChart", "DiscreteBar"],
    }

    c = paths.create_collection(
        config=paths.load_collection_config(),
        tb=tb,
        indicator_names=["electricity"],
        dimensions=["source", "metric"],
        common_view_config=common_view_config,
    )

    # Set an explicit title on every single-source view, so grapher does not fall back to the
    # indicator display name.
    set_view_titles(c, dims_max)

    # Add "by source" stacked views that decompose each aggregate into its constituent sources.
    add_decomposition_views(c)

    #
    # Save outputs.
    #
    c.save()


# Natural-language source names for view titles (lower-case, to read inside a sentence).
SOURCE_TITLE_NAMES = {
    "total": "total",
    "coal": "coal",
    "oil": "oil",
    "gas": "gas",
    "fossil": "fossil fuels",
    "nuclear": "nuclear",
    "hydro": "hydropower",
    "wind": "wind",
    "solar": "solar",
    "solar_and_wind": "solar and wind",
    "renewables": "renewables",
    "other_renewables": "other renewables",
    "bioenergy": "bioenergy",
    "low_carbon": "low-carbon sources",
}

# Total-only metrics have a single fixed title (they only exist for source "total").
TOTAL_ONLY_TITLES = {
    "demand": "Electricity demand",
    "demand_per_capita": "Electricity demand per person",
    "net_imports": "Net electricity imports",
    "imports_share": "Net electricity imports as a share of demand",
    "carbon_intensity": "Carbon intensity of electricity",
}


def _view_title(source: str, metric: str) -> str:
    if metric in TOTAL_ONLY_TITLES:
        return TOTAL_ONLY_TITLES[metric]
    if source == "total":
        return {
            "generation": "Electricity generation",
            "per_capita": "Electricity generation per person",
            "share_of_generation": "Electricity generation",
        }[metric]
    name = SOURCE_TITLE_NAMES[source]
    return {
        "generation": f"Electricity generation from {name}",
        "per_capita": f"Electricity generation from {name} per person",
        "share_of_generation": f"Share of electricity generation from {name}",
    }[metric]


# Unit phrase per metric, and composition notes for composite sources. The original charts spelled out
# what each grouping contains (e.g. which renewables are included); we keep those notes.
METRIC_UNIT_PHRASE = {
    "generation": "Measured in [terawatt-hours](#dod:watt-hours).",
    "per_capita": "Measured in [kilowatt-hours](#dod:watt-hours) per person.",
    "share_of_generation": "Measured as a percentage of total electricity generation.",
}
SOURCE_COMPOSITION = {
    "fossil": "Fossil fuels include coal, oil, and gas.",
    "renewables": "Renewables include solar, wind, hydropower, bioenergy, geothermal, wave, and tidal.",
    "low_carbon": "Low-carbon sources are the sum of nuclear and renewables.",
    "other_renewables": "Other renewables include bioenergy, geothermal, wave, and tidal.",
    "solar_and_wind": "Combined electricity generation from solar and wind.",
    "wind": "Includes both onshore and offshore wind.",
}
# Total-only metrics have a single fixed subtitle (they only exist for source "total").
TOTAL_ONLY_SUBTITLES = {
    "demand": "Total electricity generation, adjusted for imports and exports. Measured in [terawatt-hours](#dod:watt-hours).",
    "demand_per_capita": "Electricity generation adjusted for imports and exports. Measured in [kilowatt-hours](#dod:watt-hours) per person.",
    "net_imports": "Electricity imports minus exports, measured in [terawatt-hours](#dod:watt-hours). Positive values are net importers; negative values are net exporters.",
    "imports_share": "Electricity imports minus exports, as a share of demand. Positive values are net importers; negative values are net exporters.",
    "carbon_intensity": "Measured in grams of [carbon dioxide-equivalents](#dod:carbondioxideequivalents) per [kilowatt-hour](#dod:watt-hours), on a lifecycle basis.",
}


def _view_subtitle(source: str, metric: str) -> str:
    if metric in TOTAL_ONLY_SUBTITLES:
        return TOTAL_ONLY_SUBTITLES[metric]
    unit = METRIC_UNIT_PHRASE[metric]
    note = SOURCE_COMPOSITION.get(source)
    return f"{unit} {note}" if note else unit


# Aggregates that can be decomposed "by source", mapped to their constituent individual sources.
# Constituents are listed top-to-bottom for the stacked chart (grapher renders the first series at the
# top). "other_renewables" already includes bioenergy, so "bioenergy" is left out to avoid double count.
AGGREGATE_DECOMPOSITION = {
    "total": ["other_renewables", "solar", "wind", "hydro", "nuclear", "gas", "oil", "coal"],
    "fossil": ["gas", "oil", "coal"],
    "renewables": ["other_renewables", "solar", "wind", "hydro"],
    "low_carbon": ["other_renewables", "solar", "wind", "hydro", "nuclear"],
    "solar_and_wind": ["solar", "wind"],
}
# Base metric each decomposition is built from -> the metric slug it becomes.
_DECOMPOSITION_METRICS = {"generation": "by_source", "per_capita": "by_source_per_capita"}
# Title stem per aggregate.
_DECOMPOSITION_STEM = {
    "total": "Electricity generation",
    "fossil": "Electricity from fossil fuels",
    "renewables": "Electricity from renewables",
    "low_carbon": "Electricity from low-carbon sources",
    "solar_and_wind": "Electricity from solar and wind",
}


def _decomposition_title(source: str, base_metric: str) -> str:
    stem = _DECOMPOSITION_STEM[source]
    return f"{stem} per person, by source" if base_metric == "per_capita" else f"{stem} by source"


def add_decomposition_views(c) -> None:
    """Add stacked "by source" views that break each aggregate into its constituent sources.

    These live on the metric dimension (by_source / by_source_per_capita) and only exist for
    aggregates, so grapher hides the metric when an individual source is selected.
    """
    base_config = {"chartTypes": ["StackedArea"], "tab": "chart", "hasMapTab": False, "hideRelativeToggle": False}
    single_views = {(v.dimensions.get("source"), v.dimensions.get("metric")): v for v in c.views}
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
                "title": _decomposition_title(source, base_metric),
                "subtitle": METRIC_UNIT_PHRASE[base_metric],
            }
            new_view = View(
                dimensions={"source": source, "metric": new_metric},
                indicators=ViewIndicators(y=indicators),
                config=config,
            )
            new_view.mark_as_grouped()
            c.views.append(new_view)


# Map color scheme per (source, metric), copied from the original production charts each view replaces
# (fetched from their chart configs) so the new maps look like the ones users already know. Views with
# no pre-existing chart fall back to a per-source family below.
ORIGINAL_MAP_SCHEMES = {
    ("bioenergy", "share_of_generation"): {"baseColorScheme": "BuGn"},
    ("coal", "generation"): {"baseColorScheme": "Oranges"},
    ("coal", "per_capita"): {"baseColorScheme": "YlOrBr"},
    ("coal", "share_of_generation"): {"baseColorScheme": "Oranges"},
    ("fossil", "generation"): {"baseColorScheme": "YlOrBr"},
    ("fossil", "per_capita"): {"baseColorScheme": "Oranges"},
    ("fossil", "share_of_generation"): {"baseColorScheme": "OrRd"},
    ("gas", "generation"): {"baseColorScheme": "Purples"},
    ("gas", "per_capita"): {"baseColorScheme": "BuPu"},
    ("gas", "share_of_generation"): {"baseColorScheme": "GnBu"},
    ("hydro", "generation"): {"baseColorScheme": "GnBu"},
    ("hydro", "per_capita"): {"baseColorScheme": "GnBu"},
    ("hydro", "share_of_generation"): {"baseColorScheme": "PuBu"},
    ("low_carbon", "generation"): {"baseColorScheme": "BuGn"},
    ("low_carbon", "share_of_generation"): {"baseColorScheme": "YlGn"},
    ("nuclear", "generation"): {"baseColorScheme": "GnBu"},
    ("nuclear", "per_capita"): {"baseColorScheme": "PuBuGn"},
    ("nuclear", "share_of_generation"): {"baseColorScheme": "YlGnBu"},
    ("oil", "generation"): {"baseColorScheme": "Reds"},
    ("oil", "per_capita"): {"baseColorScheme": "OrRd"},
    ("oil", "share_of_generation"): {"baseColorScheme": "Oranges"},
    ("renewables", "generation"): {"baseColorScheme": "GnBu"},
    ("renewables", "per_capita"): {"baseColorScheme": "YlGn"},
    ("renewables", "share_of_generation"): {"baseColorScheme": "BuGn"},
    ("solar", "generation"): {"baseColorScheme": "YlOrRd"},
    ("solar", "per_capita"): {"baseColorScheme": "YlOrRd"},
    ("solar", "share_of_generation"): {"baseColorScheme": "YlGnBu"},
    ("solar_and_wind", "generation"): {"baseColorScheme": "YlOrRd"},
    ("solar_and_wind", "per_capita"): {"baseColorScheme": "YlGnBu"},
    ("solar_and_wind", "share_of_generation"): {"baseColorScheme": "Greens"},
    ("total", "carbon_intensity"): {"baseColorScheme": "YlOrBr"},
    ("total", "demand"): {"baseColorScheme": "YlGnBu"},
    ("total", "demand_per_capita"): {"baseColorScheme": "PuBuGn"},
    ("total", "generation"): {"baseColorScheme": "YlGnBu"},
    ("total", "imports_share"): {"baseColorScheme": "RdBu"},
    ("total", "net_imports"): {"baseColorScheme": "RdBu", "colorSchemeInvert": True},
    ("total", "per_capita"): {"baseColorScheme": "PuBuGn"},
    ("wind", "generation"): {"baseColorScheme": "PuBu"},
    ("wind", "per_capita"): {"baseColorScheme": "PuBuGn"},
    ("wind", "share_of_generation"): {"baseColorScheme": "PuBuGn"},
}

# Per-source fallback for views without a pre-existing chart, so those maps are not all the same color.
SOURCE_FALLBACK_SCHEME = {
    "total": "YlGnBu",
    "coal": "Oranges",
    "oil": "OrRd",
    "gas": "Purples",
    "fossil": "YlOrBr",
    "nuclear": "GnBu",
    "hydro": "GnBu",
    "wind": "PuBuGn",
    "solar": "YlOrRd",
    "solar_and_wind": "YlOrRd",
    "renewables": "GnBu",
    "other_renewables": "YlGn",
    "bioenergy": "BuGn",
    "low_carbon": "BuGn",
}


# Metrics that are unbounded positive magnitudes: their map should use log-spaced bins with an
# open-ended top bracket (">X"). Percentages (share_of_generation, imports_share) stay closed, and
# the signed net-imports metric is diverging.
LOG_METRICS = {"generation", "per_capita", "demand", "demand_per_capita", "carbon_intensity"}


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


def _map_config(source: str, metric: str, vmax: float | None = None) -> dict:
    scheme = ORIGINAL_MAP_SCHEMES.get((source, metric))
    if scheme is None:
        if metric in ("net_imports", "imports_share"):
            scheme = {"baseColorScheme": "RdBu", "colorSchemeInvert": True}
        elif metric == "carbon_intensity":
            scheme = {"baseColorScheme": "YlOrBr"}
        else:
            scheme = {"baseColorScheme": SOURCE_FALLBACK_SCHEME.get(source, "YlGnBu")}
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
        source = v.dimensions["source"]
        if source not in SOURCE_TITLE_NAMES:
            # Grouped/stacked views already have a title from group_views.
            continue
        metric = v.dimensions["metric"]
        config = dict(v.config or {})
        config["title"] = _view_title(source, metric)
        config["subtitle"] = _view_subtitle(source, metric)
        config["map"] = _map_config(source, metric, dims_max.get((source, metric)))
        v.config = config
