"""Multidim for the energy mix (source x metric), based on Total Energy Supply."""

from copy import deepcopy

from etl.collection.model.view import View, ViewIndicators
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# OWID and Energy-Institute aggregate entities. They are excluded when sizing the map color scale
# because grapher leaves them off the map, and their values (e.g. the World's ~166,000 TWh) would
# otherwise dominate and push the top bin above every country. Entities with an "(EI)" suffix are the
# Statistical Review's own regions and are excluded by suffix.
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

# Map each source's column prefix to its dimension slug.
SOURCE_SLUGS = {
    "coal": "coal",
    "oil": "oil",
    "gas": "gas",
    "fossil_fuels": "fossil_fuels",
    "nuclear": "nuclear",
    "hydro": "hydro",
    "solar": "solar",
    "wind": "wind",
    "solar_and_wind": "solar_and_wind",
    "other_renewables": "other_renewables",
    "renewables": "renewables",
    "low_carbon_energy": "low_carbon_energy",
    "biofuels": "biofuels",
    "total_energy_supply": "total",
}
# Map each metric's column suffix to its dimension slug.
METRIC_SUFFIXES = {
    "_twh": "total",
    "_per_capita_kwh": "per_capita",
    "_share_pct": "share",
    "_annual_change_twh": "annual_change",
}


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("energy_mix")
    tb = ds.read("energy_mix", reset_index=False)

    #
    # Process data.
    #
    # Assign (source, metric) dimensions to each grapher column that maps to a view.
    column_dimensions = {}
    for source_prefix, source_slug in SOURCE_SLUGS.items():
        for suffix, metric_slug in METRIC_SUFFIXES.items():
            column = f"{source_prefix}{suffix}"
            if column in tb.columns:
                column_dimensions[column] = {"source": source_slug, "metric": metric_slug}

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
        tb[column].m.original_short_name = "energy"

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
        indicator_names=["energy"],
        dimensions=["source", "metric"],
        common_view_config=common_view_config,
    )

    # Set an explicit title on every single-source view. Otherwise grapher falls back to the
    # indicator's display name (e.g. the annual-change indicators inherit "Total energy supply"),
    # which mislabels the view.
    set_view_titles(c, dims_max)

    # Add "by source" stacked views that decompose each aggregate into its constituent sources.
    add_decomposition_views(c)

    #
    # Save outputs.
    #
    c.save()


# Natural-language source names for view titles (lower-case, to read inside a sentence).
SOURCE_TITLE_NAMES = {
    "total": "total energy supply",
    "coal": "coal",
    "oil": "oil",
    "gas": "gas",
    "fossil_fuels": "fossil fuels",
    "nuclear": "nuclear",
    "hydro": "hydropower",
    "wind": "wind",
    "solar": "solar",
    "solar_and_wind": "solar and wind",
    "renewables": "renewables",
    "other_renewables": "other renewables",
    "biofuels": "biofuels",
    "low_carbon_energy": "low-carbon energy",
}


def _view_title(source: str, metric: str) -> str:
    if source == "total":
        return {
            "total": "Total energy supply",
            "per_capita": "Total energy supply per person",
            "share": "Total energy supply",
            "annual_change": "Annual change in total energy supply",
        }[metric]
    name = SOURCE_TITLE_NAMES[source]
    return {
        "total": f"Energy supply from {name}",
        "per_capita": f"Energy supply from {name} per person",
        "share": f"Share of energy supply from {name}",
        "annual_change": f"Annual change in energy supply from {name}",
    }[metric]


# Unit phrase per metric, and composition notes for composite sources. The original charts spelled out
# what each grouping contains (e.g. which renewables are included); we keep those notes but drop the
# "primary energy / substitution method" wording, which no longer applies to Total Energy Supply.
METRIC_UNIT_PHRASE = {
    "total": "Measured in [terawatt-hours](#dod:watt-hours) of [total energy supply](#dod:total-energy-supply).",
    "per_capita": "Measured in [kilowatt-hours](#dod:watt-hours) of [total energy supply](#dod:total-energy-supply) per person.",
    "share": "Measured as a percentage of [total energy supply](#dod:total-energy-supply).",
    "annual_change": "Year-on-year change in [total energy supply](#dod:total-energy-supply), measured in [terawatt-hours](#dod:watt-hours).",
}
SOURCE_COMPOSITION = {
    "fossil_fuels": "Fossil fuels are the sum of coal, oil, and gas.",
    "renewables": "Renewables include hydropower, solar, wind, geothermal, wave and tidal, and bioenergy.",
    "low_carbon_energy": "Low-carbon energy is the sum of nuclear and renewables.",
    "other_renewables": "Other renewables include geothermal, wave, and tidal energy.",
    "solar_and_wind": "Combined energy supply from solar and wind.",
}


def _view_subtitle(source: str, metric: str) -> str:
    unit = METRIC_UNIT_PHRASE[metric]
    note = SOURCE_COMPOSITION.get(source)
    return f"{unit} {note}" if note else unit


# Aggregates that can be decomposed "by source", mapped to their constituent individual sources.
# Constituents are listed top-to-bottom for the stacked chart (grapher renders the first series at the
# top), so the smallest sit at the top and the largest at the bottom.
AGGREGATE_DECOMPOSITION = {
    "total": ["other_renewables", "biofuels", "solar", "wind", "hydro", "nuclear", "gas", "oil", "coal"],
    "fossil_fuels": ["gas", "oil", "coal"],
    "renewables": ["other_renewables", "biofuels", "solar", "wind", "hydro"],
    "low_carbon_energy": ["other_renewables", "biofuels", "solar", "wind", "hydro", "nuclear"],
    "solar_and_wind": ["solar", "wind"],
}
# Base metric each decomposition is built from -> the metric slug it becomes.
_DECOMPOSITION_METRICS = {"total": "by_source", "per_capita": "by_source_per_capita"}
# Title stem per aggregate.
_DECOMPOSITION_STEM = {
    "total": "Total energy supply",
    "fossil_fuels": "Fossil fuel supply",
    "renewables": "Renewable energy supply",
    "low_carbon_energy": "Low-carbon energy supply",
    "solar_and_wind": "Solar and wind supply",
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
    ("coal", "annual_change"): {"baseColorScheme": "BrBG", "colorSchemeInvert": True},
    ("coal", "per_capita"): {"baseColorScheme": "OrRd"},
    ("coal", "share"): {"baseColorScheme": "YlOrBr"},
    ("fossil_fuels", "annual_change"): {"baseColorScheme": "BrBG", "colorSchemeInvert": True},
    ("fossil_fuels", "per_capita"): {"baseColorScheme": "YlOrBr"},
    ("fossil_fuels", "share"): {"baseColorScheme": "YlOrBr"},
    ("fossil_fuels", "total"): {"baseColorScheme": "YlOrBr"},
    ("gas", "annual_change"): {"baseColorScheme": "PiYG", "colorSchemeInvert": True},
    ("gas", "per_capita"): {"baseColorScheme": "BuPu"},
    ("gas", "share"): {"baseColorScheme": "Blues"},
    ("hydro", "annual_change"): {"baseColorScheme": "RdYlBu"},
    ("hydro", "per_capita"): {"baseColorScheme": "PuBu"},
    ("hydro", "share"): {"baseColorScheme": "PuBu"},
    ("hydro", "total"): {"baseColorScheme": "GnBu"},
    ("low_carbon_energy", "annual_change"): {"baseColorScheme": "PuOr"},
    ("low_carbon_energy", "per_capita"): {"baseColorScheme": "Purples"},
    ("low_carbon_energy", "share"): {"baseColorScheme": "Greens"},
    ("low_carbon_energy", "total"): {"baseColorScheme": "GnBu"},
    ("nuclear", "annual_change"): {"baseColorScheme": "RdYlBu"},
    ("nuclear", "per_capita"): {"baseColorScheme": "PuBuGn"},
    ("nuclear", "share"): {"baseColorScheme": "BuPu"},
    ("nuclear", "total"): {"baseColorScheme": "BuPu"},
    ("oil", "annual_change"): {"baseColorScheme": "PRGn", "colorSchemeInvert": True},
    ("oil", "per_capita"): {"baseColorScheme": "OrRd"},
    ("oil", "share"): {"baseColorScheme": "YlOrRd"},
    ("renewables", "annual_change"): {"baseColorScheme": "RdBu"},
    ("renewables", "per_capita"): {"baseColorScheme": "YlGnBu"},
    ("renewables", "share"): {"baseColorScheme": "Greens"},
    ("renewables", "total"): {"baseColorScheme": "YlGn"},
    ("solar", "annual_change"): {"baseColorScheme": "RdBu"},
    ("solar", "per_capita"): {"baseColorScheme": "PuBu"},
    ("solar", "share"): {"baseColorScheme": "YlOrRd"},
    ("solar", "total"): {"baseColorScheme": "YlOrRd"},
    ("solar_and_wind", "annual_change"): {"baseColorScheme": "RdBu"},
    ("solar_and_wind", "per_capita"): {"baseColorScheme": "BuGn"},
    ("solar_and_wind", "share"): {"baseColorScheme": "BuGn"},
    ("solar_and_wind", "total"): {"baseColorScheme": "YlOrRd"},
    ("total", "annual_change"): {"baseColorScheme": "BrBG", "colorSchemeInvert": True},
    ("total", "per_capita"): {"baseColorScheme": "OrRd"},
    ("total", "total"): {"baseColorScheme": "YlGnBu"},
    ("wind", "annual_change"): {"baseColorScheme": "PRGn"},
    ("wind", "per_capita"): {"baseColorScheme": "YlGn"},
    ("wind", "share"): {"baseColorScheme": "PuBuGn"},
    ("wind", "total"): {"baseColorScheme": "PuBuGn"},
}

# Per-source fallback for views without a pre-existing chart, so those maps are not all the same color.
SOURCE_FALLBACK_SCHEME = {
    "total": "YlGnBu",
    "coal": "YlOrBr",
    "oil": "OrRd",
    "gas": "BuPu",
    "fossil_fuels": "YlOrBr",
    "nuclear": "BuPu",
    "hydro": "PuBu",
    "wind": "PuBuGn",
    "solar": "YlOrRd",
    "solar_and_wind": "BuGn",
    "renewables": "Greens",
    "other_renewables": "YlGn",
    "biofuels": "YlGn",
    "low_carbon_energy": "GnBu",
}


# Metrics that are unbounded positive magnitudes: their map should use log-spaced bins with an
# open-ended top bracket (">X"). Percentages (share) stay closed, and annual_change is diverging.
LOG_METRICS = {"total", "per_capita"}


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
    # timeTolerance fills the newest map year for countries whose latest data is a year or two old
    # (e.g. EIA-extended countries end in 2024 while the Statistical Review reaches 2025).
    scheme = ORIGINAL_MAP_SCHEMES.get((source, metric))
    if scheme is None:
        if metric == "annual_change":
            scheme = {"baseColorScheme": "BrBG", "colorSchemeInvert": True}
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
