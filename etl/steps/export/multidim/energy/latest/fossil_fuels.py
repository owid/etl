"""Multidim for fossil fuels (fuel x metric x per capita)."""

import math
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

FUELS = ["coal", "oil", "gas"]
# Per-fuel column suffixes (base units) of the physical trade/consumption columns (mirroring the
# garden step). Totals and per-capita variants share the same suffix.
TRADE_SUFFIX = {"coal": "tonnes", "oil": "m3", "gas": "m3"}
TRADE_PC_SUFFIX = {"coal": "tonnes", "oil": "m3", "gas": "m3"}
TRADE_METRICS = ["consumption", "imports", "exports", "net_imports"]


def _column_dimensions() -> dict:
    """Map each grapher column to its (fuel, metric, per_capita) dimensions."""
    dims = {}
    # NOTE: The World-only reserves-to-production ratio stays in the dataset but is deliberately not
    # exposed here; it lives in the standalone "Years of fossil fuel reserves left" chart, where the
    # three fuels can be overlaid.
    for fuel in FUELS:
        dims[f"{fuel}_production_twh"] = {"fuel": fuel, "metric": "production", "per_capita": "total"}
        dims[f"{fuel}_production_per_capita_kwh"] = {"fuel": fuel, "metric": "production", "per_capita": "per_capita"}
        # Consumption in energy units (Statistical Review, extended with EIA — same blend as production).
        dims[f"{fuel}_consumption_twh"] = {"fuel": fuel, "metric": "consumption", "per_capita": "total"}
        dims[f"{fuel}_consumption_per_capita_kwh"] = {
            "fuel": fuel,
            "metric": "consumption",
            "per_capita": "per_capita",
        }
        # Trade and consumption in physical units (EIA): coal in tonnes, oil and gas in cubic meters.
        # The physical consumption columns are named "consumption" in the garden but take the
        # "consumption_physical" metric slug here (the energy-content series owns "consumption").
        for metric in TRADE_METRICS:
            slug = "consumption_physical" if metric == "consumption" else metric
            dims[f"{fuel}_{metric}_{TRADE_SUFFIX[fuel]}"] = {"fuel": fuel, "metric": slug, "per_capita": "total"}
            dims[f"{fuel}_{metric}_per_capita_{TRADE_PC_SUFFIX[fuel]}"] = {
                "fuel": fuel,
                "metric": slug,
                "per_capita": "per_capita",
            }
    # Production in physical units (Statistical Review): coal and oil in tonnes, gas in cubic meters.
    # Units differ per fuel, so there is no "total" (they can't be summed).
    dims.update(
        {
            "coal_production_tonnes": {"fuel": "coal", "metric": "production_physical", "per_capita": "total"},
            "oil_production_tonnes": {"fuel": "oil", "metric": "production_physical", "per_capita": "total"},
            "gas_production_m3": {"fuel": "gas", "metric": "production_physical", "per_capita": "total"},
            "coal_production_per_capita_tonnes": {
                "fuel": "coal",
                "metric": "production_physical",
                "per_capita": "per_capita",
            },
            "oil_production_per_capita_tonnes": {
                "fuel": "oil",
                "metric": "production_physical",
                "per_capita": "per_capita",
            },
            "gas_production_per_capita_m3": {
                "fuel": "gas",
                "metric": "production_physical",
                "per_capita": "per_capita",
            },
            # Proved reserves in physical units (EIA; oil and gas through 2021, coal through 2023).
            "coal_reserves_tonnes": {"fuel": "coal", "metric": "reserves", "per_capita": "total"},
            "oil_reserves_m3": {"fuel": "oil", "metric": "reserves", "per_capita": "total"},
            "gas_reserves_m3": {"fuel": "gas", "metric": "reserves", "per_capita": "total"},
            "coal_reserves_per_capita_tonnes": {"fuel": "coal", "metric": "reserves", "per_capita": "per_capita"},
            "oil_reserves_per_capita_m3": {"fuel": "oil", "metric": "reserves", "per_capita": "per_capita"},
            "gas_reserves_per_capita_m3": {"fuel": "gas", "metric": "reserves", "per_capita": "per_capita"},
            # Total fossil fuel production and consumption (the aggregates that the "by fuel"
            # breakdowns decompose).
            "total_production_twh": {"fuel": "total", "metric": "production", "per_capita": "total"},
            "total_production_per_capita_kwh": {"fuel": "total", "metric": "production", "per_capita": "per_capita"},
            "total_consumption_twh": {"fuel": "total", "metric": "consumption", "per_capita": "total"},
            "total_consumption_per_capita_kwh": {"fuel": "total", "metric": "consumption", "per_capita": "per_capita"},
        }
    )
    return dims


COLUMN_DIMENSIONS = _column_dimensions()


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("fossil_fuels")
    tb = ds.read("fossil_fuels", reset_index=False)

    #
    # Process data.
    #
    # Keep only the columns that map to a (fuel, metric, per_capita) view, and set their dimensions.
    tb = tb[list(COLUMN_DIMENSIONS)]
    # Reference magnitudes per view for sizing the map bins, across countries only (aggregates
    # excluded): the 99th percentile for the zero-floored log bins (single outliers ignored), and the
    # true extremes for the diverging net-imports bins (the outer edge follows the smaller tail, so a
    # percentile would shrink the scale by a decade). See set_view_titles.
    country_level = tb.index.get_level_values("country")
    is_country = ~(country_level.isin(AGGREGATE_ENTITIES) | country_level.str.contains("(EI)", regex=False))
    tb_countries = tb[is_country]
    dims_stats = {
        (dims["fuel"], dims["metric"], dims["per_capita"]): (
            float(tb_countries[column].astype("float64").min()),
            float(tb_countries[column].astype("float64").quantile(0.99)),
            float(tb_countries[column].astype("float64").max()),
        )
        for column, dims in COLUMN_DIMENSIONS.items()
    }
    for column, dims in COLUMN_DIMENSIONS.items():
        tb[column].m.dimensions = dims
        tb[column].m.original_short_name = "fossil_fuels"

    common_view_config = {
        "hasMapTab": True,
        "tab": "map",
        # Line + bar tabs (grapher's default), so single-fuel views keep the bar tab the
        # original charts had.
        "chartTypes": ["LineChart", "DiscreteBar"],
    }

    c = paths.create_collection(
        config=paths.load_collection_config(),
        tb=tb,
        indicator_names=["fossil_fuels"],
        dimensions=["fuel", "metric", "per_capita"],
        common_view_config=common_view_config,
    )

    # Set an explicit title on every single-fuel view, so grapher does not fall back to the
    # indicator display name.
    set_view_titles(c, dims_stats)

    # Add "by fuel" stacked views that decompose total fossil fuel production and consumption into
    # coal, oil, and gas.
    add_decomposition_views(c)

    #
    # Save outputs.
    #
    c.save()


# Single-fuel views (coal/oil/gas) plus the "total" aggregate get an explicit title/subtitle/map.
FUEL_TITLE_NAMES = {"total": "fossil fuels", "coal": "coal", "oil": "oil", "gas": "gas"}


def _view_title(fuel: str, metric: str, count: str) -> str:
    per_person = " per person" if count == "per_capita" else ""
    if fuel == "total":
        stem = {"production": "Fossil fuel production", "consumption": "Fossil fuel consumption"}[metric]
        return f"{stem}{per_person}"
    name = FUEL_TITLE_NAMES[fuel]
    if metric == "net_imports":
        return f"Net imports of {name}{per_person}"
    stem = {
        # The physical-unit views share the energy views' titles; the metric dropdown, subtitle, and
        # axis carry the unit.
        "production": f"{name.capitalize()} production",
        "production_physical": f"{name.capitalize()} production",
        "consumption": f"{name.capitalize()} consumption",
        "consumption_physical": f"{name.capitalize()} consumption",
        "imports": f"{name.capitalize()} imports",
        "exports": f"{name.capitalize()} exports",
        "reserves": f"{name.capitalize()} reserves",
    }[metric]
    return f"{stem}{per_person}"


# Composition note appended to the total (all fossil fuels) views, so the subtitle spells out what
# "fossil fuels" covers.
FOSSIL_FUELS_NOTE = "Fossil fuels include coal, oil, and gas."

# Unit phrase for the energy-content metrics (production and its "by fuel" decomposition).
ENERGY_UNIT_PHRASE = {
    "total": "Measured in [terawatt-hours](#dod:watt-hours).",
    "per_capita": "Measured in [kilowatt-hours](#dod:watt-hours) per person.",
}

# Physical units per (fuel, metric, per_capita). All physical metrics are stored in base units
# (tonnes for coal, cubic meters for oil and gas), so grapher applies magnitude prefixes itself.
PHYSICAL_UNITS = {
    ("coal", "production_physical", "total"): "tonnes",
    ("coal", "production_physical", "per_capita"): "tonnes per person",
    ("oil", "production_physical", "total"): "tonnes",
    ("oil", "production_physical", "per_capita"): "tonnes per person",
    ("gas", "production_physical", "total"): "cubic meters",
    ("gas", "production_physical", "per_capita"): "cubic meters per person",
    ("coal", "reserves", "total"): "tonnes",
    ("coal", "reserves", "per_capita"): "tonnes per person",
    ("oil", "reserves", "total"): "cubic meters",
    ("oil", "reserves", "per_capita"): "cubic meters per person",
    ("gas", "reserves", "total"): "cubic meters",
    ("gas", "reserves", "per_capita"): "cubic meters per person",
}
_TRADE_UNITS = {
    "coal": ("tonnes", "tonnes per person"),
    "oil": ("cubic meters", "cubic meters per person"),
    "gas": ("cubic meters", "cubic meters per person"),
}
for _fuel, (_unit, _unit_pc) in _TRADE_UNITS.items():
    for _metric in ["consumption_physical", "imports", "exports", "net_imports"]:
        PHYSICAL_UNITS[(_fuel, _metric, "total")] = _unit
        PHYSICAL_UNITS[(_fuel, _metric, "per_capita")] = _unit_pc

NET_IMPORTS_NOTE = (
    "Net imports are imports minus exports; negative values indicate that the country exports more than it imports."
)
# "Oil" covers a different basket of commodities in each metric, so every oil view spells out what is
# included in its subtitle. Production combines the Statistical Review (crude, shale oil, oil sands,
# condensates, NGLs) with an EIA fill that also counts other liquid fuels; trade and reserves (EIA)
# are crude only; consumption (EIA) is all liquids.
_OIL_TRADE_NOTE = "Includes crude oil and lease condensate; refined petroleum products are not included."
OIL_NOTES = {
    "production": "Includes crude oil, condensates, natural gas liquids, and other liquid fuels.",
    "production_physical": "Includes crude oil, shale oil, oil sands, condensates, and natural gas liquids.",
    "consumption": "Includes refined petroleum products and other liquid fuels.",
    "consumption_physical": "Includes all petroleum products and other liquid fuels, such as biofuels.",
    "imports": _OIL_TRADE_NOTE,
    "exports": _OIL_TRADE_NOTE,
    "net_imports": _OIL_TRADE_NOTE,
    "reserves": "Includes crude oil and lease condensate.",
}


# Definition carried over from the original per-fuel reserves charts, which spelled out what a "proved
# reserve" means. The unit is filled in per fuel.
RESERVES_DEFINITION = (
    "Proved reserves, measured in {unit}, are generally those quantities that can be recovered in the "
    "future from known reservoirs under existing economic and operating conditions, according to "
    "geological and engineering information."
)


def _view_subtitle(fuel: str, metric: str, count: str) -> str:
    if metric in ("production", "consumption"):
        sentence = ENERGY_UNIT_PHRASE[count]
        if fuel == "total":
            sentence = f"{sentence} {FOSSIL_FUELS_NOTE}"
    elif metric == "reserves" and count == "total":
        sentence = RESERVES_DEFINITION.format(unit=PHYSICAL_UNITS[(fuel, metric, count)])
    else:
        sentence = f"Measured in {PHYSICAL_UNITS[(fuel, metric, count)]}."
        if metric == "net_imports":
            sentence = f"{sentence} {NET_IMPORTS_NOTE}"
    if fuel == "oil" and metric in OIL_NOTES:
        sentence = f"{sentence} {OIL_NOTES[metric]}"
    return sentence


def _decomposition_title(metric: str, count: str) -> str:
    stem = {"production": "Fossil fuel production", "consumption": "Fossil fuel consumption"}[metric]
    return f"{stem} per person, by fuel" if count == "per_capita" else f"{stem} by fuel"


def add_decomposition_views(c) -> None:
    """Add stacked "by fuel" views that break total fossil fuel production and consumption into coal,
    oil, and gas.

    Constituents are listed top-to-bottom for the stacked chart (grapher renders the first series at
    the top), so coal sits at the bottom.
    """
    base_config = {
        "chartTypes": ["StackedArea"],
        "tab": "chart",
        "hasMapTab": False,
        "hideRelativeToggle": False,
        "yAxis": {"min": 0},
    }
    single_views = {
        (v.dimensions.get("fuel"), v.dimensions.get("metric"), v.dimensions.get("per_capita")): v for v in c.views
    }
    for metric in ["production", "consumption"]:
        for count in ["total", "per_capita"]:
            indicators = []
            for constituent in ["gas", "oil", "coal"]:
                view = single_views.get((constituent, metric, count))
                if view is not None and view.indicators.y:
                    indicators.extend(deepcopy(view.indicators.y))
            if not indicators:
                continue
            config = {
                **base_config,
                "title": _decomposition_title(metric, count),
                "subtitle": ENERGY_UNIT_PHRASE[count],
            }
            new_view = View(
                dimensions={"fuel": "total", "metric": f"{metric}_by_fuel", "per_capita": count},
                indicators=ViewIndicators(y=indicators),
                config=config,
            )
            new_view.mark_as_grouped()
            c.views.append(new_view)


# Map color scheme per (fuel, metric, per_capita), copied from the original production charts each
# view replaces (fetched from their chart configs) so the new maps look like the ones users already
# know. Views with no pre-existing chart fall back to a per-fuel family below.
ORIGINAL_MAP_SCHEMES = {
    ("coal", "production", "per_capita"): {"baseColorScheme": "YlOrBr"},
    ("coal", "production", "total"): {"baseColorScheme": "OrRd"},
    ("gas", "production", "per_capita"): {"baseColorScheme": "BuPu"},
    ("gas", "production", "total"): {"baseColorScheme": "Purples"},
    ("oil", "production", "per_capita"): {"baseColorScheme": "YlOrRd"},
    ("oil", "production", "total"): {"baseColorScheme": "YlOrRd"},
}

# Per-fuel fallback for views without a pre-existing chart (e.g. total, reserves, trade).
FUEL_FALLBACK_SCHEME = {"total": "YlOrBr", "coal": "OrRd", "oil": "YlOrRd", "gas": "Purples"}


def _log_thresholds(vmax: float | None, max_bins: int = 7) -> list[float] | None:
    """1-2-5 log-spaced bin edges from 0 up to the largest ladder value strictly below vmax.

    Keeping the top edge below the data max makes grapher render an open-ended top bin
    (isOpenRight = last edge < data max), matching the original charts' brackets.
    """
    if vmax is None or not (vmax > 0):
        return None
    ladder = [m * 10**p for p in range(-3, 13) for m in (1, 2, 5)]
    below = [v for v in ladder if v < vmax]
    if len(below) < 2:
        return None
    return [0] + below[-(max_bins - 1) :]


def _diverging_thresholds(vmin: float | None, vmax: float | None, decades: int = 4) -> list[float] | None:
    """Symmetric decade-spaced bin edges around zero for diverging indicators (net imports).

    The outermost edge is the largest power of ten not exceeding the smaller tail's extreme, so
    actual data extends beyond the edges on both sides and grapher renders open-ended brackets
    ("<-X" and ">X") at both ends.
    """
    if vmin is None or vmax is None or not (vmin < 0 < vmax):
        return None
    top = 10 ** math.floor(math.log10(min(-vmin, vmax)))
    positive = [top / 10**i for i in range(decades)]
    return sorted({-edge for edge in positive} | {0} | set(positive))


def _map_config(fuel: str, metric: str, count: str, stats: tuple[float, float, float] | None = None) -> dict:
    vmin, vmax_q99, vmax = stats if stats is not None else (None, None, None)
    # Net imports are diverging (negative for net exporters): symmetric decade bins around zero,
    # open-ended on both sides since the indicator is unbounded in both directions.
    if metric == "net_imports":
        color_scale = {"baseColorScheme": "RdBu"}
        edges = _diverging_thresholds(vmin, vmax)
        if edges:
            color_scale["binningStrategy"] = "manual"
            color_scale["customNumericValues"] = edges
        return {"colorScale": color_scale, "timeTolerance": 3}
    scheme = ORIGINAL_MAP_SCHEMES.get((fuel, metric, count)) or {"baseColorScheme": FUEL_FALLBACK_SCHEME[fuel]}
    color_scale = dict(scheme)
    if (fuel, metric, count) in FIXED_MAP_EDGES:
        color_scale["binningStrategy"] = "manual"
        # Trailing sentinel (smaller than the top edge) makes grapher render an open-ended top bracket.
        color_scale["customNumericValues"] = FIXED_MAP_EDGES[(fuel, metric, count)] + [1]
    else:
        edges = _log_thresholds(vmax_q99)
        if edges:
            color_scale["binningStrategy"] = "manual"
            # Trailing sentinel (smaller than the top edge) forces grapher to render an open-ended
            # top bracket (">X"), independent of where the top edge sits relative to the data max.
            color_scale["customNumericValues"] = edges + [1]
    return {"colorScale": color_scale, "timeTolerance": 3}


# Hand-set map brackets that beat the auto log-ladder for a given view, carried over from the original
# chart. Gas reserves span such a wide range that the 1-2-5 ladder looks noisy; the original 100b, 300b,
# 1t, 3t, 10t, 30t brackets (cubic meters) read far better.
FIXED_MAP_EDGES = {
    ("gas", "reserves", "total"): [0, 1e11, 3e11, 1e12, 3e12, 1e13, 3e13],
}


def set_view_titles(c, dims_stats: dict) -> None:
    for v in c.views:
        fuel = v.dimensions["fuel"]
        if fuel not in FUEL_TITLE_NAMES:
            # Grouped/stacked view already has a title from group_views.
            continue
        metric = v.dimensions["metric"]
        count = v.dimensions["per_capita"]
        config = dict(v.config or {})
        config["title"] = _view_title(fuel, metric, count)
        config["subtitle"] = _view_subtitle(fuel, metric, count)
        config["map"] = _map_config(fuel, metric, count, dims_stats.get((fuel, metric, count)))
        # Zero-floored axis everywhere except net imports, which are legitimately negative for net
        # exporters (grapher then picks the axis range automatically).
        if metric != "net_imports":
            config["yAxis"] = {"min": 0}
        v.config = config
