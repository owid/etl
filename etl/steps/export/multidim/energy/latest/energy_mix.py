"""Multidim for the energy mix (source x metric), based on Total Energy Supply."""

import math
from copy import deepcopy

from etl.collection.model.view import Indicator, View, ViewIndicators
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
    # Reference magnitude per (source, metric) for sizing the map bins: the 99th percentile across
    # countries only (aggregates excluded, single outliers ignored). For annual change we use the 99th
    # percentile of the *absolute* change, to size the symmetric diverging bins. See set_view_titles.
    country_level = tb.index.get_level_values("country")
    is_country = ~(country_level.isin(AGGREGATE_ENTITIES) | country_level.str.contains("(EI)", regex=False))
    tb_countries = tb[is_country]
    dims_max = {}
    for column, dims in column_dimensions.items():
        series = tb_countries[column].astype("float64")
        series = series.abs() if dims["metric"] == "annual_change" else series
        dims_max[(dims["source"], dims["metric"])] = float(series.quantile(0.99))
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

    # Add the carbon-intensity-of-energy view (Total only), from the Global Carbon Budget.
    add_carbon_intensity_view(c)

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
        "total": f"Total energy supply from {name}",
        "per_capita": f"Total energy supply from {name} per person",
        "share": f"Share of total energy supply from {name}",
        "annual_change": f"Annual change in total energy supply from {name}",
    }[metric]


# Unit phrase per metric, and composition notes for composite sources. The original charts spelled out
# what each grouping contains (e.g. which renewables are included); we keep those notes but drop the
# "primary energy / substitution method" wording, which no longer applies to Total Energy Supply.
METRIC_UNIT_PHRASE = {
    "total": "Measured in [terawatt-hours](#dod:watt-hours) of [total energy supply](#dod:total-energy-supply).",
    "per_capita": "Measured in [kilowatt-hours](#dod:watt-hours) of [total energy supply](#dod:total-energy-supply) per person.",
    "share": "Measured as a percentage of [total energy supply](#dod:total-energy-supply).",
    "annual_change": "Annual change in [total energy supply](#dod:total-energy-supply) in one year, relative to the previous year.",
}
SOURCE_COMPOSITION = {
    "fossil_fuels": "Fossil fuels are the sum of coal, oil, and gas.",
    "renewables": "Renewables include hydropower, solar, wind, biofuels, and other renewables (geothermal, biomass, and waste).",
    "low_carbon_energy": "Low-carbon energy is the sum of nuclear and renewables.",
    "other_renewables": "Other renewables include geothermal, biomass, and waste.",
    "solar_and_wind": "Combined energy supply from solar and wind.",
}


# The total-energy-supply views get a definitional subtitle (title and subtitle read as one idea)
# instead of the generic unit phrase, so we explain what TES means and surface "primary". One shared
# definition clause, with the unit tail adapted per metric.
_TES_DEFINITION = (
    "[Total energy supply](#dod:total-energy-supply) is the primary energy a country uses after "
    "accounting for imports and exports"
)
TOTAL_SUPPLY_SUBTITLE = {
    "total": f"{_TES_DEFINITION}, measured in [terawatt-hours](#dod:watt-hours).",
    "per_capita": f"{_TES_DEFINITION}, measured in [kilowatt-hours](#dod:watt-hours) per person.",
    "annual_change": (
        "Annual change in [total energy supply](#dod:total-energy-supply) in one year, relative to the "
        "previous year. Total energy supply is the primary energy a country uses after accounting for "
        "imports and exports."
    ),
}


def _view_subtitle(source: str, metric: str) -> str:
    if source == "total" and metric in TOTAL_SUPPLY_SUBTITLE:
        return TOTAL_SUPPLY_SUBTITLE[metric]
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
# Canonical OWID per-source colors, copied from the original by-source energy charts so the stacked
# "by source" views keep the colors users know (hydropower blue, coal dark red, etc.). "biofuels" here
# is the same bucket the electricity charts label "bioenergy", so it inherits that color.
SOURCE_COLORS = {
    "coal": "#883039",
    "oil": "#c15065",
    "gas": "#6d3e91",
    "nuclear": "#00847e",
    "hydro": "#286bbb",
    "solar": "#e56e5a",
    "wind": "#00295b",
    "biofuels": "#bc8e5a",
    "other_renewables": "#578145",
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
# Footnotes for specific "by source" decomposition views.
_DECOMPOSITION_NOTES = {"renewables": "Traditional biomass is not included."}


def _decomposition_title(source: str, base_metric: str) -> str:
    stem = _DECOMPOSITION_STEM[source]
    return f"{stem} per person, by source" if base_metric == "per_capita" else f"{stem} by source"


# Carbon intensity of energy (CO2 per unit of total energy supply) lives in the Global Carbon Budget,
# which already divides emissions by this same TES. Referenced by short path, expanded via the dep.
CARBON_INTENSITY_INDICATOR = "global_carbon_budget#emissions_total_per_unit_energy"


def add_carbon_intensity_view(c) -> None:
    """Add the Total-only carbon-intensity view, sourced from the Global Carbon Budget."""
    config = {
        "hasMapTab": True,
        "tab": "map",
        "chartTypes": ["LineChart", "DiscreteBar"],
        "title": "Carbon intensity of energy",
        "subtitle": (
            "Measured in grams of CO₂ emitted per [kilowatt-hour](#dod:watt-hours) of "
            "[total energy supply](#dod:total-energy-supply)."
        ),
        "map": {"colorScale": {"baseColorScheme": "YlOrBr"}, "timeTolerance": 3},
    }
    view = View(
        dimensions={"source": "total", "metric": "carbon_intensity"},
        indicators=ViewIndicators(y=[Indicator(catalogPath=CARBON_INTENSITY_INDICATOR)]),
        config=config,
    )
    c.views.append(view)


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
                    for indicator in deepcopy(view.indicators.y):
                        color = SOURCE_COLORS.get(constituent)
                        if color:
                            indicator.update_display({"color": color})
                        indicators.append(indicator)
            if not indicators:
                continue
            config = {
                **base_config,
                "title": _decomposition_title(source, base_metric),
                "subtitle": (
                    TOTAL_SUPPLY_SUBTITLE[base_metric] if source == "total" else METRIC_UNIT_PHRASE[base_metric]
                ),
            }
            note = _DECOMPOSITION_NOTES.get(source)
            if note:
                config["note"] = note
            new_view = View(
                dimensions={"source": source, "metric": new_metric},
                indicators=ViewIndicators(y=indicators),
                config=config,
            )
            new_view.mark_as_grouped()
            c.views.append(new_view)


# Map colorScale per (source, metric), copied verbatim from the original production charts each view
# replaces (fetched from their chart configs) so the new maps reproduce the brackets, color schemes, and
# special bins (e.g. the nuclear "No nuclear" =0 bucket) users already know. When an entry carries explicit
# customNumericValues, _map_config uses them as-is; entries with only a color scheme, or views with no
# pre-existing chart, fall back to a per-source family + auto-sized bins below.
ORIGINAL_MAP_SCHEMES = {
    ("coal", "annual_change"): {
        "baseColorScheme": "BrBG",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -100, -50, -20, 0, 20, 50, 100, 1],
        "customNumericColors": [None],
        "colorSchemeInvert": True,
    },
    ("coal", "per_capita"): {
        "baseColorScheme": "OrRd",
        "binningStrategy": "manual",
        "customNumericValues": [0, 1000, 2000, 5000, 10000, 20000, 50000],
        "customNumericColors": [None],
    },
    ("coal", "share"): {
        "baseColorScheme": "YlOrBr",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60, 70, 1],
        "customNumericColors": [None],
    },
    ("fossil_fuels", "annual_change"): {
        "baseColorScheme": "BrBG",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -200, -100, -50, -20, 0, 20, 50, 100, 200, 1],
        "customNumericColors": [None],
        "colorSchemeInvert": True,
    },
    ("fossil_fuels", "per_capita"): {
        "baseColorScheme": "YlOrBr",
        "binningStrategy": "manual",
        "customNumericValues": [0, 2000, 5000, 10000, 20000, 50000, 100000, 1],
        "customNumericColors": [None],
    },
    ("fossil_fuels", "share"): {
        "baseColorScheme": "YlOrBr",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        "customNumericColors": [None, None, None, None],
    },
    ("fossil_fuels", "total"): {
        "baseColorScheme": "YlOrBr",
        "binningStrategy": "manual",
        "customNumericValues": [0, 200, 500, 1000, 2000, 5000, 10000, 20000, 1],
        "customNumericColors": [None],
    },
    ("gas", "annual_change"): {
        "baseColorScheme": "PiYG",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -50, -20, -10, 0, 10, 20, 50, 1],
        "customNumericColors": [None],
        "colorSchemeInvert": True,
    },
    ("gas", "per_capita"): {
        "baseColorScheme": "BuPu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 2000, 5000, 10000, 20000, 50000, 100000, 1],
        "customNumericColors": [None, None],
    },
    ("gas", "share"): {
        "baseColorScheme": "Blues",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    },
    ("hydro", "annual_change"): {
        "baseColorScheme": "RdYlBu",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -30, -20, -10, 0, 10, 20, 30, 1],
        "customNumericColors": [None],
    },
    ("hydro", "per_capita"): {
        "baseColorScheme": "PuBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 200, 500, 1000, 2000, 5000, 10000, 20000, 1],
        "customNumericColors": [None, None, None],
    },
    ("hydro", "share"): {
        "baseColorScheme": "PuBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 1, 2, 5, 10, 20, 50, 100],
    },
    ("hydro", "total"): {
        "baseColorScheme": "GnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 100, 200, 500, 1000, 2000, 1],
    },
    ("low_carbon_energy", "annual_change"): {
        "baseColorScheme": "PuOr",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -100, -30, -10, 0, 10, 30, 100, 1],
    },
    ("low_carbon_energy", "per_capita"): {
        "baseColorScheme": "Purples",
        "binningStrategy": "manual",
        "customNumericValues": [0, 300, 1000, 3000, 10000, 30000, 100000, 1],
    },
    ("low_carbon_energy", "share"): {
        "baseColorScheme": "Greens",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60, 70, 1],
        "customNumericColors": [None, None, None],
    },
    ("low_carbon_energy", "total"): {
        "baseColorScheme": "GnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 30, 100, 300, 1000, 3000, 1],
    },
    ("nuclear", "annual_change"): {
        "baseColorScheme": "RdYlBu",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -50, -20, -10, 0, 10, 20, 50, 1],
        "customNumericColors": [None],
    },
    ("nuclear", "per_capita"): {
        "baseColorScheme": "PuBuGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 100, 200, 500, 1000, 2000, 5000, 10000, 1],
    },
    ("nuclear", "share"): {
        "baseColorScheme": "BuPu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 5, 10, 15, 20, 25, 30, 35],
        "customNumericColors": [None],
    },
    ("nuclear", "total"): {
        "baseColorScheme": "BuPu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 30, 100, 300, 1000, 1],
        "customNumericColors": [None, None, None],
    },
    ("oil", "annual_change"): {
        "baseColorScheme": "PRGn",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -100, -50, -20, -10, 0, 10, 20, 50, 100, 1],
        "customNumericColors": [None, None, None],
        "colorSchemeInvert": True,
    },
    ("oil", "per_capita"): {
        "baseColorScheme": "OrRd",
        "binningStrategy": "manual",
        "customNumericValues": [0, 1000, 3000, 10000, 30000, 100000, 1],
        "customNumericColors": [None],
    },
    ("oil", "share"): {
        "baseColorScheme": "YlOrRd",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
    },
    ("renewables", "annual_change"): {
        "baseColorScheme": "RdBu",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -50, -20, -10, 0, 10, 20, 50, 100, 200],
        "customNumericColors": [None, None, None],
    },
    ("renewables", "per_capita"): {
        "baseColorScheme": "YlGnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 1000, 2000, 5000, 10000, 20000, 50000, 100000, 1],
    },
    ("renewables", "share"): {
        "baseColorScheme": "Greens",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60, 70, 1],
        "customNumericColors": [None, None],
    },
    ("renewables", "total"): {
        "baseColorScheme": "YlGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 100, 200, 500, 1000, 2000, 5000, 1],
    },
    ("solar", "annual_change"): {
        "baseColorScheme": "RdBu",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -30, -10, -3, -1, 0, 1, 3, 10, 30, 1],
        "customNumericColors": [None, None, None, None],
    },
    ("solar", "per_capita"): {
        "baseColorScheme": "PuBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 30, 100, 300, 1000, 3000, 1],
    },
    ("solar", "share"): {
        "baseColorScheme": "YlOrRd",
        "binningStrategy": "manual",
        "customNumericValues": [0, 1, 2, 3, 4, 5, 6, 1],
    },
    ("solar", "total"): {
        "baseColorScheme": "YlOrRd",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 50, 100, 200, 500, 1000, 1],
        "customNumericColors": [None],
    },
    ("solar_and_wind", "annual_change"): {
        "baseColorScheme": "RdBu",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -30, -10, -3, 0, 3, 10, 30, 1],
        "customNumericColors": [None],
    },
    ("solar_and_wind", "per_capita"): {
        "baseColorScheme": "BuGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 30, 100, 300, 1000, 3000, 1],
    },
    ("solar_and_wind", "share"): {
        "baseColorScheme": "BuGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 5, 10, 15, 20, 25, 1],
    },
    ("solar_and_wind", "total"): {
        "baseColorScheme": "YlOrRd",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 50, 100, 200, 500, 1000, 2000, 1],
        "customNumericColors": [None],
    },
    ("total", "annual_change"): {
        "baseColorScheme": "BrBG",
        "binningStrategy": "manual",
        "customNumericValues": [-300, -100, -30, -10, 0, 10, 30, 100, 300],
        "customNumericColors": [None],
        "colorSchemeInvert": True,
    },
    ("total", "per_capita"): {
        "baseColorScheme": "OrRd",
        "binningStrategy": "manual",
        "customNumericValues": [0, 1000, 3000, 10000, 30000, 100000, 300000],
        "customNumericColors": [None, None, None, None],
        "customNumericLabels": ["", "", "", "", "", "", "", "", "", "", "", ""],
    },
    ("total", "total"): {
        "baseColorScheme": "YlGnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 500, 1000, 2000, 5000, 10000, 20000, 1],
    },
    ("wind", "annual_change"): {
        "baseColorScheme": "PRGn",
        "binningStrategy": "manual",
        "customNumericValues": [-30, -10, -3, -1, 0, 1, 3, 10, 30],
    },
    ("wind", "per_capita"): {
        "baseColorScheme": "YlGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 100, 200, 500, 1000, 2000, 5000, 1],
    },
    ("wind", "share"): {
        "baseColorScheme": "PuBuGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 5, 10, 15, 20, 1],
        "customNumericColors": [None],
    },
    ("wind", "total"): {
        "baseColorScheme": "PuBuGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 20, 50, 100, 200, 500, 1000, 1],
    },
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


def _share_thresholds(vmax: float | None) -> tuple[list[float], bool] | None:
    """Bin edges for share-of-total metrics (bounded 0-100), following the original charts.

    Sources that span the full range get decade bins 0..100, closed at both ends (as in
    fossil-fuels-share-energy). Sources that never come near 100% get a scale capped at the step
    above the 99th percentile with an open-ended top bracket (as in renewable-share-energy), with
    the step chosen so the scale keeps roughly 5-10 bins. Returns (edges, open_top).
    """
    if vmax is None or not (vmax > 0):
        return None
    for step in (10, 5, 2, 1, 0.5, 0.2, 0.1):
        if math.ceil(vmax / step) >= 5:
            break
    top = min(100, math.ceil(vmax / step) * step)
    edges = [round(i * step, 3) for i in range(int(top / step) + 1)]
    return edges, top < 100


def _diverging_thresholds(vabs: float | None, levels: int = 4) -> list[float] | None:
    """Symmetric 1-3-10 bin edges around zero for a diverging metric (annual change), open both ends.

    A leading value greater than the most-negative edge and a trailing value smaller than the
    most-positive edge make grapher render open-ended "<" and ">" brackets at both ends, so a source
    that mostly grows still shows an open bracket for the rare (real) declines, matching the original
    annual-change charts.
    """
    if vabs is None or not (vabs > 0):
        return None
    ladder = [m * 10**p for p in range(-3, 13) for m in (1, 3)]
    below = [v for v in ladder if v <= vabs]
    if not below:
        return None
    pos = [round(v, 6) for v in below[-levels:]]
    edges = [-v for v in reversed(pos)] + [0] + pos
    # Sentinels (the innermost non-zero edges) at the array ends force open brackets on both sides.
    return [-pos[0]] + edges + [pos[0]]


def _map_config(source: str, metric: str, vmax: float | None = None) -> dict:
    # timeTolerance fills the newest map year for countries whose latest data is a year or two old
    # (e.g. EIA-extended countries end in 2024 while the Statistical Review reaches 2025).
    scheme = ORIGINAL_MAP_SCHEMES.get((source, metric))
    if scheme is not None and len(scheme.get("customNumericValues", [])) >= 3:
        # The original chart had explicit manual brackets: reproduce them verbatim, so the brackets,
        # color scheme, open-ended bins, and special bins all match the maps users already know. Only
        # views with no pre-existing chart get auto-sized bins.
        color_scale = dict(scheme)
        color_scale.setdefault("binningStrategy", "manual")
        return {"colorScale": color_scale, "timeTolerance": 3}
    if scheme is None:
        if metric == "annual_change":
            scheme = {"baseColorScheme": "BrBG", "colorSchemeInvert": True}
        else:
            scheme = {"baseColorScheme": SOURCE_FALLBACK_SCHEME.get(source, "YlGnBu")}
    color_scale = dict(scheme)
    if metric == "share":
        # Bounded metric: never leave it to grapher's automatic (ckmeans) binning, which invents
        # arbitrary data-driven brackets and an open top on a 0-100 scale.
        thresholds = _share_thresholds(vmax)
        if thresholds:
            edges, open_top = thresholds
            color_scale["binningStrategy"] = "manual"
            # The trailing sentinel (smaller than the top edge) makes the top bracket open-ended.
            color_scale["customNumericValues"] = edges + ([edges[1] / 100] if open_top else [])
    elif metric == "annual_change":
        # Diverging metric: symmetric decade bins open on both ends, so the lower bracket is open
        # even for sources that almost always grow (e.g. solar).
        edges = _diverging_thresholds(vmax)
        if edges:
            color_scale["binningStrategy"] = "manual"
            color_scale["customNumericValues"] = edges
    elif metric in LOG_METRICS:
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
