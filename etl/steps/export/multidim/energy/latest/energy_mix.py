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
    "_annual_change_pct": "annual_change_pct",
}
# Diverging metrics whose map bins are symmetric around zero (sized from the 99th percentile of the
# absolute change): the absolute year-on-year change and the percentage year-on-year change.
DIVERGING_METRICS = {"annual_change", "annual_change_pct"}


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
        series = series.abs() if dims["metric"] in DIVERGING_METRICS else series
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
        # "Primary energy use" (not bare "Primary energy"), so the quantity reads as a complete noun
        # phrase. The precise term "total energy supply" stays DoD'd in the subtitle. Source-specific
        # titles below keep "primary energy from {source}", where "from ..." already completes the phrase.
        return {
            "total": "Primary energy use",
            "per_capita": "Primary energy use per person",
            "share": "Primary energy use",
            "annual_change": "Annual change in primary energy use",
            "annual_change_pct": "Annual percentage change in primary energy use",
        }[metric]
    name = SOURCE_TITLE_NAMES[source]
    return {
        "total": f"Primary energy from {name}",
        "per_capita": f"Primary energy from {name} per person",
        "share": f"Share of primary energy from {name}",
        "annual_change": f"Annual change in primary energy from {name}",
        "annual_change_pct": f"Annual percentage change in primary energy from {name}",
    }[metric]


# Unit phrase per metric, and composition notes for composite sources. The original charts spelled out
# what each grouping contains (e.g. which renewables are included); we keep those notes but drop the
# "primary energy / substitution method" wording, which no longer applies to Total Energy Supply.
METRIC_UNIT_PHRASE = {
    "total": "Measured in [terawatt-hours](#dod:watt-hours) of [total energy supply](#dod:total-energy-supply).",
    "per_capita": "Measured in [kilowatt-hours](#dod:watt-hours) of [total energy supply](#dod:total-energy-supply) per person.",
    "share": "Measured as a percentage of [total energy supply](#dod:total-energy-supply).",
    "annual_change": "Change in [total energy supply](#dod:total-energy-supply) relative to the previous year, measured in [terawatt-hours](#dod:watt-hours).",
    "annual_change_pct": "Percentage change in [total energy supply](#dod:total-energy-supply) relative to the previous year.",
}
SOURCE_COMPOSITION = {
    "fossil_fuels": "Fossil fuels are the sum of coal, oil, and gas.",
    "renewables": "Renewables include hydropower, solar, wind, geothermal, bioenergy, and waste, but not traditional biomass.",
    "low_carbon_energy": "Low-carbon energy is the sum of nuclear and renewables.",
    # No entry for "other_renewables": its composition is carried as a footnote (OTHER_RENEWABLES_NOTE),
    # not inline in the subtitle, wherever "Other renewables" appears (standalone views and by-source stacks).
    # No entry for "solar_and_wind": the title already says "solar and wind", so a composition note
    # ("Combined energy supply from solar and wind") would just restate it.
}


def _view_subtitle(source: str, metric: str) -> str:
    # The subtitle carries the unit and nothing else. What a composite source is made of goes in the
    # footnote (see _view_note), for every source and metric alike: it is a definition the reader can
    # consult, not part of what the chart is showing.
    return METRIC_UNIT_PHRASE[metric]


def _view_note(source: str) -> str | None:
    """Footnote for a view: what the source is made of, then any caveat about its constituents.

    Both sentences can apply at once. Low-carbon energy is the sum of nuclear and renewables, and
    "renewables" itself then needs defining, so the two are joined rather than split across the
    subtitle and the footnote.
    """
    parts = [SOURCE_COMPOSITION.get(source), SOURCE_NOTES.get(source)]
    return " ".join(p for p in parts if p) or None


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
    "total": "Primary energy use",
    "fossil_fuels": "Fossil fuel supply",
    "renewables": "Renewable energy supply",
    "low_carbon_energy": "Low-carbon energy supply",
    "solar_and_wind": "Solar and wind supply",
}
# Footnotes for specific "by source" decomposition views.
# Footnote for the total primary-energy views: the Statistical Review covers commercially-traded
# energy, so traditional biomass is not part of the total. It gets its own biomass-inclusive chart.
TRADITIONAL_BIOMASS_NOTE = "Traditional biomass is not included."
_DECOMPOSITION_NOTES = {"renewables": TRADITIONAL_BIOMASS_NOTE, "total": TRADITIONAL_BIOMASS_NOTE}
# "Other renewables" needs a footnote spelling out its contents wherever it appears: the standalone
# other-renewables views (via SOURCE_NOTES below) and as a band in the by-source stacks (appended in
# add_decomposition_views). It is deliberately kept out of the subtitle. In the primary-energy (EI) data,
# biomass burned for power/heat sits inside "other renewables"; liquid biofuels are a separate source.
OTHER_RENEWABLES_NOTE = "Other renewables include geothermal, biomass, and waste."

# Footnotes for single-source views, keyed by source. The total views flag that traditional biomass is
# excluded; the low-carbon views spell out what "renewables" contains (its subtitle only says low-carbon
# energy is nuclear plus renewables); the other-renewables views spell out their contents.
SOURCE_NOTES = {
    "total": TRADITIONAL_BIOMASS_NOTE,
    "low_carbon_energy": "Renewables include hydropower, solar, wind, geothermal, bioenergy, and waste, but not traditional biomass.",
    "other_renewables": OTHER_RENEWABLES_NOTE,
}


def _decomposition_title(source: str, base_metric: str) -> str:
    stem = _DECOMPOSITION_STEM[source]
    return f"{stem} per person, by source" if base_metric == "per_capita" else f"{stem} by source"


# Fields that the views built here have to repeat, because they are assembled after
# create_collection and so do not inherit `definitions.common_views` from the config.yml.
# Keep in sync with that block.
COMMON_VIEW_EXTRAS = {
    "originUrl": "https://ourworldindata.org/energy",
    "relatedQuestions": [
        {
            "text": "Why has our energy data changed?",
            "url": "https://ourworldindata.org/how-primary-energy-is-measured-has-changed-across-our-charts",
        }
    ],
}

# Carbon intensity of energy (CO2 per unit of total energy supply) lives in the Global Carbon Budget,
# which already divides emissions by this same TES. Referenced by short path, expanded via the dep.
CARBON_INTENSITY_INDICATOR = "carbon_intensity_of_energy#emissions_total_per_unit_energy"
# Explicit brackets, because this is the one map-bearing view that would otherwise fall back to
# grapher's automatic (ckmeans) binning. That starts the lowest bin at the data minimum and leaves it
# open below, so a quantity whose floor is zero got a "<150 g" bracket that hid the ~25 countries
# under 150 g, 8 of them under 100 g. Even 50 g steps from a zero floor, open above: log spacing would
# be wrong here, because carbon intensity is a ratio clustered in a narrow band (nearly every country
# sits between 100 and 350 g) rather than a heavy-tailed magnitude.
CARBON_INTENSITY_MAP_EDGES = [0, 50, 100, 150, 200, 250, 300, 350]


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
        "map": {
            "colorScale": {
                "baseColorScheme": "YlOrBr",
                "binningStrategy": "manual",
                "customNumericValues": CARBON_INTENSITY_MAP_EDGES + [_open_top_sentinel(CARBON_INTENSITY_MAP_EDGES)],
            },
            "timeTolerance": 3,
        },
        **COMMON_VIEW_EXTRAS,
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
    base_config = {
        "chartTypes": ["StackedArea"],
        "tab": "chart",
        "hasMapTab": False,
        "hideRelativeToggle": False,
        **COMMON_VIEW_EXTRAS,
    }
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
                "subtitle": METRIC_UNIT_PHRASE[base_metric],
            }
            # Footnote: spell out "Other renewables" wherever it is a band, plus any aggregate-level note
            # (e.g. traditional biomass is excluded from the total and renewables stacks).
            notes = []
            if "other_renewables" in constituents:
                notes.append(OTHER_RENEWABLES_NOTE)
            if source in _DECOMPOSITION_NOTES:
                notes.append(_DECOMPOSITION_NOTES[source])
            if notes:
                config["note"] = " ".join(notes)
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
        # TES counts hydro at face value (~2.5x lower than the substitution method), so the top of the
        # range is now China at ~1400 TWh; a 1-3-10 scale with an open ">1000" top avoids empty bins.
        "customNumericValues": [0, 10, 30, 100, 300, 1000, 1],
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
        # Leading [0, 0] "No nuclear" grey bin, as on the nuclear share map (see ("nuclear", "share")).
        "customNumericValues": [0, 0, 100, 200, 500, 1000, 2000, 5000, 10000, 1],
        "customNumericColors": ["#dedede", None, None, None, None, None, None, None, None],
        "customNumericLabels": ["No nuclear", "", None, None, None, None, None, None, None],
        "customNumericColorsActive": True,
    },
    ("nuclear", "share"): {
        "baseColorScheme": "BuPu",
        "binningStrategy": "manual",
        # Leading [0, 0] bin: countries with no nuclear read as a distinct grey "No nuclear", not the
        # lowest shade (which would imply a sliver of nuclear). Mirrors the electricity nuclear-share map.
        "customNumericValues": [0, 0, 5, 10, 15, 20, 25, 30, 35],
        "customNumericColors": ["#dedede", None, None, None, None, None, None, None],
        "customNumericLabels": ["No nuclear", "", None, None, None, None, None, None],
        "customNumericColorsActive": True,
    },
    ("nuclear", "total"): {
        "baseColorScheme": "BuPu",
        "binningStrategy": "manual",
        # Leading [0, 0] "No nuclear" grey bin, as on the nuclear share map (see ("nuclear", "share")).
        "customNumericValues": [0, 0, 10, 30, 100, 300, 1000, 1],
        "customNumericColors": ["#dedede", None, None, None],
        "customNumericLabels": ["No nuclear", "", None, None, None, None, None],
        "customNumericColorsActive": True,
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
        # TES counts renewables at face value (lower than the substitution method), so the top of the
        # range is now China at ~4100 TWh; a 1-3-10 scale with an open ">3000" top avoids empty bins.
        "customNumericValues": [0, 10, 30, 100, 300, 1000, 3000, 1],
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
        "customNumericValues": [0, 2, 4, 6, 8, 10, 12, 1],
        "customNumericColors": [None],
    },
    ("wind", "total"): {
        "baseColorScheme": "PuBuGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 20, 50, 100, 200, 500, 1000, 1],
    },
    ("coal", "annual_change_pct"): {
        "baseColorScheme": "BrBG",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -50, -20, -10, 0, 10, 20, 50, 1],
        "colorSchemeInvert": True,
    },
    ("fossil_fuels", "annual_change_pct"): {
        "baseColorScheme": "BrBG",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -10, -5, 0, 5, 10, 1],
        "customNumericColors": [None, None],
        "colorSchemeInvert": True,
    },
    ("gas", "annual_change_pct"): {
        "baseColorScheme": "PiYG",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -50, -20, -10, 0, 10, 20, 50, 1],
        "customNumericColors": [None, None],
        "colorSchemeInvert": True,
    },
    ("hydro", "annual_change_pct"): {
        "baseColorScheme": "PuOr",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -50, -20, -10, 0, 10, 20, 50, 1],
        "customNumericColors": [None],
    },
    ("low_carbon_energy", "annual_change_pct"): {
        "baseColorScheme": "RdBu",
        "binningStrategy": "manual",
        "customNumericValues": [-50, -15, -10, -5, 0, 5, 10, 15, 50],
        "customNumericColors": [None],
    },
    ("nuclear", "annual_change_pct"): {
        "baseColorScheme": "RdBu",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -50, -20, -10, 0, 10, 20, 50, 1],
        "customNumericColors": [None, None],
    },
    ("renewables", "annual_change_pct"): {
        "baseColorScheme": "RdBu",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -20, -15, -10, -5, 0, 5, 10, 15, 20, 1],
        "customNumericColors": [None, None],
    },
    ("solar", "annual_change_pct"): {
        "baseColorScheme": "PuOr",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -50, -20, -10, 0, 10, 20, 50, 100],
    },
    ("solar_and_wind", "annual_change_pct"): {
        "baseColorScheme": "RdYlBu",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -50, -20, -10, 0, 10, 20, 50, 100],
        "customNumericColors": [None, None, None],
    },
    ("wind", "annual_change_pct"): {
        "baseColorScheme": "PuOr",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -50, -20, -10, 0, 10, 20, 50, 1],
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


def _open_top_sentinel(edges: list[float]) -> float:
    """Value to append after the bin edges so grapher renders an open-ended top bracket.

    Grapher builds bins from consecutive pairs of `customNumericValues`, flags the last bin
    as open-ended when its max sits below the data max, and tests membership against the
    bin's min. So appending any value below the top edge yields a ">top edge" bracket. The
    sentinel is derived from the edges rather than hard-coded, because a fixed value (e.g. 1)
    stops being below the top edge on views whose values never reach it.
    """
    return edges[1] / 100


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
        if metric in DIVERGING_METRICS:
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
    elif metric in DIVERGING_METRICS:
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
            color_scale["customNumericValues"] = edges + [_open_top_sentinel(edges)]
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
        note = _view_note(source)
        if note:
            config["note"] = note
        v.config = config
