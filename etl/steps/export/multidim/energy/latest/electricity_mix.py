"""Multidim for the electricity mix (source x metric, plus total-only metrics)."""

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
    # Standalone views use the clean excluding-bioenergy series (geo/wave/tidal only; starts where the
    # split exists, no pre-2000 cliff). The stacked "by source" views instead swap in the gap-filled
    # coalesced column (other_renewables_generation) to keep full history — see add_decomposition_views.
    "other_renewables": "other_renewables_excluding_bioenergy",
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

    #
    # Process data.
    #
    # Build the source x metric grid for both frequencies: annual (from the combined electricity_mix
    # table) and monthly (from the Ember-only, date-indexed electricity_mix_monthly table).
    dims_max = {}
    tables = []
    common_view_configs = []
    for frequency, table_name in [("annual", "electricity_mix"), ("monthly", "electricity_mix_monthly")]:
        tb = ds.read(table_name, reset_index=False)
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
                    column_dimensions[column] = {"source": source_slug, "metric": metric_slug, "frequency": frequency}
        # Total-only metrics (demand, imports, carbon intensity).
        for metric_slug, column in TOTAL_ONLY_METRICS.items():
            if column in tb.columns:
                column_dimensions[column] = {"source": "total", "metric": metric_slug, "frequency": frequency}

        tb = tb[list(column_dimensions)]
        # Reference magnitude per (source, metric, frequency) for sizing the map's bins: the 99th
        # percentile across countries only (aggregates excluded, single outliers ignored). Monthly values
        # are ~1/12 of the annual ones, so they get their own scale. See set_view_titles / _map_config.
        country_level = tb.index.get_level_values("country")
        is_country = ~(country_level.isin(AGGREGATE_ENTITIES) | country_level.str.contains("(EI)", regex=False))
        tb_countries = tb[is_country]
        for column, dims in column_dimensions.items():
            dims_max[(dims["source"], dims["metric"], frequency)] = float(
                tb_countries[column].astype("float64").quantile(0.99)
            )
            tb[column].m.dimensions = dims
            tb[column].m.original_short_name = "electricity"

        tables.append(tb)
        # Annual defaults to the map tab; monthly defaults to the line chart (a single-month map is less
        # useful), but both keep the map tab available so the time slider can scrub through the map.
        common_view_configs.append(
            {
                "hasMapTab": True,
                "tab": "map" if frequency == "annual" else "chart",
                # Slope and dumbbell charts let users compare a metric across countries between two time
                # points (e.g. coal's share in 2000 vs 2025). All Group-1 (non-stacked) chart types, so
                # they can coexist; not added to the by-source stacks, which are Group-2 (stacked) only.
                "chartTypes": ["LineChart", "DiscreteBar", "Dumbbell", "SlopeChart"],
            }
        )

    c = paths.create_collection(
        config=paths.load_collection_config(),
        tb=tables,
        indicator_names=["electricity"],
        dimensions=["source", "metric", "frequency"],
        common_view_config=common_view_configs,
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
    "carbon_intensity": "Lifecycle carbon intensity of electricity",
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
# Footnote spelling out what "Other renewables" contains, required wherever it appears (kept out of the
# subtitle). The standalone other-renewables views use the clean excluding-bioenergy column
# (geothermal/wave/tidal only), so they get the plain note. The stacked "by source" views use the coalesced
# column, which lumps pre-2000 bioenergy into "other renewables" for countries that can't be split that far
# back, so they get the extended note with the caveat.
OTHER_RENEWABLES_NOTE = "Other renewables include geothermal, wave, and tidal."
OTHER_RENEWABLES_STACKED_NOTE = (
    "Other renewables include geothermal, wave, and tidal; bioenergy may be included prior to 2000 due to "
    "limited data availability."
)
SOURCE_COMPOSITION = {
    "fossil": "Fossil fuels include coal, oil, and gas.",
    "renewables": "Renewables include solar, wind, hydropower, bioenergy, geothermal, wave, and tidal.",
    "low_carbon": "Low-carbon sources are the sum of nuclear and renewables.",
    # No entry for "other_renewables": its composition is carried as a footnote (OTHER_RENEWABLES_NOTE),
    # not inline in the subtitle.
    # No entry for "solar_and_wind": the title already says "solar and wind", so a composition note
    # ("Combined electricity generation from solar and wind") would just restate it.
    "wind": "Includes both onshore and offshore wind.",
}

# Footnotes for standalone single-source views, keyed by source.
SOURCE_NOTES = {
    "other_renewables": OTHER_RENEWABLES_NOTE,
}
# Total-only metrics have a single fixed subtitle (they only exist for source "total").
TOTAL_ONLY_SUBTITLES = {
    "demand": "Total electricity generation, adjusted for imports and exports. Measured in [terawatt-hours](#dod:watt-hours).",
    "demand_per_capita": "Electricity generation adjusted for imports and exports. Measured in [kilowatt-hours](#dod:watt-hours) per person.",
    "net_imports": "Net electricity imports are calculated as electricity imports minus exports. Countries with positive values are net importers of electricity; negative values are net exporters. Measured in [terawatt-hours](#dod:watt-hours).",
    "imports_share": "Net electricity imports are calculated as electricity imports minus exports. This is given as a share of a country's electricity demand. Countries with positive values are net importers of electricity; negative values are net exporters.",
    "carbon_intensity": "Measured in grams of [carbon dioxide-equivalents](#dod:carbondioxideequivalents) emitted per [kilowatt-hour](#dod:watt-hours) of electricity generated. Emissions are estimated on a lifecycle basis, including upstream, supply chain and manufacturing stages, and cover all greenhouse gases.",
}


def _view_subtitle(source: str, metric: str) -> str:
    if metric in TOTAL_ONLY_SUBTITLES:
        return TOTAL_ONLY_SUBTITLES[metric]
    unit = METRIC_UNIT_PHRASE[metric]
    note = SOURCE_COMPOSITION.get(source)
    # Lead with what the series is (the composition note), then the unit, so the subtitle reads as
    # "Fossil fuels include coal, oil, and gas. Measured as a percentage of..." rather than the reverse.
    return f"{note} {unit}" if note else unit


# Aggregates that can be decomposed "by source", mapped to their constituent individual sources.
# Constituents are listed top-to-bottom for the stacked chart (grapher renders the first series at the
# top). "other_renewables" (geo/wave/tidal) and "bioenergy" are separate bands; both use gap-filled helper
# columns so the stack keeps full history (see add_decomposition_views and the garden helper columns).
AGGREGATE_DECOMPOSITION = {
    "total": ["other_renewables", "bioenergy", "solar", "wind", "hydro", "nuclear", "gas", "oil", "coal"],
    "fossil": ["gas", "oil", "coal"],
    "renewables": ["other_renewables", "bioenergy", "solar", "wind", "hydro"],
    "low_carbon": ["other_renewables", "bioenergy", "solar", "wind", "hydro", "nuclear"],
    "solar_and_wind": ["solar", "wind"],
}
# Canonical OWID per-source colors, copied from the original "Electricity production by source" chart so
# the stacked "by source" views keep the colors users know (hydropower blue, coal dark red, etc.).
SOURCE_COLORS = {
    "coal": "#883039",
    "oil": "#c15065",
    "gas": "#6d3e91",
    "nuclear": "#00847e",
    "hydro": "#286bbb",
    "solar": "#e56e5a",
    "wind": "#00295b",
    "bioenergy": "#bc8e5a",
    "other_renewables": "#578145",
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
    base_config = {
        "chartTypes": ["StackedArea"],
        "tab": "chart",
        "hasMapTab": False,
        "hideRelativeToggle": False,
        "originUrl": "https://ourworldindata.org/electricity-mix",
    }
    single_views = {
        (v.dimensions.get("source"), v.dimensions.get("metric"), v.dimensions.get("frequency")): v for v in c.views
    }
    for frequency in ["annual", "monthly"]:
        for source, constituents in AGGREGATE_DECOMPOSITION.items():
            for base_metric, new_metric in _DECOMPOSITION_METRICS.items():
                indicators = []
                for constituent in constituents:
                    view = single_views.get((constituent, base_metric, frequency))
                    if view is not None and view.indicators.y:
                        for indicator in deepcopy(view.indicators.y):
                            # Swap the standalone (clean) columns for the gap-filled ones so the stack keeps
                            # full history; the clean columns stay on the standalone source views.
                            if constituent == "bioenergy":
                                indicator.catalogPath = indicator.catalogPath.replace(
                                    "bioenergy_generation", "bioenergy_stacked_generation"
                                )
                            elif constituent == "other_renewables":
                                indicator.catalogPath = indicator.catalogPath.replace(
                                    "other_renewables_excluding_bioenergy_generation", "other_renewables_generation"
                                )
                            color = SOURCE_COLORS.get(constituent)
                            if color:
                                indicator.update_display({"color": color})
                            indicators.append(indicator)
                if not indicators:
                    continue
                config = {
                    **base_config,
                    "title": _with_frequency(_decomposition_title(source, base_metric), frequency),
                    "subtitle": METRIC_UNIT_PHRASE[base_metric],
                }
                # The stacked "other renewables" band uses the coalesced column, so it needs the extended
                # footnote flagging that pre-2000 bioenergy may be lumped in.
                if "other_renewables" in constituents:
                    config["note"] = OTHER_RENEWABLES_STACKED_NOTE
                new_view = View(
                    dimensions={"source": source, "metric": new_metric, "frequency": frequency},
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
    ("coal", "generation"): {
        "baseColorScheme": "Oranges",
        "binningStrategy": "manual",
        "customNumericValues": [0, 20, 50, 100, 200, 500, 1000, 2000, 5000, 1],
        "customNumericColors": [None],
    },
    ("coal", "per_capita"): {
        "baseColorScheme": "YlOrBr",
        "binningStrategy": "manual",
        "customNumericValues": [0, 100, 200, 500, 1000, 2000, 5000, 1],
    },
    ("coal", "share_of_generation"): {
        "baseColorScheme": "Oranges",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        "customNumericColors": [None, None, None],
    },
    ("fossil", "generation"): {
        "baseColorScheme": "YlOrBr",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 1],
        "customNumericColors": [None, None, None, None, None],
    },
    ("fossil", "per_capita"): {
        "baseColorScheme": "Oranges",
        "binningStrategy": "manual",
        "customNumericValues": [0, 100, 200, 500, 1000, 2000, 5000, 10000, 1],
        "customNumericColors": [None, None, None],
    },
    ("fossil", "share_of_generation"): {
        "baseColorScheme": "OrRd",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100.001],
        "customNumericColors": [None, None, None, None, None],
        "customNumericLabels": [None, None, None, None, None, None, None, None, "", ""],
    },
    ("gas", "generation"): {
        "baseColorScheme": "Purples",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 30, 100, 300, 1000, 1],
    },
    ("gas", "per_capita"): {
        "baseColorScheme": "BuPu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 100, 200, 500, 1000, 2000, 5000, 10000, 1],
        "customNumericColors": [None],
    },
    ("gas", "share_of_generation"): {
        "baseColorScheme": "GnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100.001],
    },
    ("hydro", "generation"): {
        "baseColorScheme": "GnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 50, 100, 200, 500, 1000, 1],
    },
    ("hydro", "per_capita"): {
        "baseColorScheme": "GnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 1],
        "customNumericColors": [None],
    },
    ("hydro", "share_of_generation"): {
        "baseColorScheme": "PuBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100.00001],
    },
    ("low_carbon", "generation"): {
        "baseColorScheme": "BuGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 20, 50, 100, 200, 500, 1000, 2000, 1],
        "customNumericColors": [None],
    },
    ("low_carbon", "per_capita"): {
        "binningStrategy": "manual",
        "customNumericValues": [0, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 1],
        "customNumericColors": [None],
    },
    ("low_carbon", "share_of_generation"): {
        "baseColorScheme": "YlGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100.001],
    },
    ("nuclear", "generation"): {
        "baseColorScheme": "GnBu",
        "binningStrategy": "manual",
        # Leading [0, 0] "No nuclear" grey bin, as on the nuclear share map (see ("nuclear", "share_of_generation")).
        "customNumericValues": [0, 0, 10, 20, 50, 100, 200, 500, 1],
        "customNumericColors": ["#dedede", None, None, None, None, None, None, None],
        "customNumericLabels": ["No nuclear", "", None, None, None, None, None, None],
        "customNumericColorsActive": True,
    },
    ("nuclear", "per_capita"): {
        "baseColorScheme": "PuBuGn",
        "binningStrategy": "manual",
        # Leading [0, 0] "No nuclear" grey bin, as on the nuclear share map (see ("nuclear", "share_of_generation")).
        "customNumericValues": [0, 0, 100, 200, 500, 1000, 2000, 5000, 1],
        "customNumericColors": ["#dedede", None, None, None, None, None, None, None],
        "customNumericLabels": ["No nuclear", "", None, None, None, None, None, None],
        "customNumericColorsActive": True,
    },
    ("nuclear", "share_of_generation"): {
        "baseColorScheme": "YlGnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 0, 10, 20, 30, 40, 50, 60, 70],
        "customNumericColors": ["#dedede", None, None, None, None, None, None, None],
        "customNumericLabels": ["No nuclear", "", None, None, None, None, None, None],
        # Without this flag grapher stores the custom "#dedede" grey but does not apply it, so the
        # exact-zero "No nuclear" bin falls back to the pale color-scheme yellow. Required to render grey.
        "customNumericColorsActive": True,
    },
    ("oil", "generation"): {
        "baseColorScheme": "Reds",
        "binningStrategy": "manual",
        "customNumericValues": [0, 2, 5, 10, 20, 50, 100, 1],
    },
    ("oil", "per_capita"): {
        "baseColorScheme": "OrRd",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 30, 100, 300, 1000, 3000, 10000],
    },
    ("oil", "share_of_generation"): {
        "baseColorScheme": "Oranges",
        "binningStrategy": "manual",
        "customNumericValues": [0, 1, 2, 5, 10, 20, 50, 1],
        "customNumericColors": [None, None],
    },
    ("renewables", "generation"): {
        "baseColorScheme": "GnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 50, 100, 200, 500, 1000, 2000, 1],
    },
    ("renewables", "per_capita"): {
        "baseColorScheme": "YlGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000, 1],
    },
    ("renewables", "share_of_generation"): {
        "baseColorScheme": "BuGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100.0001],
        "customNumericColors": [None, None, None],
    },
    ("solar", "generation"): {
        "baseColorScheme": "YlOrRd",
        "binningStrategy": "manual",
        "customNumericValues": [0, 0.1, 0.3, 1, 3, 10, 30, 100, 300, 1],
        "customNumericColors": [None],
    },
    ("solar", "per_capita"): {
        "baseColorScheme": "YlOrRd",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 50, 100, 200, 500, 1000, 1],
    },
    ("solar", "share_of_generation"): {
        "baseColorScheme": "YlGnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 1, 2, 5, 10, 20, 1],
        "customNumericColors": [None],
    },
    ("solar_and_wind", "generation"): {
        "baseColorScheme": "YlOrRd",
        "binningStrategy": "manual",
        "customNumericValues": [0, 0.1, 0.3, 1, 3, 10, 30, 100, 300],
    },
    ("solar_and_wind", "per_capita"): {
        "baseColorScheme": "YlGnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 20, 50, 100, 200, 500, 1000, 2000, 1],
    },
    ("solar_and_wind", "share_of_generation"): {
        "baseColorScheme": "Greens",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 30, 40, 50, 60],
    },
    ("total", "carbon_intensity"): {
        "baseColorScheme": "YlOrBr",
        "binningStrategy": "manual",
        "customNumericValues": [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1],
        "customNumericColors": [None, None],
    },
    ("total", "demand"): {
        "baseColorScheme": "YlGnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 100, 200, 500, 1000, 2000, 5000, 1],
    },
    ("total", "demand_per_capita"): {
        "baseColorScheme": "PuBuGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 200, 500, 1000, 2000, 5000, 10000, 20000, 1],
    },
    ("total", "generation"): {
        "baseColorScheme": "YlGnBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 100, 200, 500, 1000, 2000, 5000, 1],
        "customNumericColors": [None],
    },
    ("total", "imports_share"): {
        "baseColorScheme": "RdBu",
        "binningStrategy": "manual",
        "customNumericValues": [-50, -20, -10, -5, 0, 5, 10, 20, 50],
    },
    ("total", "net_imports"): {
        "baseColorScheme": "RdBu",
        "binningStrategy": "manual",
        "customNumericValues": [-1, -40, -30, -20, -10, 0, 10, 20, 30, 40, 1],
        "customNumericColors": [None, None],
        "colorSchemeInvert": True,
    },
    ("total", "per_capita"): {
        "baseColorScheme": "PuBuGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 200, 500, 1000, 2000, 5000, 10000, 20000, 1],
        "customNumericLabels": ["", "", "", "", "", "", "", "", "", ""],
    },
    ("wind", "generation"): {
        "baseColorScheme": "PuBu",
        "binningStrategy": "manual",
        "customNumericValues": [0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1],
    },
    ("wind", "per_capita"): {
        "baseColorScheme": "PuBuGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 10, 20, 50, 100, 200, 500, 1000, 2000, 1],
        "customNumericColors": [None],
    },
    ("wind", "share_of_generation"): {
        "baseColorScheme": "PuBuGn",
        "binningStrategy": "manual",
        "customNumericValues": [0, 1, 2, 5, 10, 20, 50, 100],
        "customNumericColors": [None, None, None, None],
    },
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


def _share_thresholds(vmax: float | None) -> tuple[list[float], bool] | None:
    """Bin edges for share-of-total metrics (bounded 0-100), following the original charts.

    Sources that span the full range get decade bins 0..100, closed at both ends. Sources that never
    come near 100% get a scale capped at the step above the 99th percentile with an open-ended top
    bracket, with the step chosen so the scale keeps roughly 5-10 bins. Returns (edges, open_top).
    """
    if vmax is None or not (vmax > 0):
        return None
    for step in (10, 5, 2, 1, 0.5, 0.2, 0.1):
        if math.ceil(vmax / step) >= 5:
            break
    top = min(100, math.ceil(vmax / step) * step)
    edges = [round(i * step, 3) for i in range(int(top / step) + 1)]
    return edges, top < 100


def _map_config(source: str, metric: str, vmax: float | None = None, frequency: str = "annual") -> dict:
    # Annual reuses the hand-tuned brackets from the original charts; monthly values are ~1/12 the size,
    # so the monthly map is auto-binned from the monthly 99th percentile instead of the annual brackets.
    scheme = ORIGINAL_MAP_SCHEMES.get((source, metric)) if frequency == "annual" else None
    if scheme is not None and len(scheme.get("customNumericValues", [])) >= 3:
        # The original chart had explicit manual brackets: reproduce them verbatim, so the brackets,
        # color scheme, open-ended bins, and special bins (e.g. the nuclear "No nuclear" =0 bucket) all
        # match the maps users already know. Only views with no pre-existing chart get auto-sized bins.
        color_scale = dict(scheme)
        color_scale.setdefault("binningStrategy", "manual")
        return {"colorScale": color_scale, "timeTolerance": 3}
    if scheme is None:
        if metric in ("net_imports", "imports_share"):
            scheme = {"baseColorScheme": "RdBu", "colorSchemeInvert": True}
        elif metric == "carbon_intensity":
            scheme = {"baseColorScheme": "YlOrBr"}
        else:
            scheme = {"baseColorScheme": SOURCE_FALLBACK_SCHEME.get(source, "YlGnBu")}
    color_scale = dict(scheme)
    if metric == "share_of_generation":
        # Bounded metric: never leave it to grapher's automatic (ckmeans) binning, which invents
        # arbitrary data-driven brackets and an open top on a 0-100 scale.
        thresholds = _share_thresholds(vmax)
        if thresholds:
            edges, open_top = thresholds
            color_scale["binningStrategy"] = "manual"
            # The trailing sentinel (smaller than the top edge) makes the top bracket open-ended.
            color_scale["customNumericValues"] = edges + ([edges[1] / 100] if open_top else [])
    elif metric in LOG_METRICS:
        edges = _log_thresholds(vmax)
        if edges:
            color_scale["binningStrategy"] = "manual"
            # Trailing sentinel (smaller than the top edge) forces grapher to render an open-ended
            # top bracket (">X"), independent of where the top edge sits relative to the data max.
            color_scale["customNumericValues"] = edges + [1]
    return {"colorScale": color_scale, "timeTolerance": 3}


def _with_frequency(title: str, frequency: str) -> str:
    """Prefix monthly view titles with "Monthly " (lowercasing the original's first letter so it reads
    naturally). This keeps a downloaded monthly chart image from being mistaken for annual data, which
    would understate the magnitudes roughly twelvefold.
    """
    if frequency == "monthly":
        return "Monthly " + title[0].lower() + title[1:]
    return title


def set_view_titles(c, dims_max: dict) -> None:
    for v in c.views:
        source = v.dimensions["source"]
        if source not in SOURCE_TITLE_NAMES:
            # Grouped/stacked views already have a title from group_views.
            continue
        metric = v.dimensions["metric"]
        frequency = v.dimensions.get("frequency", "annual")
        config = dict(v.config or {})
        config["title"] = _with_frequency(_view_title(source, metric), frequency)
        config["subtitle"] = _view_subtitle(source, metric)
        config["map"] = _map_config(source, metric, dims_max.get((source, metric, frequency)), frequency)
        note = SOURCE_NOTES.get(source)
        if note:
            config["note"] = note
        v.config = config
