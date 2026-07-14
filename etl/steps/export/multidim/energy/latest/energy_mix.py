"""Multidim for the energy mix (source x metric), based on Total Energy Supply."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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
    for column, dims in column_dimensions.items():
        tb[column].m.dimensions = dims
        tb[column].m.original_short_name = "energy"

    common_view_config = {
        "hasMapTab": True,
        "tab": "map",
        "chartTypes": ["LineChart"],
    }

    c = paths.create_collection(
        config=paths.load_collection_config(),
        tb=tb,
        indicator_names=["energy"],
        dimensions=["source", "metric"],
        common_view_config=common_view_config,
    )

    # Add stacked breakdown views: all individual sources, and the fossil/nuclear/renewables split.
    stacked_view_config = {
        "chartTypes": ["StackedArea"],
        "tab": "chart",
        "hasMapTab": False,
        "title": "{title}",
        "subtitle": "{subtitle}",
    }
    metric_titles = {
        "total": "Total energy supply by source",
        "per_capita": "Energy supply per person, by source",
        "share": "Share of total energy supply, by source",
        "annual_change": "Annual change in energy supply, by source",
    }
    # NOTE: choices are listed top-to-bottom because grapher's StackedArea renders the first series at
    # the top. Listing the smallest source first puts it at the top and the largest at the bottom,
    # matching the original charts (e.g. coal at the bottom, other renewables at the top).
    c.group_views(
        groups=[
            {
                "dimension": "source",
                "choices": [
                    "other_renewables",
                    "biofuels",
                    "solar",
                    "wind",
                    "hydro",
                    "nuclear",
                    "gas",
                    "oil",
                    "coal",
                ],
                "choice_new_slug": "all_sources",
                "view_config": stacked_view_config,
            },
            {
                "dimension": "source",
                "choices": ["renewables", "nuclear", "fossil_fuels"],
                "choice_new_slug": "fossil_nuclear_renewables",
                "view_config": stacked_view_config,
            },
        ],
        params={
            "title": lambda view: metric_titles[view.dimensions["metric"]],
            "subtitle": _grouped_subtitle,
        },
    )
    # Stacked areas of year-on-year changes are unreadable; keep breakdowns only for level metrics.
    c.views = [
        v
        for v in c.views
        if not (
            v.dimensions["source"] in ("all_sources", "fossil_nuclear_renewables")
            and v.dimensions["metric"] == "annual_change"
        )
    ]

    # Set an explicit title on every single-source view. Otherwise grapher falls back to the
    # indicator's display name (e.g. the annual-change indicators inherit "Total energy supply"),
    # which mislabels the view. Grouped (stacked) views already carry a title from group_views.
    set_view_titles(c)

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
    "total": "Measured in [terawatt-hours](#dod:watt-hours).",
    "per_capita": "Measured in [kilowatt-hours](#dod:watt-hours) per person.",
    "share": "Measured as a percentage of total energy supply.",
    "annual_change": "Year-on-year change in energy supply, measured in [terawatt-hours](#dod:watt-hours).",
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


def _grouped_subtitle(view) -> str:
    """Subtitle for the stacked breakdown views."""
    metric = view.dimensions["metric"]
    unit = METRIC_UNIT_PHRASE[metric]
    if view.dimensions["source"] == "fossil_nuclear_renewables":
        return (
            f"{unit} Fossil fuels are coal, oil, and gas; renewables include hydropower, solar, wind, "
            "geothermal, wave and tidal, and bioenergy."
        )
    return unit


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


def _map_config(source: str, metric: str) -> dict:
    # timeTolerance fills the newest map year for countries whose latest data is a year or two old
    # (e.g. EIA-extended countries end in 2024 while the Statistical Review reaches 2025).
    scheme = ORIGINAL_MAP_SCHEMES.get((source, metric))
    if scheme is None:
        if metric == "annual_change":
            scheme = {"baseColorScheme": "BrBG", "colorSchemeInvert": True}
        else:
            scheme = {"baseColorScheme": SOURCE_FALLBACK_SCHEME.get(source, "YlGnBu")}
    return {"colorScale": scheme, "timeTolerance": 3}


def set_view_titles(c) -> None:
    for v in c.views:
        source = v.dimensions["source"]
        if source not in SOURCE_TITLE_NAMES:
            # Grouped/stacked views already have a title from group_views.
            continue
        metric = v.dimensions["metric"]
        config = dict(v.config or {})
        config["title"] = _view_title(source, metric)
        config["subtitle"] = _view_subtitle(source, metric)
        config["map"] = _map_config(source, metric)
        v.config = config
