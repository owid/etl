"""Multidim for the electricity mix (source x metric, plus total-only metrics)."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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
    for column, dims in column_dimensions.items():
        tb[column].m.dimensions = dims
        tb[column].m.original_short_name = "electricity"

    common_view_config = {
        "hasMapTab": True,
        "tab": "map",
        "chartTypes": ["LineChart"],
    }

    c = paths.create_collection(
        config=paths.load_collection_config(),
        tb=tb,
        indicator_names=["electricity"],
        dimensions=["source", "metric"],
        common_view_config=common_view_config,
    )

    # Add stacked breakdown views: all individual sources, and the fossil/nuclear/renewables split.
    # NOTE: "other_renewables" includes bioenergy, so the standalone "bioenergy" choice is left out
    # of the breakdown to avoid double counting.
    stacked_view_config = {
        "chartTypes": ["StackedArea"],
        "tab": "chart",
        "hasMapTab": False,
        "title": "{title}",
    }
    metric_titles = {
        "generation": "Electricity generation by source",
        "per_capita": "Per capita electricity generation by source",
        "share_of_generation": "Share of electricity generation by source",
    }
    c.group_views(
        groups=[
            {
                "dimension": "source",
                "choices": ["coal", "oil", "gas", "nuclear", "hydro", "wind", "solar", "other_renewables"],
                "choice_new_slug": "all_sources",
                "view_config": stacked_view_config,
            },
            {
                "dimension": "source",
                "choices": ["fossil", "nuclear", "renewables"],
                "choice_new_slug": "fossil_nuclear_renewables",
                "view_config": stacked_view_config,
            },
        ],
        params={"title": lambda view: metric_titles[view.dimensions["metric"]]},
    )

    # Set an explicit title on every single-source view, so grapher does not fall back to the
    # indicator display name. Grouped (stacked) views already carry a title from group_views.
    set_view_titles(c)

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


# Map color scheme per source, so views are not all the same green. Signed metrics (net imports)
# use a diverging scheme; carbon intensity uses a "more is worse" red ramp.
SOURCE_MAP_SCHEME = {
    "total": "GnBu",
    "coal": "Greys",
    "oil": "Oranges",
    "gas": "OrRd",
    "fossil": "Greys",
    "nuclear": "Purples",
    "hydro": "Blues",
    "wind": "GnBu",
    "solar": "YlOrRd",
    "solar_and_wind": "BuGn",
    "renewables": "Greens",
    "other_renewables": "YlGn",
    "bioenergy": "YlGn",
    "low_carbon": "Greens",
}


def _map_config(source: str, metric: str) -> dict:
    if metric in ("net_imports", "imports_share"):
        return {"colorScale": {"baseColorScheme": "BrBG", "colorSchemeInvert": True}, "timeTolerance": 3}
    if metric == "carbon_intensity":
        return {"colorScale": {"baseColorScheme": "OrRd"}, "timeTolerance": 3}
    return {"colorScale": {"baseColorScheme": SOURCE_MAP_SCHEME.get(source, "GnBu")}, "timeTolerance": 3}


def set_view_titles(c) -> None:
    for v in c.views:
        source = v.dimensions["source"]
        if source not in SOURCE_TITLE_NAMES:
            # Grouped/stacked views already have a title from group_views.
            continue
        metric = v.dimensions["metric"]
        config = dict(v.config or {})
        config["title"] = _view_title(source, metric)
        config["map"] = _map_config(source, metric)
        v.config = config
