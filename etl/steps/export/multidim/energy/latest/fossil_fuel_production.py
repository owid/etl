"""Multidim for fossil fuel production (fuel x metric)."""

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

    # Add a stacked breakdown view with all three fuels.
    metric_titles = {
        "production": "Fossil fuel production by fuel",
        "per_capita": "Per capita fossil fuel production by fuel",
        "reserves_ratio": "Reserves-to-production ratio",
    }
    # NOTE: choices are listed top-to-bottom because grapher's StackedArea renders the first series at
    # the top, so listing coal last puts it at the bottom, matching the original charts.
    c.group_views(
        groups=[
            {
                "dimension": "fuel",
                "choices": ["gas", "oil", "coal"],
                "choice_new_slug": "all_fuels",
                "view_config": {
                    "chartTypes": ["StackedArea"],
                    "tab": "chart",
                    "hasMapTab": False,
                    "title": "{title}",
                    "subtitle": "{subtitle}",
                },
            },
        ],
        params={
            "title": lambda view: metric_titles[view.dimensions["metric"]],
            "subtitle": lambda view: METRIC_UNIT_PHRASE[view.dimensions["metric"]],
        },
    )
    # Stacking reserves-to-production ratios (years of production left) makes no sense; drop that combination.
    c.views = [
        v for v in c.views if not (v.dimensions["fuel"] == "all_fuels" and v.dimensions["metric"] == "reserves_ratio")
    ]

    # Set an explicit title on every single-fuel view, so grapher does not fall back to the
    # indicator display name. The grouped (stacked) view already carries a title from group_views.
    set_view_titles(c, dims_max)

    #
    # Save outputs.
    #
    c.save()


FUEL_TITLE_NAMES = {"coal": "coal", "oil": "oil", "gas": "gas"}

# Unit phrase per metric, used for both single-fuel and stacked views.
METRIC_UNIT_PHRASE = {
    "production": "Measured in [terawatt-hours](#dod:watt-hours).",
    "per_capita": "Measured in [kilowatt-hours](#dod:watt-hours) per person.",
    "reserves_ratio": "Number of years of production left at current reserves and production rates.",
}


def _view_title(fuel: str, metric: str) -> str:
    name = FUEL_TITLE_NAMES[fuel]
    return {
        "production": f"{name.capitalize()} production",
        "per_capita": f"{name.capitalize()} production per person",
        "reserves_ratio": f"Reserves-to-production ratio for {name}",
    }[metric]


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

# Per-fuel fallback for views without a pre-existing chart (e.g. the reserves-to-production ratio).
FUEL_FALLBACK_SCHEME = {"coal": "OrRd", "oil": "YlOrRd", "gas": "Purples"}

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
        config["subtitle"] = METRIC_UNIT_PHRASE[metric]
        config["map"] = _map_config(fuel, metric, dims_max.get((fuel, metric)))
        v.config = config
