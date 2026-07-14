"""Multidim for fossil fuel production (fuel x metric)."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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
    set_view_titles(c)

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


def _map_config(fuel: str, metric: str) -> dict:
    scheme = ORIGINAL_MAP_SCHEMES.get((fuel, metric)) or {"baseColorScheme": FUEL_FALLBACK_SCHEME[fuel]}
    return {"colorScale": scheme, "timeTolerance": 3}


def set_view_titles(c) -> None:
    for v in c.views:
        fuel = v.dimensions["fuel"]
        if fuel not in FUEL_TITLE_NAMES:
            # Grouped/stacked view already has a title from group_views.
            continue
        config = dict(v.config or {})
        config["title"] = _view_title(fuel, v.dimensions["metric"])
        config["subtitle"] = METRIC_UNIT_PHRASE[v.dimensions["metric"]]
        config["map"] = _map_config(fuel, v.dimensions["metric"])
        v.config = config
