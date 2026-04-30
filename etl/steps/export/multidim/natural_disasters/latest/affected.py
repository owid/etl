"""Multi-dim chart on the human impact of natural disasters (excluding deaths).

Each column of the source grapher tables is tagged with dimension metadata
(type, impact, timespan, metric) so that the multidim explorer can pick the right
indicator for each combination of choices.
"""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Disaster types to expose, in the order they appear in the dimension choices.
DISASTER_TYPES = [
    "all_disasters",
    "all_disasters_excluding_extreme_temperature",
    "drought",
    "earthquake",
    "volcanic_activity",
    "flood",
    "dry_mass_movement",
    "extreme_weather",
    "wildfire",
    "extreme_temperature",
]

# Disaster types stacked together for the "All disasters (by type)" view.
INDIVIDUAL_DISASTER_TYPES = [
    "drought",
    "earthquake",
    "volcanic_activity",
    "flood",
    "dry_mass_movement",
    "extreme_weather",
    "wildfire",
    "extreme_temperature",
]

# Map (impact_slug, metric_slug) -> garden indicator prefix.
INDICATOR_BY_IMPACT_METRIC = {
    ("total_affected", "total_number"): "total_affected",
    ("total_affected", "per_capita"): "total_affected_per_100k_people",
    ("injured", "total_number"): "injured",
    ("injured", "per_capita"): "injured_per_100k_people",
    ("requiring_assistance", "total_number"): "affected",
    ("requiring_assistance", "per_capita"): "affected_per_100k_people",
    ("homeless", "total_number"): "homeless",
    ("homeless", "per_capita"): "homeless_per_100k_people",
}

# Sentence fragment used in titles for each impact choice.
# Maps (impact, metric) to the verb-phrase placed between "Annual ... of people" and the disaster.
IMPACT_PHRASES = {
    ("total_affected", "total_number"): ("Annual number of people affected by", "from"),
    ("total_affected", "per_capita"): ("Annual rate of people affected by", "from"),
    ("injured", "total_number"): ("Annual number of people injured by", "from"),
    ("injured", "per_capita"): ("Annual rate of people injured by", "from"),
    ("requiring_assistance", "total_number"): (
        "Annual number of people requiring immediate assistance during",
        "from",
    ),
    ("requiring_assistance", "per_capita"): (
        "Annual rate of people requiring immediate assistance during",
        "from",
    ),
    ("homeless", "total_number"): ("Annual number of people left homeless by", "from"),
    ("homeless", "per_capita"): ("Annual rate of people left homeless by", "from"),
}

# Human-readable phrase used in chart titles for each disaster-type choice.
DISASTER_PHRASES = {
    "all_disasters": "all disasters",
    "all_disasters_excluding_extreme_temperature": "all disasters excluding extreme temperatures",
    "all_stacked": "disasters",
    "drought": "droughts",
    "earthquake": "earthquakes",
    "volcanic_activity": "volcanic activity",
    "flood": "floods",
    "dry_mass_movement": "dry mass movements",
    "extreme_weather": "storms",
    "wildfire": "wildfires",
    "extreme_temperature": "extreme temperatures",
}

COMMON_VIEW_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
    "chartTypes": ["StackedBar"],
    "hasMapTab": True,
    "tab": "chart",
    "yAxis": {"min": 0},
    "originUrl": "https://ourworldindata.org/natural-disasters",
    # Default chart timeline starts at 2000, when reporting coverage becomes more complete.
    "minTime": 2000,
    # Pin the map to a single year so it doesn't fall back to a "first year vs last year"
    # comparison view when minTime/maxTime defines a range on the chart.
    "map": {"time": "latest"},
}

STACKED_VIEW_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
    "chartTypes": ["StackedBar"],
    "hasMapTab": False,
    "tab": "chart",
    "yAxis": {"min": 0},
    "originUrl": "https://ourworldindata.org/natural-disasters",
    "minTime": 2000,
    # Without this, a year drops out of the stack as soon as one disaster type
    # has no reported data — even if other types had a non-zero value.
    "missingDataStrategy": "show",
}

# Footnote shown on every chart, flagging the limited reporting coverage in earlier decades.
NOTE = (
    "Figures are based on reported data, with limited coverage before around 2000, "
    "so historical trends may partly reflect reporting improvements."
)


def _prepare_table(tb: Table, garden_timespan: str, mdim_timespan: str) -> Table:
    columns_to_keep = ["country", "year"]
    for (impact, metric), indicator in INDICATOR_BY_IMPACT_METRIC.items():
        for type_slug in DISASTER_TYPES:
            col = f"{indicator}_{type_slug}_{garden_timespan}"
            assert col in tb.columns, f"Column {col} not found in {tb.metadata.short_name}"
            tb[col].metadata.original_short_name = "value"
            tb[col].metadata.dimensions = {
                "type": type_slug,
                "impact": impact,
                "timespan": mdim_timespan,
                "metric": metric,
            }
            columns_to_keep.append(col)

    tb = tb[columns_to_keep]
    tb.metadata.dimensions = [
        {"name": "country", "slug": "country"},
        {"name": "year", "slug": "year"},
        {"name": "Disaster type", "slug": "type"},
        {"name": "Impact", "slug": "impact"},
        {"name": "Timespan", "slug": "timespan"},
        {"name": "Metric", "slug": "metric"},
    ]
    return tb


def run() -> None:
    config = paths.load_collection_config()

    ds = paths.load_dataset("natural_disasters")
    tb_yearly = _prepare_table(
        ds.read("natural_disasters_yearly", load_data=False),
        garden_timespan="yearly",
        mdim_timespan="annual",
    )
    tb_decadal = _prepare_table(
        ds.read("natural_disasters_decadal", load_data=False),
        garden_timespan="decadal",
        mdim_timespan="decadal",
    )

    c = paths.create_collection(
        config=config,
        tb=[tb_yearly, tb_decadal],
        indicator_names="value",
        common_view_config=COMMON_VIEW_CONFIG,
    )

    c.group_views(
        groups=[
            {
                "dimension": "type",
                "choice_new_slug": "all_stacked",
                "choices": INDIVIDUAL_DISASTER_TYPES,
                "view_config": STACKED_VIEW_CONFIG,
            }
        ],
    )

    c.set_global_config(
        {
            "title": _title,
            "subtitle": _subtitle,
            "note": NOTE,
            # The "All disasters" total view is shown only as a world map (no chart tab),
            # to complement the stacked-by-type view, which only has a chart tab.
            # Setting chartTypes=[] hides the chart tab; Grapher then auto-switches to the
            # map tab when the user navigates to this view.
            "chartTypes": lambda view: (
                [] if view.dimensions["type"] == "all_disasters" else ["StackedBar"]
            ),
            "tab": lambda view: "map" if view.dimensions["type"] == "all_disasters" else "chart",
        }
    )

    c.save()


def _title(view) -> str:
    impact = view.dimensions["impact"]
    metric = view.dimensions["metric"]
    type_phrase = DISASTER_PHRASES[view.dimensions["type"]]
    body, _ = IMPACT_PHRASES[(impact, metric)]
    title = f"{body} {type_phrase}"
    if view.dimensions["timespan"] == "decadal":
        return f"{title} (10-year average)"
    return title


def _subtitle(view) -> str:
    parts = []
    if view.dimensions["impact"] == "total_affected":
        parts.append(
            "The total number of people affected is the sum of those injured, requiring assistance, and left homeless."
        )
    if view.dimensions["metric"] == "per_capita":
        parts.append("Rates are measured per 100,000 people.")
    if view.dimensions["timespan"] == "decadal":
        parts.append("Decadal figures are measured as the annual average over the subsequent ten-year period.")
    parts.append(
        "Disasters include all geophysical, meteorological, and climate events such as earthquakes, "
        "volcanic activity, drought, wildfires, storms, and flooding."
    )
    return " ".join(parts)
