"""Multi-dim chart on deaths from natural disasters."""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

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

# Map metric -> garden indicator prefix.
INDICATOR_BY_METRIC = {
    "total_number": "total_dead",
    "per_capita": "total_dead_per_100k_people",
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
}

STACKED_VIEW_CONFIG = {
    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
    "chartTypes": ["StackedBar"],
    "hasMapTab": False,
    "tab": "chart",
    "yAxis": {"min": 0},
}


def _prepare_table(tb: Table, garden_timespan: str, mdim_timespan: str) -> Table:
    """Filter the wide grapher table to the columns we need and tag them with
    dimension metadata so the multidim explorer can pick the right indicator."""
    columns_to_keep = ["country", "year"]
    for metric, indicator in INDICATOR_BY_METRIC.items():
        for type_slug in DISASTER_TYPES:
            col = f"{indicator}_{type_slug}_{garden_timespan}"
            assert col in tb.columns, f"Column {col} not found in {tb.metadata.short_name}"
            tb[col].metadata.original_short_name = "value"
            tb[col].metadata.dimensions = {
                "type": type_slug,
                "timespan": mdim_timespan,
                "metric": metric,
            }
            columns_to_keep.append(col)

    tb = tb[columns_to_keep]
    tb.metadata.dimensions = [
        {"name": "country", "slug": "country"},
        {"name": "year", "slug": "year"},
        {"name": "Disaster type", "slug": "type"},
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
        }
    )

    c.save()


def _title(view) -> str:
    type_phrase = DISASTER_PHRASES[view.dimensions["type"]]
    if view.dimensions["metric"] == "per_capita":
        body = f"Annual death rate from {type_phrase}"
    else:
        body = f"Annual deaths from {type_phrase}"
    if view.dimensions["timespan"] == "decadal":
        return f"Decadal average: {body}"
    return body


def _subtitle(view) -> str:
    parts = []
    if view.dimensions["metric"] == "per_capita":
        parts.append("Death rates are measured as the number of deaths per 100,000 people.")
    if view.dimensions["timespan"] == "decadal":
        parts.append(
            "Decadal figures are measured as the annual average over the subsequent ten-year period."
        )
    parts.append(
        "Disasters include all geophysical, meteorological, and climate events such as earthquakes, "
        "volcanic activity, drought, wildfires, storms, and flooding."
    )
    return " ".join(parts)
