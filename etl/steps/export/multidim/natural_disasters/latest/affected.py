"""Multi-dim chart on the human impact of natural disasters (excluding deaths).

Each column of the source grapher tables is tagged with dimension metadata
(type, impact, timespan, metric) so that the multidim explorer can pick the right
indicator for each combination of choices.
"""

from shared import (
    ALL_DISASTERS_EXCL_EXTREME_TEMP_SUBTITLE,
    ALL_DISASTERS_SUBTITLE,
    COMMON_VIEW_CONFIG,
    DISASTER_DESCRIPTIONS,
    DISASTER_PHRASES,
    DISASTER_TYPES_EXCLUDING_EXTREME_TEMPERATURE,
    INDIVIDUAL_DISASTER_TYPES,
    NOTE,
    STACKED_VIEW_CONFIG,
    add_total_indicator_for_map,
    apply_decadal_time_range,
    apply_disaster_colors,
    prepare_table,
)

from etl.helpers import PathFinder

paths = PathFinder(__file__)

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
    ("injured", "per_capita"): ("Annual injury rate from", "from"),
    ("requiring_assistance", "total_number"): (
        "Annual number of people requiring immediate assistance due to",
        "from",
    ),
    ("requiring_assistance", "per_capita"): (
        "Annual rate of people requiring immediate assistance due to",
        "from",
    ),
    ("homeless", "total_number"): ("Annual number of people left homeless by", "from"),
    ("homeless", "per_capita"): ("Annual rate of people left homeless by", "from"),
}


def _title(view) -> str:
    impact = view.dimensions["impact"]
    metric = view.dimensions["metric"]
    type_phrase = DISASTER_PHRASES[view.dimensions["type"]]
    body, _ = IMPACT_PHRASES[(impact, metric)]
    title = f"{body} {type_phrase}"
    if view.dimensions["timespan"] == "decadal":
        return f"Decadal average: {title}"
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
    type_slug = view.dimensions["type"]
    if type_slug == "all_stacked":
        parts.append(ALL_DISASTERS_SUBTITLE)
    elif type_slug == "all_disasters_excluding_extreme_temperature":
        parts.append(ALL_DISASTERS_EXCL_EXTREME_TEMP_SUBTITLE)
    elif type_slug in DISASTER_DESCRIPTIONS:
        parts.append(DISASTER_DESCRIPTIONS[type_slug])
    return " ".join(parts)


def run() -> None:
    #
    # Load inputs.
    #
    config = paths.load_collection_config()
    ds = paths.load_dataset("natural_disasters")
    tb_yearly = ds.read("natural_disasters_yearly", load_data=False)
    tb_decadal = ds.read("natural_disasters_decadal", load_data=False)

    #
    # Process data.
    #
    # Sample the all-disasters aggregate description_key for grouped views (must
    # run before prepare_table drops the column).
    sample_description_key = list(tb_yearly["total_affected_all_disasters_yearly"].metadata.description_key or [])
    indicators = [
        ({"impact": impact, "metric": metric}, prefix)
        for (impact, metric), prefix in INDICATOR_BY_IMPACT_METRIC.items()
    ]
    extra_dimensions = [("Impact", "impact")]
    tb_yearly = prepare_table(tb_yearly, "yearly", "annual", indicators, extra_dimensions)
    tb_decadal = prepare_table(tb_decadal, "decadal", "decadal", indicators, extra_dimensions)

    c = paths.create_collection(
        config=config,
        tb=[tb_yearly, tb_decadal],
        indicator_names="value",
        common_view_config=COMMON_VIEW_CONFIG,
    )
    grouped_view_metadata = {
        "presentation": {"title_public": _title},
        "description_short": _subtitle,
        "description_key": sample_description_key,
    }

    c.group_views(
        groups=[
            {
                "dimension": "type",
                "choice_new_slug": "all_stacked",
                "choices": INDIVIDUAL_DISASTER_TYPES,
                "view_config": STACKED_VIEW_CONFIG,
                "view_metadata": grouped_view_metadata,
            },
            {
                "dimension": "type",
                "choice_new_slug": "all_disasters_excluding_extreme_temperature",
                "choices": DISASTER_TYPES_EXCLUDING_EXTREME_TEMPERATURE,
                "view_config": STACKED_VIEW_CONFIG,
                "view_metadata": grouped_view_metadata,
            },
        ],
    )

    c.set_global_config(
        {
            "title": _title,
            "subtitle": _subtitle,
            "note": NOTE,
        }
    )

    # Expose the all-disasters total on the map tab of the stacked-by-type views.
    add_total_indicator_for_map(c, lambda d: INDICATOR_BY_IMPACT_METRIC[(d["impact"], d["metric"])])

    # Pin a stable colour to each y-indicator based on its disaster type.
    apply_disaster_colors(c)

    # Decadal views default to the full 1900-onwards range; annual views stay at 2000.
    apply_decadal_time_range(c)

    #
    # Save outputs.
    #
    c.save()
