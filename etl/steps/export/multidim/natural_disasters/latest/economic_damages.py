"""Multi-dim chart on economic damages from natural disasters."""

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
    apply_disaster_colors,
    prepare_table,
)

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Map metric -> garden indicator prefix.
# We use damages in current US$ (not adjusted for inflation): the inflation-adjusted
# series produces unrealistic values for countries that have experienced hyperinflation.
INDICATOR_BY_METRIC = {
    "total_damages": "total_damages",
    "share_of_gdp": "total_damages_per_gdp",
}


def _title(view) -> str:
    type_phrase = DISASTER_PHRASES[view.dimensions["type"]]
    if view.dimensions["metric"] == "share_of_gdp":
        body = f"Annual economic damages from {type_phrase} as a share of GDP"
    else:
        body = f"Annual economic damages from {type_phrase}"
    if view.dimensions["timespan"] == "decadal":
        return f"Decadal average: {body}"
    return body


def _subtitle(view) -> str:
    parts = []
    if view.dimensions["metric"] == "total_damages":
        parts.append("Estimated damages are reported in current US$ (not adjusted for inflation).")
    else:
        parts.append("Damages are expressed as a share of gross domestic product (GDP).")
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
    sample_description_key = list(tb_yearly["total_damages_all_disasters_yearly"].metadata.description_key or [])
    indicators = [({"metric": metric}, prefix) for metric, prefix in INDICATOR_BY_METRIC.items()]
    tb_yearly = prepare_table(tb_yearly, "yearly", "annual", indicators)
    tb_decadal = prepare_table(tb_decadal, "decadal", "decadal", indicators)

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
    add_total_indicator_for_map(c, lambda d: INDICATOR_BY_METRIC[d["metric"]])

    # Pin a stable colour to each y-indicator based on its disaster type.
    apply_disaster_colors(c)

    #
    # Save outputs.
    #
    c.save()
