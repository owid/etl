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
    }
    metric_titles = {
        "total": "Total energy supply by source",
        "per_capita": "Energy supply per person, by source",
        "share": "Share of total energy supply, by source",
        "annual_change": "Annual change in energy supply, by source",
    }
    c.group_views(
        groups=[
            {
                "dimension": "source",
                "choices": [
                    "coal",
                    "oil",
                    "gas",
                    "nuclear",
                    "hydro",
                    "wind",
                    "solar",
                    "biofuels",
                    "other_renewables",
                ],
                "choice_new_slug": "all_sources",
                "view_config": stacked_view_config,
            },
            {
                "dimension": "source",
                "choices": ["fossil_fuels", "nuclear", "renewables"],
                "choice_new_slug": "fossil_nuclear_renewables",
                "view_config": stacked_view_config,
            },
        ],
        params={"title": lambda view: metric_titles[view.dimensions["metric"]]},
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


def set_view_titles(c) -> None:
    for v in c.views:
        source = v.dimensions["source"]
        if source not in SOURCE_TITLE_NAMES:
            # Grouped/stacked views already have a title from group_views.
            continue
        config = dict(v.config or {})
        config["title"] = _view_title(source, v.dimensions["metric"])
        v.config = config
