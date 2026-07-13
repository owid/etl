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
        "chartTypes": ["LineChart"],
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
    c.group_views(
        groups=[
            {
                "dimension": "fuel",
                "choices": ["coal", "oil", "gas"],
                "choice_new_slug": "all_fuels",
                "view_config": {
                    "chartTypes": ["StackedArea"],
                    "tab": "chart",
                    "hasMapTab": False,
                    "title": "{title}",
                },
            },
        ],
        params={"title": lambda view: metric_titles[view.dimensions["metric"]]},
    )
    # Stacking reserves-to-production ratios (years of production left) makes no sense; drop that combination.
    c.views = [
        v for v in c.views if not (v.dimensions["fuel"] == "all_fuels" and v.dimensions["metric"] == "reserves_ratio")
    ]

    #
    # Save outputs.
    #
    c.save()
