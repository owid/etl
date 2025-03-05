from etl.collections import multidim
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("vaccination_coverage")
    tb = ds.read("vaccination_coverage", load_data=True)

    config["views"] = multidim.generate_views_for_dimensions(
        dimensions=config["dimensions"],
        tables=[tb],
        dimensions_order_in_slug=("metric", "antigen"),
        warn_on_missing_combinations=False,
        additional_config={
            "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.005.json",
            "chartTypes": ["LineChart"],
            "hasMapTab": True,
            "tab": "map",
            "map": {
                "colorScale": {"baseColorScheme": "YlGbBu"},
            },
        },
    )

    multidim.upsert_multidim_data_page(
        config=config,
        paths=paths,
    )
