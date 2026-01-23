"""Per capita CO₂ emissions relative to global average.

This chart automatically calculates the global average per capita CO₂ emissions
and uses it as the threshold in the map visualization.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    """Create chart with dynamically calculated global average threshold."""
    # Load the dataset to get the latest global average
    ds = paths.load_dataset("global_carbon_budget")
    tb = ds.read("global_carbon_budget")

    # Get the latest year's global per capita emissions
    world_data = tb[tb["country"] == "World"].sort_values("year", ascending=False)
    assert len(world_data) > 0, "No data found for World"
    latest_global_per_capita = float(world_data["emissions_total_per_capita"].iloc[0])
    assert latest_global_per_capita > 4, "Unexpected global emissions per capita."

    # Create chart with dynamic threshold substituted into YAML template
    paths.create_graph(
        yaml_params={
            "global_per_capita_threshold": latest_global_per_capita,
        }
    )
