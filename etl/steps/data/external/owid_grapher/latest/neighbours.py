"""Neighbouring countries lists for each entity.

Published as JSON for use by Grapher codebase (peer countries, context panels).
Not meant to be imported to MySQL.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load neighbours list table from garden.
    ds_neighbours = paths.load_dataset("neighbours")
    tb = ds_neighbours.read("neighbours_list")

    # Convert semicolon-separated strings to lists.
    list_columns = ["neighbours_1", "neighbours_2", "neighbours_3", "neighbours_4", "neighbours_10"]
    for col in list_columns:
        tb[col] = tb[col].apply(lambda x: x.split(";") if x else [])

    # Rename columns to value_* format.
    tb = tb.rename(
        columns={
            "country": "entity",
            "neighbours_1": "value_neighbours_border",  # Neighbors by border share
            "neighbours_2": "value_neighbours_balanced",  # Neighbors by balanced score (1:1 border/distance)
            "neighbours_3": "value_neighbours_distance",  # Neighbors by distance-weighted score (~2:1 distance/border)
            "neighbours_4": "value_neighbours_population",  # Neighbors by population
            "neighbours_10": "value_nearest_borders",  # Nearest countries by distance
        }
    )

    # Set index and name.
    tb = tb.set_index(["entity"], verify_integrity=True)
    tb.metadata.short_name = "neighbours"

    # Save as JSON.
    ds = paths.create_dataset(tables=[tb], formats=["json"])
    ds.save()
