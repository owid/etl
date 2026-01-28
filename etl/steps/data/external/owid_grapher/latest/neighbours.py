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

    # Pick one neighbour list
    COLUMN = "value_neighbours_balanced"
    # "value_neighbours_border",  # Neighbors by border share
    # "value_neighbours_balanced",  # Neighbors by balanced score (1:1 border/distance)
    # "value_neighbours_distance",  # Neighbors by distance-weighted score (~2:1 distance/border)
    # "value_neighbours_population",  # Neighbors by population
    # "value_nearest_borders",  # Nearest countries by distance

    tb = tb.rename(
        columns={
            "country": "entity",
            COLUMN: "value",
        }
    )

    # Set index and name.
    tb = tb.format(["entity"], short_name="neighbours")

    # Save as JSON.
    ds = paths.create_dataset(tables=[tb], formats=["json"])
    ds.save()
