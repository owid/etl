"""
This collection was created as a proof of concept. It can be deleted if not used.
"""

from owid.catalog.meta import TableDimension

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # Load (flattened) table from grapher channel with dimension metadata
    tb = paths.load_dataset("ceds_air_pollutants").read("ceds_air_pollutants", load_data=False)
    # Use the updated function with mapping from original_short_name to configuration values.
    add_dimension(
        tb,
        dimension={"name": "Per capita", "slug": "per_capita"},
        mapping={
            "emissions_per_capita": {"new_short_name": "emissions", "value": "True"},
            "emissions": {"value": "False"},
        },
    )

    # Create collection
    c = paths.create_collection(
        config=config,
        tb=tb,
    )

    # Sort choices alphabetically
    c.sort_choices({"pollutant": lambda x: sorted(x)})

    # Save
    c.save()


def add_dimension(table, dimension: TableDimension, mapping: dict) -> None:
    """Adds a dimension and updates column metadata.

    The mapping is a dict where each key is an original short name and its value is a dict.
    The value dict can contain:
      - 'new_short_name': Optional new short name.
      - 'value': The value value to set for the dimension.
    """
    table.m.dimensions.append(dimension)
    for _, v in table.items():
        key = v.m.original_short_name
        if key in mapping:
            config = mapping[key]
            v.m.dimensions[dimension["slug"]] = config["value"]
            if "new_short_name" in config:
                v.m.original_short_name = config["new_short_name"]
