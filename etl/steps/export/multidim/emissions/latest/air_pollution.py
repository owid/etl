"""
This mdim was created as a proof of concept. It can be deleted if not used.
"""

from owid.catalog.meta import TableDimension

from etl.collections import multidim
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Load (flattened) table from grapher channel with dimension metadata
    table = paths.load_dataset("ceds_air_pollutants").read("ceds_air_pollutants", load_data=False)

    # Use the updated function with mapping from original_short_name to configuration values.
    add_dimension(
        table,
        dimension={"name": "Per capita", "slug": "per_capita"},
        mapping={
            "emissions_per_capita": {"new_short_name": "emissions", "value": "True"},
            "emissions": {"value": "False"},
        },
    )

    # Update config with dimensions and views
    config.update(multidim.expand_config(table))

    # Sort pollutants by name
    # TODO: should sorting be part of dimension metadata in garden channel?
    config["dimensions"][0]["choices"] = sorted(config["dimensions"][0]["choices"], key=lambda x: x["name"])

    # Upsert mdim config to DB
    multidim.upsert_multidim_data_page(
        config=config,
        paths=paths,
    )


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
