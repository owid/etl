"""
This mdim was created as a proof of concept. It can be deleted if not used.
"""

from etl.collections import multidim
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Load (flattened) table from grapher channel with dimension metadata
    table = paths.load_dataset("ceds_air_pollutants").read("ceds_air_pollutants", load_data=False)

    # Add per-capita dimension
    # TODO: if expand_config worked for multiple indicators, then this would be unnecessary
    #   it's implemented by Lucas at the moment
    assert table.m.dimensions
    table.m.dimensions.append({"name": "Per capita", "slug": "per_capita"})

    # Add dimensions to columns
    for _, v in table.items():
        if v.m.original_short_name == "emissions_per_capita":
            v.m.dimensions["per_capita"] = "True"
            # change indicator short name
            v.m.original_short_name = "emissions"
        elif v.m.original_short_name == "emissions":
            v.m.dimensions["per_capita"] = "False"

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
