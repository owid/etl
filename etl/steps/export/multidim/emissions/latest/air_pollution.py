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

    table = table.filter(regex="transport")

    # Update config with dimensions and views
    config.update(multidim.expand_config(table, indicator_name="emissions"))

    # Upsert mdim config to DB
    multidim.upsert_multidim_data_page(
        config=config,
        paths=paths,
    )
