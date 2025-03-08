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
    table = paths.load_dataset("wgm_2018").read("wgm_2018", load_data=False)

    table = table.filter(regex="gender_all__age_group_all")

    # Update config with dimensions and views
    config.update(multidim.expand_config(table, indicator_name="share"))

    # Upsert mdim config to DB
    multidim.upsert_multidim_data_page(
        config=config,
        paths=paths,
    )
