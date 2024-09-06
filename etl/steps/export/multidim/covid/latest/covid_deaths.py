from pathlib import Path

import yaml

from etl import multidim
from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

CURRENT_DIR = Path(__file__).parent


def run(dest_dir: str) -> None:
    engine = get_engine()

    # Load YAML file
    with open(CURRENT_DIR / f"{paths.short_name}.yml") as istream:
        config = yaml.safe_load(istream)

    multidim.upsert_multidim_data_page("mdd-covid-deaths", config, engine)
