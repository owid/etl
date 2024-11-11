from pathlib import Path

from owid.catalog import utils

from etl import multidim
from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

CURRENT_DIR = Path(__file__).parent


def run(dest_dir: str) -> None:
    engine = get_engine()

    # Load YAML file
    config = utils.dynamic_yaml_to_dict(utils.dynamic_yaml_load(CURRENT_DIR / "emigration.yml"))

    multidim.upsert_multidim_data_page("mdd-emigration", config, engine)
