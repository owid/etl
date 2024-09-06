from etl import multidim
from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    engine = get_engine()

    filenames = [
        "covid.cases.yml",
        "covid.deaths.yml",
    ]
    # Load YAML file
    for fname in filenames:
        config = paths.load_mdim_config(fname)
        slug = f"mdd-{fname.replace('.yml', '').replace('_', '-')}"
        multidim.upsert_multidim_data_page(slug, config, engine)
