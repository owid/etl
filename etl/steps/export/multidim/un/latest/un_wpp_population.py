from etl import multidim
from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    engine = get_engine()

    # Load data
    ds = paths.load_dataset("un_wpp")
    tb = ds.read("population", load_data=False)

    # 1: Load config from YAML (might contain curated info)
    config = paths.load_mdim_config()

    # 2: Bake config automatically from table
    config_new = multidim.expand_config(tb=tb, indicator_name="population")

    # 3: Combine both sources (basically dimensions and views)
    config["dimensions"] = multidim.combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config["dimensions"],
    )
    config["views"] = config["views"] + config_new["views"]

    # 4: Upsert to DB
    multidim.upsert_multidim_data_page(
        "mdd-population-un",
        config,
        engine,
        paths.dependencies,
    )


def fname_to_slug(fname: str) -> str:
    return f"mdd-{fname.replace('.yml', '').replace('.', '-').replace('_', '-')}"
