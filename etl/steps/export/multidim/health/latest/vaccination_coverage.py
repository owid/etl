from etl.collections import multidim

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# etlr multidim
def run(dest_dir: str) -> None:
    config = paths.load_mdim_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("vaccination_coverage")
    tb = ds.read("vaccination_coverage", load_data=False)

    # 2: Bake config automatically from table
    config_new = multidim.expand_config(
        tb,
        indicator_names=["coverage", "unvaccinated_one_year_olds"],
        dimensions=["antigen"],
    )

    # 3: Combine both sources (basically dimensions and views)
    # config["dimensions"] = config_coverage["dimensions"]
    config["dimensions"] = multidim.combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config["dimensions"],
    )

    config["views"] = config_new["views"]

    # 4: Upsert to DB
    multidim.upsert_multidim_data_page(
        mdim_name="mdd-vaccination-who",
        config=config,
        paths=paths,
    )
