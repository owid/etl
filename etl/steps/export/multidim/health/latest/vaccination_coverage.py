from etl.collections import multidim

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# etlr multidim
def run() -> None:
    # engine = get_engine()
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("vaccination_coverage")
    tb = ds.read("vaccination_coverage", load_data=False)

    # 2: Bake config automatically from table
    config_new = multidim.expand_config(
        tb,
        indicator_names=["coverage", "unvaccinated", "vaccinated"],
        dimensions=["antigen"],
        indicators_slug="metric",
        indicator_as_dimension=True,
    )
    # 3: Combine both sources (basically dimensions and views)
    config["dimensions"] = multidim.combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config.get("dimensions", {}),
    )
    config["views"] = config_new["views"]

    # 4: Upsert to DB
    mdim = paths.create_mdim(
        config=config,
        mdim_name="mdd-vaccination-who",
    )
    mdim.save()
