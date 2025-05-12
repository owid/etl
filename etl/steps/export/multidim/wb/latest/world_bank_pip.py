from etl.collection import multidim

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

DIMENSIONS_CONFIG = {
    "ppp_version": ["2017.0"],
    "poverty_line": "*",
    "table": ["Income or consumption consolidated", "Consumption", "Income"],
    "welfare_type": "*",
    "decile": "*",
    "survey_comparability": "*",
}


# etlr multidim
def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("world_bank_pip_dimensional")
    tb = ds.read("world_bank_pip_dimensional", load_data=False)

    # Bake config automatically from table
    config_new = multidim.expand_config(
        tb,  # type: ignore
        indicator_names=["headcount_ratio", "headcount"],
        dimensions=DIMENSIONS_CONFIG,
    )

    # Combine both sources
    config["dimensions"] = multidim.combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config.get("dimensions", {}),
    )
    config["views"] = config_new["views"]

    # Create mdim
    mdim = paths.create_mdim(
        config=config,
        mdim_name="poverty",
    )

    # # Edit order of slugs
    # mdim.sort_choices({"poverty_line": lambda x: sorted(x)})

    # Save & upload
    mdim.save()
