from etl.collections.explorers import create_explorer, expand_config
from etl.collections.multidim import combine_config_dimensions
from etl.files import yaml_dump

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


AGES_BASIC = (
    [
        "15-64",
        "15+",
        "18+",
        "1",
    ]
    + [f"{i}-{i+4}" for i in range(5, 20, 5)]
    + [f"{i}-{i+9}" for i in range(20, 100, 10)]
    + ["100+"]
)
AGES_NAME = {age: f"{age.replace('-', '–')} year" if age != "1" else "1 year" for age in AGES_BASIC}


# etlr multidim
def run(dest_dir: str) -> None:
    # engine = get_engine()
    # Load configuration from adjacent yaml file.
    config = paths.load_explorer_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("un_wpp")
    tb = ds.read("population", load_data=False)

    # 2: Bake config automatically from table
    config_new = expand_config(
        tb,
        indicator_names=["population", "population_density"],
        dimensions={
            "age": ["all", "0", "0-4", "0-14", "0-24"] + AGES_BASIC,
            "sex": "*",
            "variant": ["estimates"],
        },
    )

    # 3: Combine
    config["dimensions"] = combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config["dimensions"],
    )
    for dim in config["dimensions"]:
        if dim["slug"] == "age":
            for choice in dim["choices"]:
                if choice["slug"] in AGES_NAME:
                    choice["name"] = AGES_NAME[choice["slug"]]

    config["views"] = config_new["views"]

    # TMP
    with open("/home/lucas/repos/etl/etl/steps/export/explorers/un/latest/un_wpp2.config.yml", "w") as f:
        yaml_dump(config, f)

    # Export
    # Create explorer
    ds_explorer = create_explorer(
        dest_dir=str(paths.dest_dir),
        config=config,
        paths=paths,
        tolerate_extra_indicators=True,
        explorer_name="population-and-demography",
    )

    ds_explorer.save()

    # config_mdim = {
    #     "title": {
    #         "title": "Population",
    #         "titleVariant": "by age and age group",
    #     },
    #     "defaultSelection": ["United States", "India", "China", "Indonesia", "Pakistan"],
    #     "dimensions": config["dimensions"],
    #     "views": config["views"],
    # }
    # upsert_multidim_data_page(
    #     config=config_mdim,
    #     paths=paths,
    #     mdim_name="population-test",
    # )
