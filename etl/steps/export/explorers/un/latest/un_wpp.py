"""
- [x] population
- [ ] population broad age groups
- [x] population change
- [ ] population growth rate
- [ ] natural population growth rate
- [x] population density
- [ ] Fertility rate
- [ ] Births
- [ ] Birth rate
- [ ] Deaths
- [ ] Death rate
- [ ] Number of child deaths
- [ ] Child mortality rate
- [ ] Number of infants deaths
- [ ] Infant mortality rate
- [ ] Life expectancy
- [ ] Age structure
- [x] Dependency ratio
- [ ] Median age
- [ ] Net migration
- [ ] Net migration rate
- [ ] Sex ratio

total done: 4/22
"""

from etl.collections.explorer import create_explorer, expand_config
from etl.collections.multidim import combine_config_dimensions

# from etl.files import yaml_dump
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
AGES_NAME = {age: f"{age.replace('-', '–')} years" if age != "1" else "1 year" for age in AGES_BASIC}


# etlr multidim
def run() -> None:
    # engine = get_engine()
    # Load configuration from adjacent yaml file.
    config = paths.load_explorer_config()

    # Add views for all dimensions
    # NOTE: using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("un_wpp")

    # 1) POPULATION
    tb = ds.read("population", load_data=False)
    # Bake config automatically from table
    config_new = expand_config(
        tb=tb,
        indicator_names=["population", "population_change", "population_density"],
        dimensions={
            "age": ["all", "0", "0-4", "0-14", "0-24"] + AGES_BASIC,
            "sex": "*",
            "variant": ["estimates"],
        },
    )
    # Combine & bake dimensions
    config["dimensions"] = combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config["dimensions"],
    )
    for dim in config["dimensions"]:
        if dim["slug"] == "age":
            for choice in dim["choices"]:
                if choice["slug"] in AGES_NAME:
                    choice["name"] = AGES_NAME[choice["slug"]]
    # Add views
    config["views"] = config_new["views"]

    # 2) DEPENDENCY RATIO
    tb_dep = ds.read("dependency_ratio", load_data=False)

    config_new = expand_config(
        tb=tb_dep,
        indicator_names=["dependency_ratio"],
        dimensions={
            "age": "*",
            "sex": "*",
            "variant": ["estimates"],
        },
        indicator_as_dimension=True,
    )

    config["dimensions"] = combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config["dimensions"],
    )
    config["views"] += config_new["views"]

    # 3) SEX RATIO
    tb_sr = ds.read("sex_ratio", load_data=False)

    config_new = expand_config(
        tb=tb_sr,
        indicator_names=["sex_ratio"],
        dimensions={
            "age": "*",
            "sex": "*",
            "variant": ["estimates"],
        },
        indicator_as_dimension=True,
    )

    config["dimensions"] = combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config["dimensions"],
    )
    config["views"] += config_new["views"]

    # 2) DEPENDENCY RATIO
    # tb_dep = ds.read("dependency_ratio", load_data=False)

    # config_new = expand_config(
    #     tb=tb_dep,
    #     indicator_names=["dependency_ratio"],
    #     dimensions={
    #         "age": "*",
    #         "sex": "*",
    #         "variant": ["estimates"],
    #     },
    #     indicator_as_dimension=True,
    # )

    # config["dimensions"] = combine_config_dimensions(
    #     config_dimensions=config_new["dimensions"],
    #     config_dimensions_yaml=config["dimensions"],
    # )
    # config["views"] += config_new["views"]

    # DEBUGGING
    # with open("/home/lucas/repos/etl/etl/steps/export/explorers/un/latest/un_wpp2.config.yml", "w") as f:
    #     yaml_dump(config, f)

    # Export
    # Create explorer
    ds_explorer = paths.create_explorer(
        config=config,
        explorer_name="population-and-demography",
    )

    ds_explorer.save(tolerate_extra_indicators=True)

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
