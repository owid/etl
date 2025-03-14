"""
PROGRESS:
[in explorer] [FAUST] [map]:
- [x] [x] [ ] population
- [x] [o] [x] population broad age groups: legend
- [x] [x] [ ] population change
- [x] [o] [ ] population growth rate: type (+/-)
- [x] [o] [ ] natural population growth rate: type (+/-)
- [x] [x] [ ] population density
- [x] [x] [ ] Fertility rate
- [x] [x] [ ] Births
- [x] [x] [ ] Birth rate
- [x] [x] [ ] Deaths
- [x] [x] [ ] Death rate
- [x] [x] [ ] Number of child deaths
- [x] [x] [ ] Child mortality rate
- [x] [x] [ ] Number of infants deaths
- [x] [x] [ ] Infant mortality rate
- [x] [x] [ ] Life expectancy
- [x] [o] [x] Age structure: Legend display names
- [x] [x] [ ] Dependency ratio
- [x] [x] [ ] Median age
- [x] [x] [ ] Net migration
- [x] [x] [ ] Net migration rate
- [x] [x] [ ] Sex ratio


x: done
o: in progress
"""

# from etl.files import yaml_dump
# from etl.db import get_engine
from etl.helpers import PathFinder

from .config_edits import ConfigEditor
from .utils import ExplorerCreator, combine_explorers

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Population
AGES_POP_LIST = (
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
AGES_POP = {age: f"{age.replace('-', '–')} years" if age != "1" else "1 year" for age in AGES_POP_LIST}

# Sex ratio
AGES_SR = {
    **{str(age): f"At age {age}" for age in ["5", "10", "15"] + list(range(20, 100, 10))},
    "100+": "At age 100 and over",
}

# Deaths
AGES_DEATHS_LIST = [f"{i}-{i+4}" for i in range(5, 20, 5)] + [f"{i}-{i+9}" for i in range(20, 100, 10)] + ["100+"]
AGES_DEATHS = {age: f"{age.replace('-', '–')} years" for age in AGES_POP_LIST}


def run() -> None:
    # LOAD DATASETS,
    ds = paths.load_dataset("un_wpp")
    ds_full = paths.load_dataset("un_wpp_full")
    explorer_creator = ExplorerCreator(paths, ds, ds_full)

    # Default config
    config_default = paths.load_explorer_config()

    #################################################################################################
    # Create individual explorers (building blocks)
    #################################################################################################

    # 1) Population
    explorer_pop = explorer_creator.create(
        table_name="population",
        config_yaml=config_default,
        indicator_names=["population", "population_change", "population_density"],
        dimensions={
            "age": ["all", "0", "0-4", "0-14", "0-24"] + AGES_POP_LIST,
            "sex": "*",
            "variant": ["estimates"],
        },
        choice_renames={"age": AGES_POP},
    )
    ConfigEditor.edit_views_pop(explorer_pop)

    # 2) Dependency ratio
    explorer_dep = explorer_creator.create(
        table_name="dependency_ratio",
        config_yaml=config_default,
        indicator_names=["dependency_ratio"],
        dimensions={
            "age": "*",
            "sex": "*",
            "variant": ["estimates"],
        },
    )
    ConfigEditor.edit_views_dr(explorer_dep)

    # 3) Sex ratio
    explorer_sr = explorer_creator.create(
        table_name="sex_ratio",
        config_yaml=paths.load_explorer_config("un_wpp.sex_ratio.config.yml"),
        indicator_names=["sex_ratio"],
        dimensions={
            "age": ["all", "0"] + list(AGES_SR.keys()),
            "sex": "*",
            "variant": ["estimates"],
        },
        choice_renames={"age": AGES_SR},
    )
    ConfigEditor.edit_views_sr(explorer_sr)

    # 4) Migration
    explorer_mig = explorer_creator.create(
        table_name="migration",
        config_yaml=config_default,
        indicator_names=["net_migration", "net_migration_rate"],
        dimensions={
            "age": "*",
            "sex": "*",
            "variant": ["estimates"],
        },
        dimensions_proj={
            "variant": ["medium"],
        },
        choice_renames={"age": AGES_SR},
    )

    # 5) Deaths
    explorer_deaths = explorer_creator.create(
        table_name="deaths",
        config_yaml=config_default,
        indicator_names=["deaths", "death_rate"],
        dimensions={
            "age": ["all", "0", "0-4"] + AGES_DEATHS_LIST,
            "sex": "*",
            "variant": ["estimates"],
        },
        choice_renames={"age": AGES_DEATHS},
    )
    ConfigEditor.edit_views_deaths(explorer_deaths)

    # 6) Births
    explorer_b = explorer_creator.create(
        table_name="births",
        config_yaml=config_default,
        indicator_names=["births", "birth_rate"],
        dimensions={
            "age": "*",
            "sex": "*",
            "variant": ["estimates"],
        },
        choice_renames={"age": lambda x: f"Mothers aged {x} years" if x != "all" else None},
    )
    ConfigEditor.edit_views_b(explorer_b)

    # 7) Median age
    explorer_ma = explorer_creator.create(
        table_name="median_age",
        config_yaml=config_default,
        indicator_names=["median_age"],
        dimensions={
            "age": ["all"],
            "sex": "*",
            "variant": ["estimates"],
        },
        choice_renames={"age": lambda x: {"all": "None"}.get(x, None)},
    )

    # 8) Life expectancy
    explorer_le = explorer_creator.create(
        table_name="life_expectancy",
        config_yaml=config_default,
        indicator_names=["life_expectancy"],
        dimensions={
            "age": "*",
            "sex": "*",
            "variant": ["estimates"],
        },
        choice_renames={
            "age": {
                0: "At birth",
                15: "Aged 15",
                65: "Aged 65",
                80: "Aged 80",
            }
        },
    )
    ConfigEditor.edit_views_le(explorer_le)

    # 9) Fertility rate
    explorer_fr = explorer_creator.create(
        table_name="fertility_rate",
        config_yaml=config_default,
        indicator_names=["fertility_rate"],
        dimensions={
            "age": "*",
            "sex": "*",
            "variant": ["estimates"],
        },
        choice_renames={"age": lambda x: f"Mothers aged {x} years" if x != "all" else None},
    )
    ConfigEditor.edit_views_fr(explorer_fr)

    # 10 Manual
    explorer_manual = explorer_creator.create_manual(
        config=paths.load_explorer_config("un_wpp.manual.config.yml"),
    )

    #################################################################################################
    # Combine explorers
    #################################################################################################

    # List with all explorers
    explorers = [
        explorer_pop,
        explorer_dep,
        explorer_sr,
        explorer_mig,
        explorer_deaths,
        explorer_b,
        explorer_ma,
        explorer_le,
        explorer_fr,
        # manual views explorers
        explorer_manual,
    ]

    # Combine them into single explorer
    explorer = combine_explorers(
        explorers=explorers,
        explorer_name="population-and-demography",
        config=explorer_pop.config,
    )

    # Sort indicator choices
    explorer.sort_choices(
        {
            "indicator": [
                "population",
                "population_broad",
                "population_change",
                "growth_rate",
                "natural_change_rate",
                "population_density",
                "fertility_rate",
                "births",
                "birth_rate",
                "deaths",
                "death_rate",
                "child_deaths",
                "child_mortality_rate",
                "infant_deaths",
                "infant_mortality_rate",
                "life_expectancy",
                "age_structure",
                "dependency_ratio",
                "median_age",
                "net_migration",
                "net_migration_rate",
                "sex_ratio",
            ]
        }
    )

    # Save explorer (upsert to DB)
    explorer.save(tolerate_extra_indicators=True)
