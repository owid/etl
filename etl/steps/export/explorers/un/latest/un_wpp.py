"""
TODO: Remove usage of `hack_metadata_propagation` once https://owid.slack.com/archives/C46U9LXRR/p1742416990042489 is solved.

This step creates the population and demography explorer. It uses MDIM-based configuration, and some custom processing.

Strategy:

    - This explorer relies on multiple tables from the UN WPP dataset. Therefore, we first create individual explorers for each table and then combine them.
    - While some of the metadata is inherited from Garden/Grapher, some of it is set manually in the YAML files or programmatically once explorers are created.
    - Most of the explorers are created programmatically, with slight edits coming from YAML files (un_wpp.config.yml and un_wpp.sex_ratio.yml).
    - In addition, some views, were created using manual configuration (see un_wpp.manual.yml).
    - To create an explorer, we use the custom-made class ExplorerCreator, which has a function `create`. While these object/functions are custom (they combine ds and ds_full tables in a particular way), some of its logic could be generalized and moved to etl.collections. For more details, please refer to the module utils.py.
    - All the created explorers are combined into a single one, which is then exported.

NOTE: This pipeline assumes that there is a TSV template in owid-content, this should probably be change din the future.
"""

from utils import ExplorerCreator, combine_explorers
from view_edits import ViewEditor

from etl.collections.explorer import hack_metadata_propagation
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# DEFINITIONS
# Rename of age dimension choices: We could alternatively specify these in the YAML file, but this can also be programmatically done.
AGES_POP_LIST = (
    [
        "15-64",
        "15+",
        "18+",
        "1",
        "1-4",
    ]
    + [f"{i}-{i+4}" for i in range(5, 20, 5)]
    + [f"{i}-{i+9}" for i in range(20, 100, 10)]
    + ["100+"]
)
AGES_POP = {age: f"{age.replace('-', '–')} years" if age != "1" else "At age 1" for age in AGES_POP_LIST}

# Sex ratio
AGES_SR = {
    **{str(age): f"At age {age}" for age in ["5", "10", "15"] + list(range(20, 100, 10))},
    "100+": "At age 100 and over",
}

# Deaths
AGES_DEATHS_LIST = [f"{i}-{i+4}" for i in range(5, 20, 5)] + [f"{i}-{i+9}" for i in range(20, 100, 10)] + ["100+"]
AGES_DEATHS = {age: f"{age.replace('-', '–')} years" for age in AGES_POP_LIST}


def run() -> None:
    # Load datasets: ds (contains data 1950-2023) and ds_full (contains data 1950-2100, i.e. includes projections)
    ds = paths.load_dataset("un_wpp")
    ds_full = paths.load_dataset("un_wpp_full")

    # Build object to deal with explorers creation. It is a wrapper around our classic create_explorer function.
    explorer_creator = ExplorerCreator(paths, ds, ds_full)

    # Default config: This config contains the default metadata for most explorers. Exceptions are sex_ratio, which needs other names for certain dimension choices, and manual views.
    config_default = paths.load_explorer_config()

    # Object used to edit view configs: Some of the views need extra-curation (this includes adding map brackets, renaming titles, etc.)
    view_editor = ViewEditor(map_brackets_yaml=paths.side_file("map_brackets.yml"))

    # HACK
    tbs = _get_tables(explorer_creator)

    #################################################################################################
    # Create individual explorers (building blocks)
    #
    # There are various tables required for this explorer. We create one explorer for each of them.
    # Later, we combine them into a single one.
    # Note that all configs have `indicator` dimension, as a way to "hack" the indicator name
    # into the view dimension config.
    #################################################################################################

    ########## Population explorer
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
        explorer_name="population-and-demography",
    )
    view_editor.edit_views_pop(explorer_pop)
    hack_metadata_propagation(
        explorer_pop,
        tbs,
    )

    # Save explorer (upsert to DB)
    ########## Dependency ratio explorer
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
    view_editor.edit_views_dr(explorer_dep)
    hack_metadata_propagation(
        explorer_dep,
        tbs,
    )

    ########## Sex ratio explorer
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
    view_editor.edit_views_sr(explorer_sr)
    hack_metadata_propagation(
        explorer_sr,
        tbs,
    )

    ########## Migration explorer
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
    view_editor.edit_views_mig(explorer_mig)
    hack_metadata_propagation(
        explorer_mig,
        tbs,
    )

    ########## Deaths explorer
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
    view_editor.edit_views_deaths(explorer_deaths)
    hack_metadata_propagation(
        explorer_deaths,
        tbs,
    )

    ########## Births explorer
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
    view_editor.edit_views_b(explorer_b)
    hack_metadata_propagation(
        explorer_b,
        tbs,
    )

    ########## Median age explorer
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
    view_editor.edit_views_ma(explorer_ma)
    hack_metadata_propagation(
        explorer_ma,
        tbs,
    )

    ########## Life expectancy explorer
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
                "0": "At birth",
                "15": "Aged 15",
                "65": "Aged 65",
                "80": "Aged 80",
            }
        },
    )
    view_editor.edit_views_le(explorer_le)
    hack_metadata_propagation(
        explorer_le,
        tbs,
    )

    ########## Fertility rate explorer
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
    view_editor.edit_views_fr(explorer_fr)
    hack_metadata_propagation(
        explorer_fr,
        tbs,
    )

    ########## Manual explorer: views with grouped indicators, and others
    explorer_manual = explorer_creator.create_manual(
        config=paths.load_explorer_config("un_wpp.manual.config.yml"),
    )
    view_editor.edit_views_manual(explorer_manual)
    hack_metadata_propagation(
        explorer_manual,
        tbs,
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
    explorer.sort_indicators(
        [
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
    )

    # # Save explorer (upsert to DB)
    explorer.save(tolerate_extra_indicators=True)


def _get_tables(explorer_creator):
    tbs = []
    for tname in explorer_creator.ds_proj.table_names:
        tb = explorer_creator.ds_proj.read(tname, load_data=False)
        cols = []
        for col in tb.columns:
            if tb[col].m.dimensions and (tb[col].m.dimensions["variant"] in {"low", "medium", "high"}):
                cols.append(col)
        tb_ = tb[cols]
        tbs.append(tb_)
    for tname in explorer_creator.ds.table_names:
        tb = explorer_creator.ds.read(tname, load_data=False)
        cols = []
        for col in tb.columns:
            if tb[col].m.dimensions and (tb[col].m.dimensions["variant"] in {"estimates"}):
                cols.append(col)
        tb_ = tb[cols]
        tbs.append(tb_)
    return tbs
