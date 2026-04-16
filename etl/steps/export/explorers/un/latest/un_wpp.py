"""
This step creates the population and demography explorer. It uses MDIM-based configuration, and some custom processing.

Strategy:

    - This explorer relies on multiple tables from the UN WPP dataset. Therefore, we first create individual explorers for each table and then combine them.
    - While some of the metadata is inherited from Garden/Grapher, some of it is set manually in the YAML files or programmatically once explorers are created.
    - Most of the explorers are created programmatically, with slight edits coming from YAML files (un_wpp.config.yml and un_wpp.sex_ratio.yml).
    - In addition, some views, were created using manual configuration (see un_wpp.manual.yml).
    - To create an explorer, we use the custom-made class ExplorerCreator, which has a function `create`. While these object/functions are custom (they combine ds and ds_full tables in a particular way), some of its logic could be generalized and moved to etl.collection. For more details, please refer to the module utils.py.
    - All the created explorers are combined into a single one, which is then exported.

NOTE: This pipeline assumes that there is a TSV template in owid-content, this should probably be change din the future.
"""

from utils import ExplorerCreator
from view_edits import ViewEditor

from etl.collection import combine_collections
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# DEFINITIONS
# Rename of age dimension choices: We could alternatively specify these in the YAML file, but this can also be programmatically done.
AGES_POP_LIST = (
    [
        "15-49",
        "15-64",
        "15+",
        "18+",
        "1",
        "1-4",
    ]
    + [f"{i}-{i + 4}" for i in range(5, 20, 5)]
    + [f"{i}-{i + 9}" for i in range(20, 100, 10)]
    + ["100+"]
)
AGES_POP = {age: f"{age.replace('-', '–')} years" if age != "1" else "At age 1" for age in AGES_POP_LIST}

# Sex ratio
AGES_SR = {
    **{str(age): f"At age {age}" for age in ["5", "10", "15"] + list(range(20, 100, 10))},
    "100+": "At age 100 and over",
}

# Deaths
AGES_DEATHS_LIST = [f"{i}-{i + 4}" for i in range(5, 20, 5)] + [f"{i}-{i + 9}" for i in range(20, 100, 10)] + ["100+"]
AGES_DEATHS = {age: f"{age.replace('-', '–')} years" for age in AGES_POP_LIST}


def run() -> None:
    # Load datasets: ds (contains data 1950-2023) and ds_full (contains data 1950-2100, i.e. includes projections)
    ds = paths.load_dataset("un_wpp")
    ds_full = paths.load_dataset("un_wpp_full")

    # Build object to deal with explorers creation. It is a wrapper around our classic create_explorer function.
    explorer_creator = ExplorerCreator(paths, ds, ds_full)

    # Default config: This config contains the default metadata for most explorers. Exceptions are sex_ratio, which needs other names for certain dimension choices, and manual views.
    config_default = paths.load_collection_config()

    # Object used to edit view configs: Some of the views need extra-curation (this includes adding map brackets, renaming titles, etc.)
    view_editor = ViewEditor(map_brackets_yaml=paths.side_file("map_brackets.yml"))

    #################################################################################################
    # Create individual explorers (building blocks)
    #
    # There are various tables required for this explorer. We create one explorer for each of them.
    # Later, we combine them into a single one.
    # Note that all configs have `indicator` dimension, as a way to "hack" the indicator name
    # into the view dimension config.
    #################################################################################################

    ########## Population explorer
    explorer_pop = explorer_creator.create_with_grouped_projections(
        table_name="population",
        config=config_default,
        indicator_names=["population", "population_change", "population_density"],
        dimensions={
            "age": ["all", "0", "0-4", "0-14", "0-24"] + AGES_POP_LIST,
            "sex": "*",
        },
        choice_renames={"age": AGES_POP},
        short_name="population-and-demography",
    )
    # `ds_grapher=ds` gives the view editor access to the Jinja-expanded
    # title_public / subtitle per indicator, which it copies onto the grouped views.
    view_editor.edit_views_pop(explorer_pop, ds_grapher=ds)

    ########## Dependency ratio explorer
    explorer_dep = explorer_creator.create_with_grouped_projections(
        table_name="dependency_ratio",
        config=config_default,
        indicator_names=["dependency_ratio"],
        dimensions={
            "age": "*",
            "sex": "*",
        },
    )
    view_editor.edit_views_dr(explorer_dep, ds_grapher=ds)

    ########## Sex ratio explorer
    explorer_sr = explorer_creator.create_with_grouped_projections(
        table_name="sex_ratio",
        config=paths.load_collection_config("un_wpp.sex_ratio.config.yml"),
        indicator_names=["sex_ratio"],
        dimensions={
            "age": ["all", "0"] + list(AGES_SR.keys()),
            "sex": "*",
        },
        choice_renames={"age": AGES_SR},
    )
    view_editor.edit_views_sr(explorer_sr, ds_grapher=ds)

    ########## Migration explorer
    explorer_mig = explorer_creator.create_with_grouped_projections(
        table_name="migration",
        config=config_default,
        indicator_names=["net_migration", "net_migration_rate"],
        dimensions={
            "age": "*",
            "sex": "*",
        },
        choice_renames={"age": AGES_SR},
    )
    view_editor.edit_views_mig(explorer_mig, ds_grapher=ds)

    ########## Deaths explorer
    # Split into two sub-explorers because the source only has low/high projections for
    # `death_rate`, not `deaths` (counts). Hiding the empty options for `deaths` avoids
    # dropdown choices that would render as estimates-only views. `death_rate` is only
    # available at age=all, so the two explorers use different `age` dimensions.
    explorer_deaths_counts = explorer_creator.create_with_grouped_projections(
        table_name="deaths",
        config=config_default,
        indicator_names=["deaths"],
        projection_variants=["medium"],
        # age=0 and age=0-4 are intentionally excluded here: those views live under
        # the `infant_deaths` / `child_deaths` indicator dropdown in the manual
        # explorer and pointing at the same catalog path from both places makes the
        # explorer system emit a `_1`-suffixed ySlug for the duplicate (owid-grapher#6362).
        dimensions={
            "age": ["all"] + AGES_DEATHS_LIST,
            "sex": "*",
        },
        choice_renames={"age": AGES_DEATHS},
    )
    view_editor.edit_views_deaths(explorer_deaths_counts, ds_grapher=ds)
    explorer_deaths_rate = explorer_creator.create_with_grouped_projections(
        table_name="deaths",
        config=config_default,
        indicator_names=["death_rate"],
        dimensions={
            "age": ["all"],
            "sex": ["all"],
        },
    )
    view_editor.edit_views_deaths(explorer_deaths_rate, ds_grapher=ds)

    ########## Births explorer
    explorer_b = explorer_creator.create_with_grouped_projections(
        table_name="births",
        config=config_default,
        indicator_names=["births", "birth_rate"],
        dimensions={
            "age": "*",
            "sex": "*",
        },
        choice_renames={"age": lambda x: f"Women aged {x} years" if x != "all" else None},
    )
    view_editor.edit_views_b(explorer_b, ds_grapher=ds)

    ########## Median age explorer
    explorer_ma = explorer_creator.create_with_grouped_projections(
        table_name="median_age",
        config=config_default,
        indicator_names=["median_age"],
        dimensions={
            "age": ["all"],
            "sex": "*",
        },
        choice_renames={"age": lambda x: {"all": "None"}.get(x, None)},
    )
    view_editor.edit_views_ma(explorer_ma, ds_grapher=ds)

    ########## Life expectancy explorer
    # At birth: all three projection scenarios (low/medium/high)
    explorer_le_birth = explorer_creator.create_with_grouped_projections(
        table_name="life_expectancy",
        config=config_default,
        indicator_names=["life_expectancy"],
        dimensions={
            "age": ["0"],
            "sex": "*",
        },
        choice_renames={"age": {"0": "At birth"}},
    )
    view_editor.edit_views_le(explorer_le_birth, ds_grapher=ds)

    # At older ages: medium projections only. UN WPP's low/medium/high scenarios
    # share identical mortality, but the source only publishes life expectancy
    # projections at ages 15/65/80 for the medium variant.
    explorer_le_other = explorer_creator.create_with_grouped_projections(
        table_name="life_expectancy",
        config=config_default,
        indicator_names=["life_expectancy"],
        projection_variants=["medium"],
        dimensions={
            "age": ["15", "65", "80"],
            "sex": "*",
        },
        choice_renames={
            "age": {
                "15": "Aged 15",
                "65": "Aged 65",
                "80": "Aged 80",
            }
        },
    )
    view_editor.edit_views_le(explorer_le_other, ds_grapher=ds)

    ########## Fertility rate explorer
    explorer_fr = explorer_creator.create_with_grouped_projections(
        table_name="fertility_rate",
        config=config_default,
        indicator_names=["fertility_rate"],
        dimensions={
            "age": "*",
            "sex": "*",
        },
        choice_renames={"age": lambda x: f"Women aged {x} years" if x != "all" else None},
    )
    view_editor.edit_views_fr(explorer_fr, ds_grapher=ds)

    ########## Growth rate explorer
    explorer_growth = explorer_creator.create_with_grouped_projections(
        table_name="growth_rate",
        config=config_default,
        indicator_names=["growth_rate"],
        dimensions={
            "age": ["all"],
            "sex": ["all"],
        },
    )
    view_editor.edit_views_rates(explorer_growth, ds_grapher=ds)

    ########## Natural change rate explorer
    explorer_natchange = explorer_creator.create_with_grouped_projections(
        table_name="natural_change_rate",
        config=config_default,
        indicator_names=["natural_change_rate"],
        dimensions={
            "age": ["all"],
            "sex": ["all"],
        },
    )
    view_editor.edit_views_rates(explorer_natchange, ds_grapher=ds)

    ########## Manual explorer: views with grouped indicators, and others
    explorer_manual = explorer_creator.create_manual(
        config=paths.load_collection_config("un_wpp.manual.config.yml"),
    )
    view_editor.edit_views_manual(explorer_manual, ds_grapher=ds)

    #################################################################################################
    # Combine explorers
    #################################################################################################

    # List with all explorers
    explorers = [
        explorer_pop,
        explorer_dep,
        explorer_sr,
        explorer_mig,
        explorer_deaths_counts,
        explorer_deaths_rate,
        explorer_b,
        explorer_ma,
        explorer_le_birth,
        explorer_le_other,
        explorer_fr,
        explorer_growth,
        explorer_natchange,
        # manual views explorers
        explorer_manual,
    ]

    # Combine them into single explorer
    c = combine_collections(
        collections=explorers,
        collection_name="population-and-demography",
        config=config_default,
    )

    # Sort indicator choices
    c.sort_indicators(
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
    c.save(tolerate_extra_indicators=True)
