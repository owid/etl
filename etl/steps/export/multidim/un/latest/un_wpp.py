"""This step creates the population and demography MDIM. It uses multiple tables from the UN WPP dataset and combines them into a single multi-dimensional collection.

Strategy:

    - This MDIM relies on multiple tables from the UN WPP dataset. Therefore, we first create individual sub-collections for each table and then combine them.
    - While some of the metadata is inherited from Garden/Grapher, some of it is set manually in the YAML files or programmatically once sub-collections are created.
    - Most of the sub-collections are created programmatically, with slight edits coming from YAML files (un_wpp.config.yml and un_wpp.sex_ratio.config.yml).
    - In addition, some views were created using manual configuration (see un_wpp.manual.config.yml).
    - To create a sub-collection, we use the custom-made class MDIMCreator, which has a function `create`. While these object/functions are custom (they combine ds and ds_full tables in a particular way), some of its logic could be generalized and moved to etl.collection. For more details, please refer to the module utils.py.
    - All the created sub-collections are combined into a single MDIM, which is then exported.

This step was migrated from the legacy explorer at `export://explorers/un/latest/un_wpp` to replace the Explorer with a Multidim (MDIM) collection. The MDIM drops the TSV/owid-content template dependency of the Explorer pipeline and emits a JSON config that grapher renders natively.
"""

from utils import MDIMCreator
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

    # Build object to deal with sub-collection creation. It is a wrapper around our classic create_collection function.
    mdim_creator = MDIMCreator(paths, ds, ds_full)

    # Default config: This config contains the default metadata for most sub-collections. Exceptions are sex_ratio, which needs other names for certain dimension choices, and manual views.
    config_default = paths.load_collection_config()

    # Object used to edit view configs: Some of the views need extra-curation (this includes adding map brackets, renaming titles, etc.)
    view_editor = ViewEditor(map_brackets_yaml=paths.side_file("map_brackets.yml"))

    #################################################################################################
    # Create individual sub-collections (building blocks)
    #
    # There are various tables required for this MDIM. We create one sub-collection for each of them.
    # Later, we combine them into a single one.
    # Note that all configs have `indicator` dimension, as a way to "hack" the indicator name
    # into the view dimension config.
    #################################################################################################

    ########## Population sub-collection
    collection_pop = mdim_creator.create_with_grouped_projections(
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
    view_editor.edit_views_pop(collection_pop, ds_grapher=ds)

    ########## Dependency ratio sub-collection
    collection_dep = mdim_creator.create_with_grouped_projections(
        table_name="dependency_ratio",
        config=config_default,
        indicator_names=["dependency_ratio"],
        dimensions={
            "age": "*",
            "sex": "*",
        },
    )
    view_editor.edit_views_dr(collection_dep, ds_grapher=ds)

    ########## Sex ratio sub-collection
    collection_sr = mdim_creator.create_with_grouped_projections(
        table_name="sex_ratio",
        config=paths.load_collection_config("un_wpp.sex_ratio.config.yml"),
        indicator_names=["sex_ratio"],
        dimensions={
            "age": ["all", "0"] + list(AGES_SR.keys()),
            "sex": "*",
        },
        choice_renames={"age": AGES_SR},
    )
    view_editor.edit_views_sr(collection_sr, ds_grapher=ds)

    ########## Migration sub-collection
    collection_mig = mdim_creator.create_with_grouped_projections(
        table_name="migration",
        config=config_default,
        indicator_names=["net_migration", "net_migration_rate"],
        dimensions={
            "age": "*",
            "sex": "*",
        },
        choice_renames={"age": AGES_SR},
    )
    view_editor.edit_views_mig(collection_mig, ds_grapher=ds)

    ########## Deaths sub-collection
    # Split into two sub-collections because the source only has low/high projections for
    # `death_rate`, not `deaths` (counts). Hiding the empty options for `deaths` avoids
    # dropdown choices that would render as estimates-only views. `death_rate` is only
    # available at age=all, so the two sub-collections use different `age` dimensions.
    collection_deaths_counts = mdim_creator.create_with_grouped_projections(
        table_name="deaths",
        config=config_default,
        indicator_names=["deaths"],
        projection_variants=["medium"],
        dimensions={
            "age": ["all", "0", "0-4"] + AGES_DEATHS_LIST,
            "sex": "*",
        },
        choice_renames={"age": {**AGES_DEATHS, "0": "Under 1 year", "0-4": "Under 5 years"}},
    )
    view_editor.edit_views_deaths(collection_deaths_counts, ds_grapher=ds)
    collection_deaths_rate = mdim_creator.create_with_grouped_projections(
        table_name="deaths",
        config=config_default,
        indicator_names=["death_rate"],
        dimensions={
            "age": ["all"],
            "sex": ["all"],
        },
    )
    view_editor.edit_views_deaths(collection_deaths_rate, ds_grapher=ds)

    ########## Births sub-collection
    collection_b = mdim_creator.create_with_grouped_projections(
        table_name="births",
        config=config_default,
        indicator_names=["births", "birth_rate"],
        dimensions={
            "age": "*",
            "sex": "*",
        },
        choice_renames={"age": lambda x: f"Women aged {x} years" if x != "all" else None},
    )
    view_editor.edit_views_b(collection_b, ds_grapher=ds)

    ########## Median age sub-collection
    collection_ma = mdim_creator.create_with_grouped_projections(
        table_name="median_age",
        config=config_default,
        indicator_names=["median_age"],
        dimensions={
            "age": ["all"],
            "sex": "*",
        },
        choice_renames={"age": lambda x: {"all": "None"}.get(x, None)},
    )
    view_editor.edit_views_ma(collection_ma, ds_grapher=ds)

    ########## Life expectancy sub-collection
    # At birth: all three projection scenarios (low/medium/high)
    collection_le_birth = mdim_creator.create_with_grouped_projections(
        table_name="life_expectancy",
        config=config_default,
        indicator_names=["life_expectancy"],
        dimensions={
            "age": ["0"],
            "sex": "*",
        },
        choice_renames={"age": {"0": "At birth"}},
    )
    view_editor.edit_views_le(collection_le_birth, ds_grapher=ds)

    # At older ages: medium projections only. UN WPP's low/medium/high scenarios
    # share identical mortality, but the source only publishes life expectancy
    # projections at ages 15/65/80 for the medium variant.
    collection_le_other = mdim_creator.create_with_grouped_projections(
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
    view_editor.edit_views_le(collection_le_other, ds_grapher=ds)

    ########## Fertility rate sub-collection
    collection_fr = mdim_creator.create_with_grouped_projections(
        table_name="fertility_rate",
        config=config_default,
        indicator_names=["fertility_rate"],
        dimensions={
            "age": "*",
            "sex": "*",
        },
        choice_renames={"age": lambda x: f"Women aged {x} years" if x != "all" else None},
    )
    view_editor.edit_views_fr(collection_fr, ds_grapher=ds)

    ########## Growth rate sub-collection
    collection_growth = mdim_creator.create_with_grouped_projections(
        table_name="growth_rate",
        config=config_default,
        indicator_names=["growth_rate"],
        dimensions={
            "age": ["all"],
            "sex": ["all"],
        },
    )
    view_editor.edit_views_rates(collection_growth, ds_grapher=ds)

    ########## Natural change rate sub-collection
    collection_natchange = mdim_creator.create_with_grouped_projections(
        table_name="natural_change_rate",
        config=config_default,
        indicator_names=["natural_change_rate"],
        dimensions={
            "age": ["all"],
            "sex": ["all"],
        },
    )
    view_editor.edit_views_rates(collection_natchange, ds_grapher=ds)

    ########## Manual sub-collection: views with grouped indicators, and others
    collection_manual = mdim_creator.create_manual(
        config=paths.load_collection_config("un_wpp.manual.config.yml"),
    )
    view_editor.edit_views_manual(collection_manual, ds_grapher=ds)

    #################################################################################################
    # Combine sub-collections
    #################################################################################################

    # List with all sub-collections
    collections = [
        collection_pop,
        collection_dep,
        collection_sr,
        collection_mig,
        collection_deaths_counts,
        collection_deaths_rate,
        collection_b,
        collection_ma,
        collection_le_birth,
        collection_le_other,
        collection_fr,
        collection_growth,
        collection_natchange,
        # manual views sub-collection
        collection_manual,
    ]

    # Combine them into a single MDIM
    c = combine_collections(
        collections=collections,
        collection_name="population-and-demography",
        config=config_default,
    )

    # Sort indicator choices. `sort_choices` is defined on the `Collection` base class
    # (the Explorer variant of this step used the Explorer-only `sort_indicators`,
    # which is a thin wrapper around `sort_choices({"indicator": order})`).
    c.sort_choices(
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

    # # Save MDIM (upsert to DB)
    c.save(tolerate_extra_indicators=True)
