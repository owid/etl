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
- [x] Sex ratio

total done: 4/22
"""

from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from owid.catalog import Table

from etl.collections.explorer import Explorer, expand_config
from etl.collections.model import Dimension, DimensionChoice
from etl.collections.multidim import combine_config_dimensions
from etl.collections.utils import has_duplicate_table_names

# from etl.files import yaml_dump
# from etl.db import get_engine
from etl.helpers import PathFinder

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


# etlr multidim
def run() -> None:
    # Load dataset
    ds = paths.load_dataset("un_wpp")
    ds_full = paths.load_dataset("un_wpp_full")

    # 1) Population
    config = paths.load_explorer_config()

    explorer_pop = create_explorer(
        tb=ds.read("population", load_data=False),
        config_yaml=config,
        indicator_names=["population", "population_change", "population_density"],
        dimensions={
            "age": ["all", "0", "0-4", "0-14", "0-24"] + AGES_POP_LIST,
            "sex": "*",
            "variant": ["estimates"],
        },
        choice_renames={"age": AGES_POP},
        explorer_name="population-and-demography",
    )

    explorer_pop_full = create_explorer(
        tb=ds_full.read("population", load_data=False),
        config_yaml=config,
        indicator_names=["population", "population_change", "population_density"],
        dimensions={
            "age": ["all", "0", "0-4", "0-14", "0-24"] + AGES_POP_LIST,
            "sex": "*",
            "variant": ["medium", "high", "low"],
        },
        choice_renames={"age": AGES_POP},
        explorer_name="population-and-demography",
    )

    # 2) DEPENDENCY RATIO
    explorer_dep = create_explorer(
        tb=ds.read("dependency_ratio", load_data=False),
        config_yaml=config,
        indicator_names=["dependency_ratio"],
        dimensions={
            "age": "*",
            "sex": "*",
            "variant": ["estimates"],
        },
        indicator_as_dimension=True,
    )

    # 3) SEX RATIO
    explorer_sr = create_explorer(
        tb=ds.read("sex_ratio", load_data=False),
        config_yaml=paths.load_explorer_config("un_wpp.sex_ratio.config.yml"),
        indicator_names=["sex_ratio"],
        dimensions={
            "age": ["all", "0"] + list(AGES_SR.keys()),
            "sex": "*",
            "variant": ["estimates"],
        },
        indicator_as_dimension=True,
        choice_renames={"age": AGES_SR},
    )

    # DEBUGGING
    # with open("/home/lucas/repos/etl/etl/steps/export/explorers/un/latest/un_wpp2.config.yml", "w") as f:
    #     yaml_dump(config, f)

    # Export
    # Combine explorers
    # TODO: falla si tenemos POP_FULL junto con SR!
    explorers = [explorer_pop, explorer_pop_full, explorer_dep]
    explorer = combine_explorers(
        explorers=explorers,
        explorer_name="population-and-demography",
        config=explorer_pop.config,
    )

    explorer.save(tolerate_extra_indicators=True)

    # Translate into mdim
    # mdim = explorer_to_mdim(explorer, "population-and-demography")
    # mdim.save()


def explorer_to_mdim(explorer: Explorer, mdim_name: str):
    """TODO: Experimental."""
    config = explorer.to_dict()
    config_mdim = {
        "title": {
            "title": "Population",
            "title_variant": "by age and age group",
        },
        "default_selection": ["United States", "India", "China", "Indonesia", "Pakistan"],
        "dimensions": config["dimensions"],
        "views": config["views"],
    }
    return paths.create_mdim(
        config=config_mdim,
        mdim_name=mdim_name,
    )


def create_explorer(
    tb: Table,
    config_yaml: Dict[str, Any],
    indicator_names: Optional[Union[str, List[str]]] = None,
    dimensions: Optional[Union[List[str], Dict[str, Union[List[str], str]]]] = None,
    common_view_config: Optional[Dict[str, Any]] = None,
    indicators_slug: Optional[str] = None,
    indicator_as_dimension: bool = False,
    explorer_name: Optional[str] = None,
    choice_renames: Optional[Dict[str, Dict[str, str]]] = None,
    catalog_path_full: bool = False,
) -> Explorer:
    """Experimental."""
    from copy import deepcopy

    config = deepcopy(config_yaml)

    # Check if there are collisions between table names
    # TODO: We should do this at indicator level. Default to 'table' for all indicators, except when there is a collision, then go to 'dataset', otherwise go to 'full'
    expand_path_mode = "table"
    if catalog_path_full:
        expand_path_mode = "full"
    elif has_duplicate_table_names(paths.dependencies):
        expand_path_mode = "dataset"
    # print(expand_path_mode)

    # Bake config automatically from table
    config_new = expand_config(
        tb=tb,
        indicator_names=indicator_names,
        dimensions=dimensions,
        common_view_config=common_view_config,
        indicators_slug=indicators_slug,
        indicator_as_dimension=indicator_as_dimension,
        expand_path_mode=expand_path_mode,
    )
    # Combine & bake dimensions
    config["dimensions"] = combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config["dimensions"],
    )
    # Add views
    config["views"] += config_new["views"]

    # Create actual explorer
    explorer = paths.create_explorer(
        config=config,
        explorer_name=explorer_name,
    )

    # Rename choice names if given
    if choice_renames is not None:
        for dim in explorer.dimensions:
            if dim.slug in choice_renames:
                renames = choice_renames[dim.slug]
                for choice in dim.choices:
                    if choice.slug in renames:
                        choice.name = renames[choice.slug]

    return explorer


def combine_explorers(explorers: List[Explorer], explorer_name: str, config: Dict[str, str]):
    """Combine multiple explorers into a single one.

    Notes:
    - All explorers should have the same dimensions (slug, name, etc.).
    - Dimensions can vary across explorers, that's fine. This function consolidates all of them. If there are multiple slugs in use with different names, this function will rename them to preserve uniqueness.
    - Checkbox dimensions are not supported yet.

    """
    # Check that there are at least 2 explorers to combine
    assert len(explorers) > 0, "No explorers to combine."
    assert len(explorers) > 1, "At least two explorers should be provided."

    # Check that all explorers have the same dimensions (slug, name, etc.)
    explorer_dims = None
    for explorer in explorers:
        dimensions_flatten = [{k: v for k, v in dim.to_dict().items() if k != "choices"} for dim in explorer.dimensions]
        if explorer_dims is None:
            explorer_dims = dimensions_flatten
        else:
            assert (
                explorer_dims == dimensions_flatten
            ), "Dimensions are not the same across explorers. Please review that dimensions are listed in the same order, have the same slugs, names, description, etc."

    # Check that there are no checkbox dimensions (only first explorer, since all dimensions are the same based on previous check)
    for dim in explorers[0].dimensions:
        if dim.ui_type == "checkbox":
            raise NotImplementedError("Checkbox dimensions are not supported yet.")

    # 0) Preliminary work #
    # Create dictionary with explorers, so to have identifiers for them
    explorers_by_id = {str(i): explorer.copy() for i, explorer in enumerate(explorers)}

    # Build dataframe with all choices. Each row provides details of a choices, and explorer identifier and the dimension slug
    df_choices, cols_choices = _build_df_choices(explorers_by_id)

    # 1) Combine dimensions (use first explorer as container/reference) #
    dimensions = _combine_dimensions(
        df_choices=df_choices,
        cols_choices=cols_choices,
        explorer=explorers[0].copy(),
    )

    # 2) Combine views #
    # Track modifications (useful later for views)
    choice_slug_changes = _extract_choice_slug_changes(df_choices)
    # Update explorer views (based on changes on choice slugs)
    explorers_by_id = _update_choice_slugs_in_views(choice_slug_changes, explorers_by_id)
    # Collect views
    views = []
    for _, explorer in explorers_by_id.items():
        explorer_views = explorer.views
        views.extend(explorer_views)

    # 3) Ad-hoc change: update explorer_name #
    assert isinstance(explorers[0].catalog_path, str), "Catalog path is not set. Please set it before saving."
    catalog_path = explorers[0].catalog_path.split("#")[0] + "#" + explorer_name

    # 4) Create final explorer #
    explorer = Explorer(
        config=config,
        dimensions=dimensions,
        views=views,
        _catalog_path=catalog_path,
    )
    return explorer


def _build_df_choices(explorers_by_id: Dict[str, Explorer]) -> Tuple[pd.DataFrame, List[str]]:
    # Collect all choices in a dataframe: choice_slug, choice_name, ..., explorer_id, dimension_slug.
    records = []
    for i, explorer in explorers_by_id.items():
        for dim in explorer.dimensions:
            for choice in dim.choices:
                records.append(
                    {
                        **choice.to_dict(),
                        "explorer_id": i,
                        "dimension_slug": dim.slug,
                    }
                )
    # This needs to change to support checkboxes
    df_choices = pd.DataFrame(records).astype("string")

    # Drop choices that are identical (same slug, same name, etc.)
    cols_choices = [col for col in df_choices.columns if col not in ["explorer_id", "dimension_slug"]]
    df_choices = df_choices.drop_duplicates(subset=cols_choices)

    # Flag choices that have same slug but differ in some of other fields
    df_choices["duplicate"] = df_choices.duplicated(subset=["slug", "dimension_slug"], keep=False)

    # Rename slugs for choices that are duplicates. 'slug' for final slugs, 'slug_original' keeps the original slug
    df_choices.loc[:, "slug_original"] = df_choices.loc[:, "slug"].copy()
    mask = df_choices["duplicate"]
    df_choices.loc[mask, "slug"] = df_choices.loc[mask, "slug"] + "__" + df_choices.loc[mask, "explorer_id"]

    return df_choices, cols_choices


def _extract_choice_slug_changes(df_choices) -> Dict[str, Any]:
    # Track modifications (useful later for views)
    slug_changes = (
        df_choices.loc[df_choices["duplicate"]]
        .groupby(["explorer_id", "dimension_slug"])
        .apply(lambda x: dict(zip(x["slug_original"], x["slug"])), include_groups=False)
        .unstack("explorer_id")
        .to_dict()
    )

    return slug_changes


def _combine_dimensions(df_choices: pd.DataFrame, cols_choices: List[str], explorer: Explorer) -> List[Dimension]:
    """Combine dimensions from different explorers"""
    dimensions = explorer.dimensions.copy()
    for dimension in dimensions:
        df_dim_choices = df_choices.loc[df_choices["dimension_slug"] == dimension.slug, cols_choices].drop_duplicates()

        assert len(df_dim_choices) == df_dim_choices["slug"].nunique(), "Duplicate slugs in dimension choices."

        # Raw choices
        choices = df_dim_choices.to_dict("records")

        # Build choices
        dimension.choices = [DimensionChoice.from_dict(c) for c in choices]

    return dimensions


def _update_choice_slugs_in_views(choice_slug_changes, explorers_by_id):
    """Access each explorer, and update choice slugs in views"""
    for explorer_id, change in choice_slug_changes.items():
        # Get explorer
        explorer = explorers_by_id[explorer_id]

        # Get views as dataframe for easy processing
        df_views_dimensions = pd.DataFrame([view.dimensions for view in explorer.views])

        # FUTURE: this needs to change in order to support checkboxes
        df_views_dimensions = df_views_dimensions.astype("string")

        # Process views
        df_views_dimensions = df_views_dimensions.replace(change)

        # Bring back views to explorers
        views_dimensions = df_views_dimensions.to_dict("records")
        for view, view_dimensions in zip(explorer.views, views_dimensions):
            # cast keys to str to satisfy type requirements
            view.dimensions = {str(key): value for key, value in view_dimensions.items()}
    return explorers_by_id
