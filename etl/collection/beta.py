"""The code here was developed for population-and-demography explorer.

However, I think there are bits of this that could be migrated into etl.collection so that others can use it. It might need some cleaning, testing, and documenting.

I also think that these functions should both work for MDIMs and Explorers!

Relevant functions:

* `create_explorer_experimental`: Create an explorer based on a table and a YAML config. It is a wrapper around `expand_config` and `combine_config_dimensions`. We could consider replacing the existing `paths.create_explorer` with this one. Currently table is optional, and YAML is mandatory.
* `combine_explorers`: Combine multiple explorers into a single one.


TODOs:

- Testing
- Consolidate combine_explorers and combine_mdims into one solution: combine_collection
- Integrate `combine_*` functions into etl.helpers.PathFinder. That's because we should use create_mdim and create_explorer (they incorporate validation of collection), which is good if has access to PathFinder (needs to access schema, dependencies, etc.).

USE CASES:
combine_explorers: etl/steps/export/multidim/covid/latest/covid.py
combine_mdims: etl/steps/export/multidim/dummy/latest/dummy.py
"""

import inspect
from copy import deepcopy
from typing import Any, Callable, Dict, List, Mapping, Optional, Set, Tuple, Union

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.collection.explorer import Explorer, create_explorer, expand_config
from etl.collection.model import Dimension, DimensionChoice
from etl.collection.multidim import Multidim, combine_config_dimensions, create_mdim
from etl.collection.utils import has_duplicate_table_names
from etl.helpers import PathFinder

log = get_logger()


log = get_logger()

MDIM_SLUG = "mdim"
MDIM_TITLE = "MDIM"

# def explorer_to_mdim(explorer: Explorer, mdim_name: str):
#     """TODO: Experimental."""
#     config = explorer.to_dict()
#     config_mdim = {
#         "title": {
#             "title": "Population",
#             "title_variant": "by age and age group",
#         },
#         "default_selection": ["United States", "India", "China", "Indonesia", "Pakistan"],
#         "dimensions": config["dimensions"],
#         "views": config["views"],
#     }
#     return paths.create_mdim(
#         config=config_mdim,
#         mdim_name=mdim_name,
#     )


def create_explorer_experimental(
    paths: PathFinder,
    config_yaml: Dict[str, Any],
    tb: Optional[Table] = None,
    indicator_names: Optional[Union[str, List[str]]] = None,
    dimensions: Optional[Union[List[str], Dict[str, Union[List[str], str]]]] = None,
    common_view_config: Optional[Dict[str, Any]] = None,
    indicators_slug: Optional[str] = None,
    indicator_as_dimension: bool = False,
    explorer_name: Optional[str] = None,
    choice_renames: Optional[Dict[str, Union[Dict[str, str], Callable]]] = None,
    catalog_path_full: bool = False,
) -> Explorer:
    """Experimental smarter explorer creation.

    Args:
    -----
    tb: Table
        Table object with data. This data will be expanded for the given indicators and dimensions.
    config_yaml: Dict[str, Any]
        Configuration YAML for the explorer. This can contain dimension renames, etc. Even views.
    indicator_names: Optional[Union[str, List[str]]]
        Name of the indicators to be used. If None, all indicators are used.
    dimensions: Optional[Union[List[str], Dict[str, Union[List[str], str]]]]
        Dimensions to be used. If None, all dimensions are used. If a list, all dimensions are used with the given names. If a dict, key represent dimensions to use and values choices to use. Note that if a list or dictionary is given, all dimensions must be present.
    common_view_config: Optional[Dict[str, Any]]
        Common view configuration to be used for all views.
    indicators_slug: Optional[str]
        Slug to be used for the indicators. A default is used.
    indicator_as_dimension: bool
        If True, the indicator is treated as a dimension.
    explorer_name: Optional[str]
        Name of the explorer. If None, the table name is used.
    choice_renames: Optional[Dict[str, Union[Dict[str, str], Callable]]]
        Renames for choices. If a dictionary, the key is the dimension slug and the value is a dictionary with the original slug as key and the new name as value. If a callable, the function should return the new name for the given slug. NOTE: If the callable returns None, the name is not changed.
    catalog_path_full: bool
        If True, the full path is used for the catalog. If False, a shorter version is used (e.g. table#indicator` or `dataset/table#indicator`).


    NOTE: This function is experimental for this step, but could be used in other steps as well. Consider migrating to etl.collection.explorer once we are happy with it.
    """
    config = deepcopy(config_yaml)

    # Read from table (programatically expand)
    config_auto = None
    if tb is not None:
        # Check if there are collisions between table names
        # TODO: We should do this at indicator level. Default to 'table' for all indicators, except when there is a collision, then go to 'dataset', otherwise go to 'full'
        expand_path_mode = "table"
        if catalog_path_full:
            expand_path_mode = "full"
        elif has_duplicate_table_names(paths.dependencies):
            expand_path_mode = "dataset"

        # Bake config automatically from table
        config_auto = expand_config(
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
            config_dimensions=config_auto["dimensions"],
            config_dimensions_yaml=config["dimensions"],
        )

        # Add views
        config["views"] += config_auto["views"]

        # Default explorer name is table name
        if explorer_name is None:
            explorer_name = tb.m.short_name
    elif explorer_name is None:
        explorer_name = "unknown"
        paths.log.info(f"No table provided. Explorer name is not set. Using '{explorer_name}'.")

    # Create actual explorer
    explorer = paths.create_explorer(
        config=config,
        explorer_name=explorer_name,
    )

    # Prune unused dimensions
    explorer.prune_dimension_choices()

    # Rename choice names if given
    if choice_renames is not None:
        for dim in explorer.dimensions:
            if dim.slug in choice_renames:
                renames = choice_renames[dim.slug]
                for choice in dim.choices:
                    if isinstance(renames, dict):
                        if choice.slug in renames:
                            choice.name = renames[choice.slug]
                    elif inspect.isfunction(renames):
                        rename = renames(choice.slug)
                        if rename:
                            choice.name = renames(choice.slug)
                    else:
                        raise ValueError("Invalid choice_renames format.")

    return explorer


def combine_explorers(
    explorers: List[Explorer],
    explorer_name: str,
    config: Dict[str, str],
    dependencies: Optional[Set[str]] = None,
):
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
    # TODO: Need to run concrete tests when merging checkboxes: should have the same exact choices, same choice_slug_true
    for dim in explorers[0].dimensions:
        if dim.ui_type == "checkbox":
            raise NotImplementedError("Checkbox dimensions are not supported yet.")

    # 0) Preliminary work #
    # Create dictionary with explorers, so to have identifiers for them
    explorers_by_id = {str(i): deepcopy(explorer) for i, explorer in enumerate(explorers)}

    # Build dataframe with all choices. Each row provides details of a choices, and explorer identifier and the dimension slug
    df_choices, cols_choices = _build_df_choices(explorers_by_id)

    # 1) Combine dimensions (use first explorer as container/reference) #
    dimensions = _combine_dimensions(
        df_choices=df_choices,
        cols_choices=cols_choices,
        collection=explorers[0].copy(),
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
    explorer_config = {
        "config": config,
        "dimensions": dimensions,
        "views": views,
        # "catalog_path": catalog_path,
    }
    explorer = create_explorer(
        config=explorer_config,
        dependencies=dependencies if dependencies is not None else set(),
        catalog_path=catalog_path,
    )

    # 5) Announce conflicts
    df_conflict = df_choices.loc[df_choices["in_conflict"]]
    if not df_conflict.empty:
        log.warning("Choice slug conflicts resolved")
        for (dimension_slug, choice_slug), group in df_conflict.groupby(["dimension_slug", "slug_original"]):
            # Now group by 'value' to see which col3 values correspond to each unique 'value'
            log.warning(f"(dimension={dimension_slug}, choice={choice_slug})")
            for _, subgroup in group.groupby("choice_slug_id"):
                explorer_ids = subgroup["collection_id"].unique().tolist()
                explorer_names = [explorers_by_id[i].short_name for i in explorer_ids]
                record = subgroup[cols_choices].drop_duplicates().to_dict("records")
                assert len(record) == 1, "Unexpected, please report!"
                log.warning(f" Explorers {explorer_names} map to {record[0]}")

    return explorer


def combine_mdims(
    mdims: List[Multidim],
    mdim_name: str,
    config: Optional[Dict[str, Any]] = None,
    dependencies: Optional[Set[str]] = None,
    mdim_dimension_name: Optional[str] = None,
    mdim_choices_names: Optional[List[str]] = None,
    force_mdim_dimension: Optional[bool] = False,
) -> Multidim:
    """Combine multiple MDIMs into a single one.

    Notes:
    - All MDIMs should have the same dimensions (slug, name, etc.).
    - Dimensions can vary across explorers, that's fine. This function consolidates all of them. If there are multiple slugs in use with different names, this function will rename them to preserve uniqueness.

    Args:
        mdims: List[Multidim]
            List of MDIMs to be combined.
        mdim_name: str
            Name of the combined MDIM.
        config: Optional[Dict[str, Any]]
            Configuration for the combined MDIM. If None, a default configuration is used. We recommend using a config YAML file and calling `paths.load_mdim_config()` to obtain a good default for config.
        dependencies: Optional[Set[str]]
            Set of dependencies for the combined MDIM. If None, an empty set is used. TODO: This should probably be required.
        mdim_dimension_name: Optional[str]
            Name of the new dimension added if there are duplicate views. This dimension will identify each MDIM and will be shown as the first MDIM dropdown. If None, a default name is used.
        mdim_choices_names: Optional[List[str]]
            List of choice names of the MDIM dimension (see `mdim_dimension_name`). Each choice stands for an MDIM in `mdims` (it should be a 1-to-1 correspondence). The length of this list should match that from `mdims`. If None, a default name is used.
        force_mdim_dimension: Optional[bool]
            If True, a new dimension to identify each MDIM is added even if there are no duplicate views. Its default value is `False`. This argument will be ignored if there are duplicate views!

    TODO:
        - Consolidate with `combine_explorers` function.
        - Integrate into `PathFinder`.
    """

    # Check that there are at least 2 MDIMs to combine
    assert len(mdims) > 0, "No MDIMs to combine."
    assert len(mdims) > 1, "At least two MDIMs should be provided."

    # Check that all MDIMs have the same dimensions (slug, name, etc.)
    mdims_dims = None
    for m in mdims:
        dimensions_flatten = [{k: v for k, v in dim.to_dict().items() if k != "choices"} for dim in m.dimensions]
        if mdims_dims is None:
            mdims_dims = dimensions_flatten
        else:
            assert (
                mdims_dims == dimensions_flatten
            ), "Dimensions are not the same across MDIMs. Please review that dimensions are listed in the same order, have the same slugs, names, description, etc."

    # If there are duplicate views, add dimension to differentiate them!
    if mdim_dimension_name is None:
        mdim_dimension_name = MDIM_TITLE
    if mdim_choices_names is not None:
        assert len(mdim_choices_names) == len(
            mdims
        ), "Length of `mdim_choices_names` should equal the length of `mdimd`."

    seen_dims = set()
    has_duplicate_views = False
    for mdim in mdims:
        mdim.check_duplicate_views()
        for view in mdim.views:
            dims = tuple(view.dimensions.items())
            if dims in seen_dims:
                has_duplicate_views = True
                break
            seen_dims.add(dims)
    ## Add dimension to differentiate them if applicable
    if has_duplicate_views or force_mdim_dimension:
        for i, mdim in enumerate(mdims):
            if mdim_choices_names is not None:
                choice = mdim_choices_names[i]
            else:
                choice = mdim.title["title"]

            dimension_mdim = Dimension(
                slug=MDIM_SLUG,
                name=mdim_dimension_name,
                choices=[
                    DimensionChoice(slug=mdim.short_name, name=choice),
                ],
            )
            mdim.dimensions = [dimension_mdim] + mdim.dimensions
            for v in mdim.views:
                v.dimensions[MDIM_SLUG] = mdim.short_name

    # 0) Preliminary work #
    # Create dictionary with MDIMs, so to have identifiers for them
    mdims_by_id = {str(i): deepcopy(mdim) for i, mdim in enumerate(mdims)}

    # Build dataframe with all choices. Each row provides details of a choices, and explorer identifier and the dimension slug
    df_choices, cols_choices = _build_df_choices(mdims_by_id)

    # 1) Combine dimensions (use first explorer as container/reference) #
    dimensions = _combine_dimensions(
        df_choices=df_choices,
        cols_choices=cols_choices,
        collection=mdims[0].copy(),
    )

    # 2) Combine views #
    # Track modifications (useful later for views)
    choice_slug_changes = _extract_choice_slug_changes(df_choices)
    # Update explorer views (based on changes on choice slugs)
    mdims_by_id = _update_choice_slugs_in_views(choice_slug_changes, mdims_by_id)
    # Collect views
    views = []
    for _, explorer in mdims_by_id.items():
        explorer_views = explorer.views
        views.extend(explorer_views)

    # 3) Ad-hoc change: update explorer_name #
    assert isinstance(mdims[0].catalog_path, str), "Catalog path is not set. Please set it before saving."
    catalog_path = mdims[0].catalog_path.split("#")[0] + "#" + mdim_name

    # 4) Prepare MDIM config
    if config is None:
        config = {}

    # Make sure there is title and default_selection. If not given, use default values.
    default_title = {"title": f"Combined MDIM: {mdim_name}", "title_variant": "Use a YAML to define these attributes"}
    if "title" not in config:
        config["title"] = default_title
    else:
        config["title"] = {**default_title, **config["title"]}
    if "default_selection" not in config:
        config["default_selection"] = mdims[0].default_selection

    # Set dimensions and views
    config["dimensions"] = dimensions
    config["views"] = views

    # 4) Create final explorer #
    mdim = create_mdim(
        config=config,
        dependencies=dependencies if dependencies is not None else set(),
        catalog_path=catalog_path,
    )

    # 5) Announce conflicts
    df_conflict = df_choices.loc[df_choices["in_conflict"]]
    if not df_conflict.empty:
        log.warning("Choice slug conflicts resolved")
        for (dimension_slug, choice_slug), group in df_conflict.groupby(["dimension_slug", "slug_original"]):
            # Now group by 'value' to see which col3 values correspond to each unique 'value'
            log.warning(f"(dimension={dimension_slug}, choice={choice_slug})")
            for _, subgroup in group.groupby("choice_slug_id"):
                collection_ids = subgroup["collection_id"].unique().tolist()
                explorer_names = [mdims_by_id[i].short_name for i in collection_ids]
                record = subgroup[cols_choices].drop_duplicates().to_dict("records")
                assert len(record) == 1, "Unexpected, please report!"
                log.warning(f" MDIMs {explorer_names} map to {record[0]}")

    return mdim


def _extract_choice_slug_changes(df_choices) -> Dict[str, Any]:
    # Track modifications (useful later for views)
    slug_changes = (
        df_choices.loc[df_choices["in_conflict"]]
        .groupby(["collection_id", "dimension_slug"])
        .apply(lambda x: dict(zip(x["slug_original"], x["slug"])), include_groups=False)
        .unstack("collection_id")
        .to_dict()
    )

    return slug_changes


def _combine_dimensions(
    df_choices: pd.DataFrame, cols_choices: List[str], collection: Union[Explorer, Multidim]
) -> List[Dimension]:
    """Combine dimensions from different explorers"""
    # Dimension bucket
    dimensions = collection.dimensions.copy()

    # Drop duplicates
    df_choices = df_choices.drop_duplicates(subset=cols_choices + ["slug", "dimension_slug"])

    # Iterate over each dimension and update the list of choices
    for dimension in dimensions:
        df_dim_choices = df_choices.loc[
            df_choices["dimension_slug"] == dimension.slug, cols_choices + ["slug"]
        ].drop_duplicates()

        assert (
            len(df_dim_choices) == df_dim_choices["slug"].nunique()
        ), f"Duplicate slugs in dimension {dimension.slug} choices."

        # Raw choices
        choices = df_dim_choices.to_dict("records")

        # Build choices
        dimension.choices = [DimensionChoice.from_dict(c) for c in choices]

    return dimensions


def _update_choice_slugs_in_views(choice_slug_changes, collection_by_id) -> Mapping[str, Union[Multidim, Explorer]]:
    """Access each explorer, and update choice slugs in views"""
    for collection_id, change in choice_slug_changes.items():
        # Get collection
        collection = collection_by_id[collection_id]

        # Get views as dataframe for easy processing
        df_views_dimensions = pd.DataFrame([view.dimensions for view in collection.views])

        # FUTURE: this needs to change in order to support checkboxes
        df_views_dimensions = df_views_dimensions.astype("string")

        # Process views
        df_views_dimensions = df_views_dimensions.replace(change)

        # Bring back views to collections
        views_dimensions = df_views_dimensions.to_dict("records")
        for view, view_dimensions in zip(collection.views, views_dimensions):
            # cast keys to str to satisfy type requirements
            view.dimensions = {str(key): value for key, value in view_dimensions.items()}
    return collection_by_id


def _build_df_choices(collections_by_id: Mapping[str, Union[Multidim, Explorer]]) -> Tuple[pd.DataFrame, List[str]]:
    # Collect all choices in a dataframe: choice_slug, choice_name, ..., collection_id, dimension_slug.
    records = []
    for i, explorer in collections_by_id.items():
        for dim in explorer.dimensions:
            for choice in dim.choices:
                records.append(
                    {
                        **choice.to_dict(),
                        "dimension_slug": dim.slug,
                        "collection_id": i,
                    }
                )
    # This needs to change to support checkboxes
    df_choices = pd.DataFrame(records).astype("string")

    # Get column names of fields from choice objects
    cols_choices = [col for col in df_choices.columns if col not in ["slug", "collection_id", "dimension_slug"]]

    # For each choice slug, assign an ID (choice_slug_id) that identifies that "slug flavour". E.g. if a slug has different names (or descriptions) across explorers, each "flavour" will have a different ID. This will be useful later to identify conflicts & rename slugs.
    df_choices["choice_slug_id"] = (
        df_choices.groupby(["dimension_slug", "slug"], group_keys=False)
        .apply(
            lambda g: pd.Series(
                pd.factorize(pd.Series(zip(*[g[c] for c in cols_choices])))[0],
                index=g.index,
            ),
            include_groups=False,
        )
        .astype("string")
    )
    # Mark choice slugs as "in conflict": A choice slug maps to different names (or descriptions) across explorers
    df_choices["in_conflict"] = (
        df_choices.groupby(["dimension_slug", "slug"], as_index=False)["choice_slug_id"].transform("nunique").ne(1)
    )

    # Mark choices as duplicates: Same choice properties for a given dimension
    df_choices["duplicate"] = df_choices.duplicated(subset=cols_choices + ["slug", "dimension_slug"])

    # Drop duplicates, except those that are in conflict
    df_choices = df_choices.loc[~df_choices["duplicate"] | df_choices["in_conflict"]]

    # Rename slugs for choices that are duplicates. 'slug' for final slugs, 'slug_original' keeps the original slug
    df_choices.loc[:, "slug_original"] = df_choices.loc[:, "slug"].copy()
    mask = df_choices["in_conflict"]
    df_choices.loc[mask, "slug"] = df_choices.loc[mask, "slug"] + "__" + df_choices.loc[mask, "choice_slug_id"]

    return df_choices, cols_choices
