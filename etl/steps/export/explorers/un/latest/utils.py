"""The code here was developed for population-and-demography explorer.

However, I think there are bits of this that could be migrated into etl.collections so that others can use it. It might need some cleaning, testing, and documenting.

Relevant functions:

* `create_explorer_experimental`: Create an explorer based on a table and a YAML config. It is a wrapper around `expand_config` and `combine_config_dimensions`. We could consider replacing the existing `paths.create_explorer` with this one. Currently table is optional, and YAML is mandatory.
* `combine_explorers`: Combine multiple explorers into a single one.

TODO: We should add testing!
"""

import inspect
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.collections.explorer import Explorer, expand_config
from etl.collections.model import Dimension, DimensionChoice
from etl.collections.multidim import combine_config_dimensions
from etl.collections.utils import has_duplicate_table_names
from etl.helpers import PathFinder

log = get_logger()
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


    NOTE: This function is experimental for this step, but could be used in other steps as well. Consider migrating to etl.collections.explorer once we are happy with it.
    """
    from copy import deepcopy

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
    # TODO: Need to run concrete tests when merging checkboxes: should have the same exact choices, same choice_slug_true
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

    # 5) Announce conflicts
    df_conflict = df_choices.loc[df_choices["in_conflict"]]
    if not df_conflict.empty:
        log.warning("Choice slug conflicts resolved")
        for (dimension_slug, choice_slug), group in df_conflict.groupby(["dimension_slug", "slug_original"]):
            # Now group by 'value' to see which col3 values correspond to each unique 'value'
            log.warning(f"(dimension={dimension_slug}, choice={choice_slug})")
            for _, subgroup in group.groupby("choice_slug_id"):
                explorer_ids = subgroup["explorer_id"].unique().tolist()
                explorer_names = [explorers_by_id[i].explorer_name for i in explorer_ids]
                record = subgroup[cols_choices].drop_duplicates().to_dict("records")
                assert len(record) == 1, "Unexpected, please report!"
                log.warning(f" Explorers {explorer_names} map to {record[0]}")

    return explorer


def _extract_choice_slug_changes(df_choices) -> Dict[str, Any]:
    # Track modifications (useful later for views)
    slug_changes = (
        df_choices.loc[df_choices["in_conflict"]]
        .groupby(["explorer_id", "dimension_slug"])
        .apply(lambda x: dict(zip(x["slug_original"], x["slug"])), include_groups=False)
        .unstack("explorer_id")
        .to_dict()
    )

    return slug_changes


def _combine_dimensions(df_choices: pd.DataFrame, cols_choices: List[str], explorer: Explorer) -> List[Dimension]:
    """Combine dimensions from different explorers"""
    # Dimension bucket
    dimensions = explorer.dimensions.copy()

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


def _build_df_choices(explorers_by_id: Dict[str, Explorer]) -> Tuple[pd.DataFrame, List[str]]:
    # Collect all choices in a dataframe: choice_slug, choice_name, ..., explorer_id, dimension_slug.
    records = []
    for i, explorer in explorers_by_id.items():
        for dim in explorer.dimensions:
            for choice in dim.choices:
                records.append(
                    {
                        **choice.to_dict(),
                        "dimension_slug": dim.slug,
                        "explorer_id": i,
                    }
                )
    # This needs to change to support checkboxes
    df_choices = pd.DataFrame(records).astype("string")

    # Get column names of fields from choice objects
    cols_choices = [col for col in df_choices.columns if col not in ["slug", "explorer_id", "dimension_slug"]]

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


#########################################################################################
# More specific utils
#
# The functions below are a bit more specific to this step, so maybe harder to generalize
#########################################################################################


class ExplorerCreator:
    """This class is particular to this step.

    This step relies on two datasets that are particular. One contains just estimates (1950-2023), and the other contains projections (1950-2100).
    """

    def __init__(self, paths, ds, ds_proj):
        self.paths = paths
        self.ds = ds
        self.ds_proj = ds_proj
        self.tbs = {"proj": {}, "estimates": {}}

    @property
    def all_tables(self):
        return [tt for t in self.tbs.values() for tt in t.values()]

    def table(self, table_name: str):
        if table_name not in self.tbs:
            self.tbs["estimates"][table_name] = self.ds.read(table_name, load_data=False)
        return self.tbs["estimates"][table_name]

    def table_proj(self, table_name: str):
        if table_name not in self.tbs:
            self.tbs["proj"][table_name] = self.ds_proj.read(table_name, load_data=False)
        return self.tbs["proj"][table_name]

    def create_manual(self, config: Dict[str, Any], **kwargs):
        explorer = create_explorer_experimental(self.paths, config, indicator_as_dimension=True, **kwargs)
        return explorer

    def create(
        self,
        table_name: str,
        dimensions: Dict[str, Union[List[str], str]],
        dimensions_proj: Optional[Dict[str, Union[List[str], str]]] = None,
        **kwargs,
    ):
        """Creates an explorer based on `tb` (1950-2023) and `tb_proj` (1950-2100)."""
        self.paths.log.info(f"Creating explorer for {table_name}")

        # Load tables
        tb = self.table(table_name)
        tb_proj = self.table_proj(table_name)

        # Explorer with estimates
        explorer = create_explorer_experimental(
            self.paths, tb=tb, dimensions=dimensions, indicator_as_dimension=True, **kwargs
        )

        # Explorer with projections
        if dimensions_proj is not None:
            dimensions.update(dimensions_proj)
        else:
            dimensions["variant"] = ["medium", "high", "low"]

        explorer_proj = create_explorer_experimental(
            self.paths, tb=tb_proj, dimensions=dimensions, indicator_as_dimension=True, **kwargs
        )

        explorer = combine_explorers(
            explorers=[explorer, explorer_proj],
            explorer_name=explorer.explorer_name,
            config=explorer.config,
        )

        return explorer
