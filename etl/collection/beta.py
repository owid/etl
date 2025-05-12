"""The code here was developed for population-and-demography explorer.

However, I think there are bits of this that could be migrated into etl.collection so that others can use it. It might need some cleaning, testing, and documenting.

I also think that these functions should both work for MDIMs and Explorers!

Relevant functions:

* `combine_explorers`: Combine multiple explorers into a single one.


TODOs:

- Testing
- Consolidate combine_explorers and combine_mdims into one solution: combine_collection
- Integrate `combine_*` functions into etl.helpers.PathFinder. That's because we should use create_mdim and create_explorer (they incorporate validation of collection), which is good if has access to PathFinder (needs to access schema, dependencies, etc.).

USE CASES:
combine_explorers: etl/steps/export/multidim/covid/latest/covid.py
combine_mdims: etl/steps/export/multidim/dummy/latest/dummy.py
"""

from copy import deepcopy
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, Union

import pandas as pd
from structlog import get_logger

from etl.collection.explorer import Explorer
from etl.collection.model import Collection
from etl.collection.model.dimension import Dimension, DimensionChoice
from etl.collection.multidim import create_collection_from_config

log = get_logger()

COLLECTION_SLUG = "_collection"
COLLECTION_TITLE = "Collection"


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
    explorer = create_collection_from_config(
        config=explorer_config,
        dependencies=dependencies if dependencies is not None else set(),
        catalog_path=catalog_path,
        validate_schema=False,
        explorer=True,
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


def combine_collections(
    collections: List[Union[Collection, Explorer]],
    collection_name: str,
    config: Optional[Dict[str, Any]] = None,
    dependencies: Optional[Set[str]] = None,
    force_collection_dimension: bool = False,
    collection_dimension_name: Optional[str] = None,
    collection_choices_names: Optional[List[str]] = None,
    is_explorer: Optional[bool] = None,
) -> Union[Collection, Explorer]:
    """Combine multiple collections (MDIMs or Explorers) into a single one.

    This function serves as a unified interface to combine either Explorers
    or MDIMs (Collections), abstracting the common logic between the two.

    Args:
        collections: List of collections (either all MDIMs or all Explorers) to combine
        collection_name: Name of the resulting combined collection
        config: Configuration for the combined collection
        dependencies: Set of dependencies for the combined collection
        force_collection_dimension: If True, adds a dimension to identify the source collection
            even if there are no duplicate views
        collection_dimension_name: Name for the dimension that identifies the source collection
            (defaults to "MDIM" for Collections or "Explorer" for Explorers)
        collection_choices_names: Names for the choices in the source dimension
            (should match the length of collections)
        is_explorer: Force the result to be an Explorer (True) or MDIM (False).
            If None (default), inferred from the input collections.

    Returns:
        A combined Collection or Explorer, matching the type of the input collections

    Notes:
        - All collections must have the same dimensions structure (slug, name, etc.)
        - Choice conflicts are resolved by renaming conflicting choices
        - If duplicate views exist, a source dimension is automatically added
    """
    # Check that there are at least 2 collections to combine
    assert len(collections) > 0, "No collections to combine."
    assert len(collections) > 1, "At least two collections should be provided."

    # Determine collection type if not specified
    if is_explorer is None:
        is_explorer = all(isinstance(c, Explorer) for c in collections)
        if not (is_explorer or all(not isinstance(c, Explorer) for c in collections)):
            raise ValueError("All collections must be of the same type (either all Explorers or all Collections)")

    # Set appropriate default dimension name based on collection type
    if collection_dimension_name is None:
        collection_dimension_name = COLLECTION_TITLE

    # Check that all collections have the same dimensions structure
    collection_dims = None
    for collection in collections:
        dimensions_flatten = [
            {k: v for k, v in dim.to_dict().items() if k != "choices"} for dim in collection.dimensions
        ]
        if collection_dims is None:
            collection_dims = dimensions_flatten
        else:
            assert (
                collection_dims == dimensions_flatten
            ), "Dimensions are not the same across collections. Please review that dimensions are listed in the same order, have the same slugs, names, description, etc."

    # Check for checkbox dimensions in the first collection
    # TODO: Implement support for checkboxes when merging
    for dim in collections[0].dimensions:
        if dim.ui_type == "checkbox" and is_explorer:
            raise NotImplementedError("Checkbox dimensions are not supported yet for Explorers.")

    # Detect duplicate views
    seen_dims = set()
    has_duplicate_views = False
    for collection in collections:
        # duplicate views within a collection
        collection.check_duplicate_views()
        # duplicate views across collections
        for view in collection.views:
            dims = tuple(view.dimensions.items())
            if dims in seen_dims:
                has_duplicate_views = True
                break
            seen_dims.add(dims)

    # Add source dimension if needed
    if has_duplicate_views or force_collection_dimension:
        for i, collection in enumerate(collections):
            if collection_choices_names is not None:
                assert len(collection_choices_names) == len(
                    collections
                ), "Length of collection_choices_names must match the number of collections"
                choice_name = collection_choices_names[i]
            else:
                choice_name = collection.title.get("title", collection.short_name)

            dimension_collection = Dimension(
                slug=COLLECTION_SLUG,
                name=collection_dimension_name,
                choices=[
                    DimensionChoice(slug=collection.short_name, name=choice_name),
                ],
            )
            collection.dimensions = [dimension_collection] + collection.dimensions
            for v in collection.views:
                v.dimensions[COLLECTION_SLUG] = collection.short_name

    # Create dictionary with collections for tracking
    collections_by_id = {str(i): deepcopy(collection) for i, collection in enumerate(collections)}

    # Build dataframe with all choices
    df_choices, cols_choices = _build_df_choices(collections_by_id)

    # Combine dimensions (use first collection as template)
    dimensions = _combine_dimensions(
        df_choices=df_choices,
        cols_choices=cols_choices,
        collection=collections[0].copy(),
    )

    # Track modifications (useful later for views)
    choice_slug_changes = _extract_choice_slug_changes(df_choices)

    # Update views based on changes to choice slugs
    collections_by_id = _update_choice_slugs_in_views(choice_slug_changes, collections_by_id)

    # Collect all views
    views = []
    for _, collection in collections_by_id.items():
        views.extend(collection.views)

    # Create catalog path
    assert isinstance(collections[0].catalog_path, str), "Catalog path is not set. Please set it before saving."
    catalog_path = collections[0].catalog_path.split("#")[0] + "#" + collection_name

    # Ensure config has minimal required fields
    if config is None:
        config = {}

    # Make sure there is title and default_selection. If not given, use default values.
    default_title = {
        "title": f"Combined Collection: {collection_name}",
        "title_variant": "Use a YAML to define these attributes",
    }
    if not is_explorer:
        if "title" not in config:
            config["title"] = default_title
        else:
            config["title"] = {**default_title, **config["title"]}
        if "default_selection" not in config:
            config["default_selection"] = collections[0].default_selection
    else:
        if "config" not in config:
            config["config"] = {}
        if "explorerTitle" not in config["config"]:
            config["config"]["explorerTitle"] = default_title["title"]
        if "explorerSubtitle" not in config["config"]:
            config["config"]["explorerSubtitle"] = default_title["title_variant"]

    # Set dimensions and views
    config["dimensions"] = dimensions
    config["views"] = views

    # Create the combined collection
    combined = create_collection_from_config(
        config=config,
        dependencies=dependencies if dependencies is not None else set(),
        catalog_path=catalog_path,
        validate_schema=True if not is_explorer else False,
        explorer=is_explorer,
    )

    # Log any conflicts that were resolved
    df_conflict = df_choices.loc[df_choices["in_conflict"]]
    if not df_conflict.empty:
        log.warning("Choice slug conflicts resolved")
        for (dimension_slug, choice_slug), group in df_conflict.groupby(["dimension_slug", "slug_original"]):
            log.warning(f"(dimension={dimension_slug}, choice={choice_slug})")
            for _, subgroup in group.groupby("choice_slug_id"):
                collection_ids = subgroup["collection_id"].unique().tolist()
                collection_names = [collections_by_id[i].short_name for i in collection_ids]
                record = subgroup[cols_choices].drop_duplicates().to_dict("records")
                assert len(record) == 1, "Unexpected, please report!"
                log.warning(f" Collections {collection_names} map to {record[0]}")

    return combined


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
    df_choices: pd.DataFrame, cols_choices: List[str], collection: Union[Explorer, Collection]
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


def _update_choice_slugs_in_views(choice_slug_changes, collection_by_id) -> Mapping[str, Union[Collection, Explorer]]:
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


def _build_df_choices(collections_by_id: Mapping[str, Union[Collection, Explorer]]) -> Tuple[pd.DataFrame, List[str]]:
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
