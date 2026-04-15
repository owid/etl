"""Methods and tools to create collections of indicators."""

import inspect
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any, TypeAlias, TypeVar, cast

from owid.catalog import Table
from structlog import get_logger

from etl.collection.core.combine import combine_collections, combine_config_dimensions
from etl.collection.core.expand import expand_config
from etl.collection.core.utils import create_collection_from_config
from etl.collection.explorer import Explorer
from etl.collection.model.core import Collection
from etl.collection.utils import has_duplicate_table_names

# Initialize logger.
log = get_logger()


# Define type variables for common patterns
T = TypeVar("T", covariant=True)
Listable: TypeAlias = T | Sequence[T]


def create_collection(
    config_yaml: dict[str, Any],
    dependencies: set[str],
    catalog_path: str,
    tb: list[Table] | Table | None = None,
    indicator_names: Listable[list[str] | None] | str = None,
    dimensions: Listable[list[str] | Mapping[str, list[str] | str] | None] = None,
    common_view_config: Listable[dict[str, Any] | None] = None,
    indicators_slug: str | None = None,
    indicator_as_dimension: bool = False,
    choice_renames: Listable[Mapping[str, dict[str, str] | Callable] | None] = None,
    catalog_path_full: bool = False,
    explorer: bool = False,
) -> Collection:
    """
    Create a collection that supports multiple tables and corresponding list parameters.

    When ``tb`` is a list of tables, one sub-collection is created per table and
    they are combined via ``combine_collections``. The parameters
    ``indicator_names``, ``dimensions``, and ``common_view_config`` can also be
    lists (one element per table); if they are single values they are broadcast.

    Args:
        config_yaml: Configuration dictionary (YAML-based). Provides the base
            structure including dimensions, views, and explorer settings. In the
            multi-table path this config is applied twice — once per sub-collection
            and once after combining — so it acts as the authoritative source for
            dimension/choice metadata (names, descriptions, ordering).
        dependencies: Set of dependency URIs for the collection.
        catalog_path: Catalog path for the collection (e.g. "namespace#short_name").
        tb: Single table or list of tables. When a list is provided, one
            sub-collection is created per table and they are combined.
        indicator_names: Indicator column names to include. A single value is
            broadcast to all tables; a list must match the number of tables.
        dimensions: Dimension specification (column names or {col: values} mapping).
            A single value is broadcast to all tables; a list must match the number
            of tables.
        common_view_config: Extra view-level config merged into every generated view.
            A single value is broadcast to all tables; a list must match the number
            of tables.
        indicators_slug: Slug used for the indicators dimension (applies to all tables).
        indicator_as_dimension: Whether to treat indicators as a dimension (applies to
            all tables).
        choice_renames: Rename display names of dimension choices. Maps dimension
            slugs to either a ``{choice_slug: new_name}`` dict or a
            ``callable(choice_slug) -> new_name | None``. When ``tb`` is a list,
            a single dict renames choices in the final collection; a list of dicts
            renames per table (keys are auto-remapped if slugs change during
            combining). Always takes precedence over names from ``config_yaml``.
        catalog_path_full: Whether to use the full catalog path for indicators
            (applies to all tables).
        explorer: Whether to create an Explorer (True) or a Collection (False).

    Returns:
        Collection (or Explorer) object.

    Raises:
        ValueError: If list parameters don't match the number of tables.
    """
    if isinstance(tb, list):
        num_tables = len(tb)

        # Build indicator_names_
        indicator_names_ = _get_indicator_names(
            indicator_names=indicator_names,
            num_tables=num_tables,
        )
        # Build dimensions_
        dimensions_ = _get_dimensions(
            dimensions=dimensions,
            num_tables=num_tables,
        )
        # Build common_view_config
        common_view_config_ = _get_common_view_config(
            common_view_config=common_view_config,
            num_tables=num_tables,
        )
        # Determine choice_renames mode: single dict (final) vs list (per-table).
        is_list_renames = isinstance(choice_renames, list)

        if is_list_renames:
            # Per-table renames: validate and distribute.
            choice_renames_ = _get_choice_renames(
                choice_renames=choice_renames,
                num_tables=num_tables,
            )
        else:
            # Single dict (or None): don't pass to sub-collections.
            choice_renames_ = [None] * num_tables

        # Create collections for each table.
        collections = []
        indicator_as_dimension_ = False
        for i in range(num_tables):
            if isinstance(indicator_names_[i], list) and len(cast(list, indicator_names_[i])) > 1:
                indicator_as_dimension_ = True

            c = create_collection_single_table(
                config_yaml=config_yaml,
                dependencies=dependencies,
                catalog_path=catalog_path,
                tb=tb[i],
                indicator_names=indicator_names_[i],
                dimensions=dimensions_[i],
                common_view_config=common_view_config_[i],
                indicators_slug=indicators_slug,
                indicator_as_dimension=indicator_as_dimension or indicator_as_dimension_,
                choice_renames=choice_renames_[i],
                catalog_path_full=catalog_path_full,
                explorer=explorer,
            )
            collections.append(c)

        if len(collections) == 1:
            if not is_list_renames and choice_renames is not None:
                # Single dict was deferred — apply now.
                _rename_choices(collections[0], cast("dict[str, dict[str, str] | Callable]", choice_renames))
            return collections[0]

        # Combine all collections, capturing slug changes for remapping.
        slug_changes: dict = {}
        c = combine_collections(
            collections=collections,
            catalog_path=catalog_path,
            config=config_yaml,
            is_explorer=explorer,
            _slug_changes_out=slug_changes,
        )

        # Apply choice_renames to the final combined collection.
        if is_list_renames:
            # Per-table renames: remap keys using slug changes, then apply.
            remapped = _remap_choice_renames(choice_renames_, slug_changes)
            if remapped:
                _rename_choices(c, remapped)
        elif choice_renames is not None:
            # Single dict: apply directly to the final collection.
            _rename_choices(c, cast("dict[str, dict[str, str] | Callable]", choice_renames))
    else:
        # Single table case - call original function directly
        c = create_collection_single_table(
            config_yaml=config_yaml,
            dependencies=dependencies,
            catalog_path=catalog_path,
            tb=tb,
            indicator_names=cast("list[str] | None", indicator_names),
            dimensions=cast("list[str] | dict[str, list[str] | str] | None", dimensions),
            common_view_config=cast("dict[str, Any] | None", common_view_config),
            indicators_slug=indicators_slug,
            indicator_as_dimension=indicator_as_dimension,
            choice_renames=cast("dict[str, dict[str, str] | Callable] | None", choice_renames),
            catalog_path_full=catalog_path_full,
            explorer=explorer,
        )

    return c


# Functions to handle listable parameters.
## Given a list or single value, ensure it is actually a list!
def _bake_listable(fct_single, obj, obj_name, num_tables):
    if fct_single(obj):
        # Single value - replicate for all tables
        return [obj] * num_tables
    # 2) list[list[str]] -> list[list[str]]
    elif _is_multi(fct_single, obj, obj_name, num_tables):
        return obj
    else:
        raise TypeError(
            f"Parameter '{obj_name}' must be a list of lists or a single value, but got {type(obj)} with value {obj}"
        )


def _is_multi(fct, obj, obj_name, num_tables):
    if isinstance(obj, list) and all(fct(i) for i in obj):
        # This is a list of indicator_names (one per table)
        if len(obj) != num_tables:
            raise ValueError(
                f"Parameter '{obj_name}' is a list of length {len(obj)}, "
                f"but expected length {num_tables} to match number of tables"
            )
        return True
    return False


IndicatorTypeReturn = list[list[str]] | list[str] | list[None]


def _get_indicator_names(
    num_tables: int,
    indicator_names: Listable[list[str] | None] | str = None,
) -> IndicatorTypeReturn:
    def _is_single(obj) -> bool:
        return (
            obj is None
            or (isinstance(obj, str))
            or (isinstance(obj, list) and len(obj) > 0 and all(isinstance(i, str) for i in obj))
        )

    return cast(
        IndicatorTypeReturn,
        _bake_listable(
            fct_single=_is_single,
            obj=indicator_names,
            obj_name="indicator_names",
            num_tables=num_tables,
        ),
    )


DimensionTypeReturn = list[list[str]] | list[dict[str, list[str] | str]] | list[None]


def _get_dimensions(
    num_tables: int,
    dimensions: Listable[list[str] | Mapping[str, list[str] | str] | None] = None,
) -> DimensionTypeReturn:
    # Distribute dimensions parameter
    def _is_single(obj) -> bool:
        return (
            obj is None
            or (isinstance(obj, list) and all(isinstance(x, str) for x in obj))
            or (
                isinstance(obj, dict)
                and all(isinstance(k, str) for k in obj.keys())
                and all(isinstance(v, (str, list)) for v in obj.values())
                and all(all(isinstance(x, str) for x in v) if isinstance(v, list) else True for v in obj.values())
            )
        )

    return cast(
        DimensionTypeReturn,
        _bake_listable(
            fct_single=_is_single,
            obj=dimensions,
            obj_name="dimensions",
            num_tables=num_tables,
        ),
    )


CommonConfigTypeReturn = list[dict[str, Any]] | list[None]


def _get_common_view_config(
    num_tables: int,
    common_view_config: Listable[dict[str, Any] | None] = None,
) -> CommonConfigTypeReturn:
    def _is_single(obj) -> bool:
        return obj is None or (isinstance(obj, dict))

    return cast(
        CommonConfigTypeReturn,
        _bake_listable(
            fct_single=_is_single,
            obj=common_view_config,
            obj_name="common_view_config",
            num_tables=num_tables,
        ),
    )


ChoiceRenamesTypeReturn = list[dict[str, Any]] | list[None]


def _get_choice_renames(
    num_tables: int,
    choice_renames: Listable[Mapping[str, dict[str, str] | Callable] | None] = None,
) -> ChoiceRenamesTypeReturn:
    def _is_single(obj) -> bool:
        return obj is None or (
            isinstance(obj, dict)
            and all(isinstance(k, str) and (isinstance(v, dict) or inspect.isfunction(v)) for k, v in obj.items())
        )

    return cast(
        ChoiceRenamesTypeReturn,
        _bake_listable(
            fct_single=_is_single,
            obj=choice_renames,
            obj_name="choice_renames",
            num_tables=num_tables,
        ),
    )


# Create collection from a single table
def create_collection_single_table(
    config_yaml: dict[str, Any],
    dependencies: set[str],
    catalog_path: str,
    tb: Table | None = None,
    indicator_names: str | list[str] | None = None,
    dimensions: list[str] | dict[str, list[str] | str] | None = None,
    common_view_config: dict[str, Any] | None = None,
    indicators_slug: str | None = None,
    indicator_as_dimension: bool = False,
    choice_renames: dict[str, dict[str, str] | Callable] | None = None,
    catalog_path_full: bool = False,
    explorer: bool = False,
) -> Collection:
    config = deepcopy(config_yaml)

    # Read from table (programmatically expand)
    config_auto = None
    if tb is not None:
        # Check if there are collisions between table names
        expand_path_mode = _get_expand_path_mode(dependencies, catalog_path_full)

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
            # Should be `config_dimensions_yaml=config.get("dimensions", {}),`?
        )

        # Add views
        config["views"] += config_auto["views"]

    # Create actual explorer
    if explorer:
        coll = create_collection_from_config(
            config=config,
            dependencies=dependencies,
            catalog_path=catalog_path,
            validate_schema=False,
            explorer=True,
        )
    else:
        coll = create_collection_from_config(
            config=config,
            dependencies=dependencies,
            catalog_path=catalog_path,
        )

    # Rename choice names if given
    _rename_choices(coll, choice_renames)

    # Cast if required
    if explorer:
        return cast(Explorer, coll)

    return coll


def _get_expand_path_mode(dependencies, catalog_path_full):
    # TODO: We should do this at indicator level. Default to 'table' for all indicators, except when there is a collision, then go to 'dataset', otherwise go to 'full'
    expand_path_mode = "table"
    if catalog_path_full:
        expand_path_mode = "full"
    elif has_duplicate_table_names(dependencies):
        expand_path_mode = "dataset"
    return expand_path_mode


def _rename_choices(coll: Collection, choice_renames: dict[str, dict[str, str] | Callable] | None = None):
    if choice_renames is not None:
        for dim in coll.dimensions:
            if dim.slug in choice_renames:
                renames = choice_renames[dim.slug]
                for choice in dim.choices:
                    if isinstance(renames, dict):
                        if choice.slug in renames:
                            choice.name = renames[choice.slug]  # ty: ignore[invalid-assignment]
                    elif inspect.isfunction(renames) or callable(renames):
                        rename = renames(choice.slug)
                        if rename:
                            choice.name = rename
                    else:
                        raise ValueError("Invalid choice_renames format.")


def _remap_choice_renames(
    choice_renames_per_table: list[dict[str, dict[str, str] | Callable] | None],
    slug_changes: dict[str, Any],
) -> dict[str, dict[str, str] | Callable]:
    """Merge per-table choice_renames into a single dict, remapping choice slugs
    to account for conflict-resolution renaming done by ``combine_collections``.

    ``slug_changes`` has structure
    ``{collection_id: {dimension_slug: {original_slug: renamed_slug}}}``.
    """
    merged: dict[str, dict[str, str] | Callable] = {}

    for i, renames in enumerate(choice_renames_per_table):
        if renames is None:
            continue
        collection_id = str(i)
        for dim_slug, dim_renames in renames.items():
            # Slug changes for this collection + dimension (may be empty).
            changes = slug_changes.get(collection_id, {}).get(dim_slug, {})

            if isinstance(dim_renames, dict):
                # Dict renames: remap keys from original → final slug.
                remapped_dict: dict[str, str] = {}
                for orig_choice_slug, new_name in cast("dict[str, str]", dim_renames).items():
                    final_slug = changes.get(orig_choice_slug, orig_choice_slug)
                    remapped_dict[final_slug] = new_name
                # Merge into the dimension entry (later tables override for
                # the same final slug, which is the expected behavior).
                existing = merged.get(dim_slug)
                if isinstance(existing, dict):
                    existing.update(remapped_dict)
                else:
                    merged[dim_slug] = remapped_dict

            elif inspect.isfunction(dim_renames) or callable(dim_renames):
                # Callable renames: wrap to reverse-map final slug → original
                # before calling the user's function.
                reverse_changes = {v: k for k, v in changes.items()}
                original_func = dim_renames

                def _make_remapped_func(rev: dict[str, str], fn: Callable) -> Callable:
                    def remapped(slug: str) -> str | None:
                        return fn(rev.get(slug, slug))

                    return remapped

                merged[dim_slug] = _make_remapped_func(reverse_changes, original_func)

    return merged
