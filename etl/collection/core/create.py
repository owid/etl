"""Methods and tools to create collections of indicators."""

import inspect
from copy import deepcopy
from typing import Any, Callable, Mapping, Sequence, TypeAlias, TypeVar, cast

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

    This is a generalization of create_collection that allows tb, indicator_names,
    dimensions, common_view_config, and choice_renames to be lists. When tb is a
    list of tables, it creates multiple collections and combines them.

    Args:
        config_yaml: Configuration dictionary
        dependencies: Set of dependencies
        catalog_path: Path to catalog
        tb: Single table or list of tables
        indicator_names: Single value/list or list of values/lists
        dimensions: Single dict or list of dicts
        common_view_config: Single config dict or list of config dicts
        indicators_slug: Indicators slug (applies to all)
        indicator_as_dimension: Whether indicator is dimension (applies to all)
        choice_renames: Single dict or list of dicts
        catalog_path_full: Whether to use full catalog path (applies to all)
        explorer: Whether to enable explorer (applies to all)

    Returns:
        Collection object

    Raises:
        ValueError: If list parameters don't match the number of tables
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
        # Build common_view_config
        choice_renames_ = _get_choice_renames(
            choice_renames=choice_renames,
            num_tables=num_tables,
        )

        # Create collections for each table
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
            return collections[0]

        # Combine all collections
        c = combine_collections(
            collections=collections,
            catalog_path=catalog_path,
            config=config_yaml,
            is_explorer=explorer,
        )
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
            f"Parameter '{obj_name}' must be a list of lists or a single value, "
            f"but got {type(obj)} with value {obj}"
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
                            choice.name = renames[choice.slug]
                    elif inspect.isfunction(renames):
                        rename = renames(choice.slug)
                        if rename:
                            choice.name = renames(choice.slug)
                    else:
                        raise ValueError("Invalid choice_renames format.")
