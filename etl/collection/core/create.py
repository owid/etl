"""Methods and tools to create collections of indicators."""

import inspect
from copy import deepcopy
from typing import Any, Callable, TypeVar, Union, cast

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
T = TypeVar("T")
OptionalListable = Union[T, list[T], None]


def create_collection_v2(
    config_yaml: dict[str, Any],
    dependencies: set[str],
    catalog_path: str,
    tb: list[Table] | Table | None = None,
    indicator_names: OptionalListable[str | list[str]] = None,
    dimensions: OptionalListable[dict[str, list[str] | str]] = None,
    common_view_config: OptionalListable[dict[str, Any]] = None,
    indicators_slug: str | None = None,
    indicator_as_dimension: bool = False,
    choice_renames: OptionalListable[dict[str, dict[str, str] | Callable]] = None,
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

        # Distribute indicator_names parameter
        if indicator_names is None:
            distributed_indicator_names = [None] * num_tables
        elif (
            isinstance(indicator_names, list)
            and len(indicator_names) > 0
            and isinstance(indicator_names[0], (str, list))
        ):
            # This is a list of indicator_names (one per table)
            if len(indicator_names) != num_tables:
                raise ValueError(
                    f"Parameter 'indicator_names' is a list of length {len(indicator_names)}, "
                    f"but expected length {num_tables} to match number of tables"
                )
            distributed_indicator_names = indicator_names
        else:
            # Single value - replicate for all tables
            distributed_indicator_names = [indicator_names] * num_tables

        # Distribute dimensions parameter
        if dimensions is None:
            distributed_dimensions = [None] * num_tables
        elif isinstance(dimensions, list) and len(dimensions) > 0 and isinstance(dimensions[0], dict):
            # This is a list of dimensions (one per table)
            if len(dimensions) != num_tables:
                raise ValueError(
                    f"Parameter 'dimensions' is a list of length {len(dimensions)}, "
                    f"but expected length {num_tables} to match number of tables"
                )
            distributed_dimensions = dimensions
        else:
            # Single value - replicate for all tables
            distributed_dimensions = [dimensions] * num_tables

        # Distribute common_view_config parameter
        if common_view_config is None:
            distributed_common_view_config = [None] * num_tables
        elif (
            isinstance(common_view_config, list)
            and len(common_view_config) > 0
            and isinstance(common_view_config[0], dict)
        ):
            # This is a list of common_view_config (one per table)
            if len(common_view_config) != num_tables:
                raise ValueError(
                    f"Parameter 'common_view_config' is a list of length {len(common_view_config)}, "
                    f"but expected length {num_tables} to match number of tables"
                )
            distributed_common_view_config = common_view_config
        else:
            # Single value - replicate for all tables
            distributed_common_view_config = [common_view_config] * num_tables

        # Distribute choice_renames parameter
        if choice_renames is None:
            distributed_choice_renames = [None] * num_tables
        elif isinstance(choice_renames, list) and len(choice_renames) > 0 and isinstance(choice_renames[0], dict):
            # This is a list of choice_renames (one per table)
            if len(choice_renames) != num_tables:
                raise ValueError(
                    f"Parameter 'choice_renames' is a list of length {len(choice_renames)}, "
                    f"but expected length {num_tables} to match number of tables"
                )
            distributed_choice_renames = choice_renames
        else:
            # Single value - replicate for all tables
            distributed_choice_renames = [choice_renames] * num_tables

        # Create collections for each table
        collections = []
        for i in range(num_tables):
            c = create_collection(
                config_yaml=config_yaml,
                dependencies=dependencies,
                catalog_path=catalog_path,
                tb=tb[i],
                indicator_names=cast("str | list[str] | None", distributed_indicator_names[i]),
                dimensions=cast("list[str] | dict[str, list[str] | str] | None", distributed_dimensions[i]),
                common_view_config=cast("dict[str, Any] | None", distributed_common_view_config[i]),
                indicators_slug=indicators_slug,
                indicator_as_dimension=indicator_as_dimension,
                choice_renames=cast("dict[str, dict[str, str] | Callable] | None", distributed_choice_renames[i]),
                catalog_path_full=catalog_path_full,
                explorer=explorer,
            )
            collections.append(c)

        # Combine all collections
        c = combine_collections(
            collections=collections,
            catalog_path=catalog_path,
            config=config_yaml,
        )
    else:
        # Single table case - call original function directly
        c = create_collection(
            config_yaml=config_yaml,
            dependencies=dependencies,
            catalog_path=catalog_path,
            tb=tb,
            indicator_names=cast("str | list[str] | None", indicator_names),
            dimensions=cast("list[str] | dict[str, list[str] | str] | None", dimensions),
            common_view_config=cast("dict[str, Any] | None", common_view_config),
            indicators_slug=indicators_slug,
            indicator_as_dimension=indicator_as_dimension,
            choice_renames=cast("dict[str, dict[str, str] | Callable] | None", choice_renames),
            catalog_path_full=catalog_path_full,
            explorer=explorer,
        )

    return c


def create_collection(
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
