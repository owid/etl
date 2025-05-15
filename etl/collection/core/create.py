"""Methods and tools to create collections of indicators."""

import inspect
from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional, Set, Union, cast

from owid.catalog import Table
from structlog import get_logger

from etl.collection.core.combine import combine_config_dimensions
from etl.collection.core.expand import expand_config
from etl.collection.core.utils import create_collection_from_config
from etl.collection.explorer import Explorer
from etl.collection.model.core import Collection
from etl.collection.utils import has_duplicate_table_names

# Initialize logger.
log = get_logger()


def create_collection(
    config_yaml: Dict[str, Any],
    dependencies: Set[str],
    catalog_path: str,
    tb: Optional[Table] = None,
    indicator_names: Optional[Union[str, List[str]]] = None,
    dimensions: Optional[Union[List[str], Dict[str, Union[List[str], str]]]] = None,
    common_view_config: Optional[Dict[str, Any]] = None,
    indicators_slug: Optional[str] = None,
    indicator_as_dimension: bool = False,
    choice_renames: Optional[Dict[str, Union[Dict[str, str], Callable]]] = None,
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
        return cast(Explorer, coll)
    else:
        coll = create_collection_from_config(
            config=config,
            dependencies=dependencies,
            catalog_path=catalog_path,
        )

    # Rename choice names if given
    _rename_choices(coll, choice_renames)

    return coll


def _get_expand_path_mode(dependencies, catalog_path_full):
    # TODO: We should do this at indicator level. Default to 'table' for all indicators, except when there is a collision, then go to 'dataset', otherwise go to 'full'
    expand_path_mode = "table"
    if catalog_path_full:
        expand_path_mode = "full"
    elif has_duplicate_table_names(dependencies):
        expand_path_mode = "dataset"
    return expand_path_mode


def _rename_choices(coll: Collection, choice_renames: Optional[Dict[str, Union[Dict[str, str], Callable]]] = None):
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
