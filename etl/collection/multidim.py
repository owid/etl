"""TODO:

- Structure MDIM config and related objects in a more pythonic way (e.g. dataclass).
- Need to be able to quickly validate the configs against schemas.
- We should try to keep explorers in mind, and see this tooling as something we may want to use for them, too.
"""

import inspect
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union, cast

from owid.catalog import Table
from structlog import get_logger

from etl.collection.common import (
    combine_config_dimensions,
    expand_config,
)
from etl.collection.explorer import Explorer
from etl.collection.model import Collection
from etl.collection.utils import (
    get_tables_by_name_mapping,
    has_duplicate_table_names,
)

# Initialize logger.
log = get_logger()


__all__ = [
    "expand_config",
    "combine_config_dimensions",
]


class CollectionSet:
    def __init__(self, path: Path):
        self.path = path
        self.mdims = self._build_dictionary()

    def _build_dictionary(self) -> Dict[str, Path]:
        dix = {}
        paths = self.path.glob(r"*.config.json")
        for p in paths:
            name = p.name.replace(".config.json", "")
            dix[name] = p
        return dix

    def read(self, mdim_name: str):
        # Check mdim exists
        if mdim_name not in self.mdims:
            raise ValueError(
                f"MDIM name not available. Available options are {self.names}. If this does not make sense to you, try running the necessary steps to re-export files to {self.path}"
            )

        # Read MDIM
        path = self.mdims[mdim_name]
        try:
            mdim = Collection.load(str(path))
        except TypeError as e:
            # This is a workaround for the TypeError that occurs when loading the config file.
            raise TypeError(
                f"Error loading MDIM config file. Please check the file format and ensure it is valid JSON. Suggestion: Re-run export step generating {mdim_name}. Error: {e}"
            )

        # Get and set catalog path
        return mdim

    @property
    def names(self):
        return list(sorted(self.mdims.keys()))


def create_explorer(
    config: dict,
    dependencies: Set[str],
    catalog_path: str,
) -> Explorer:
    coll = create_collection_from_config(
        config=config,
        dependencies=dependencies,
        catalog_path=catalog_path,
        validate_schema=False,
        explorer=True,
    )
    return cast(Explorer, coll)


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


def process_views(
    mdim_or_explorer,
    dependencies: Set[str],
    combine_metadata_when_mult: bool = False,
):
    """Process views in Explorer configuration.

    TODO: See if we can converge to one solution with etl.collection.multidim.process_views.
    """
    # Get table information (table URI) by (i) table name and (ii) dataset_name/table_name
    tables_by_name = get_tables_by_name_mapping(dependencies)

    for view in mdim_or_explorer.views:
        # Expand paths
        view.expand_paths(tables_by_name)

        # Combine metadata/config with definitions.common_views
        if (mdim_or_explorer.definitions is not None) and (mdim_or_explorer.definitions.common_views is not None):
            view.combine_with_common(mdim_or_explorer.definitions.common_views)

        # Combine metadata in views which contain multiple indicators
        if combine_metadata_when_mult and view.metadata_is_needed:  # Check if view "contains multiple indicators"
            # TODO
            # view["metadata"] = build_view_metadata_multi(indicators, tables_by_uri)
            # log.info(
            #     f"View with multiple indicators detected. You should edit its `metadata` field to reflect that! This will be done programmatically in the future. Check view with dimensions {view.dimensions}"
            # )
            pass


def create_collection_from_config(
    config: dict,
    dependencies: Set[str],
    catalog_path: str,
    validate_schema: bool = True,
    explorer: bool = False,
) -> Explorer | Collection:
    # Read config as structured object
    if explorer:
        c = Explorer.from_dict(dict(**config, catalog_path=catalog_path))
    else:
        c = Collection.from_dict(dict(**config, catalog_path=catalog_path))

    # Edit views
    process_views(c, dependencies=dependencies)

    # Validate config
    if validate_schema:
        c.validate_schema()

    # Ensure that all views are in choices
    c.validate_views_with_dimensions()

    # Validate duplicate views
    c.check_duplicate_views()

    return c


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


def get_tables_by_uri_mapping(tables_by_name: Dict[str, List[Table]]) -> Dict[str, Table]:
    """Mapping table URIs (complete) to table objects."""
    mapping = {}
    for table_name, tables in tables_by_name.items():
        for table in tables:
            uri = table.dataset.uri + "/" + table_name
            mapping[uri] = table
    return mapping
