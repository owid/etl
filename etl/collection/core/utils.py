from typing import Any, Dict, Set, Union, overload

from etl.collection.explorer import Explorer
from etl.collection.model.core import Collection
from etl.collection.utils import get_tables_by_name_mapping


def process_views(
    collection: Union[Collection, Explorer],
    dependencies: Set[str],
    combine_metadata_when_mult: bool = False,
):
    """Process views in Collection configuration."""
    # Get table information (table URI) by (i) table name and (ii) dataset_name/table_name
    tables_by_name = get_tables_by_name_mapping(dependencies)

    for view in collection.views:
        # Expand paths
        view.expand_paths(tables_by_name)

        # Combine metadata/config with definitions.common_views
        if (collection.definitions is not None) and (collection.definitions.common_views is not None):
            view.combine_with_common(collection.definitions.common_views)

        # Combine metadata in views which contain multiple indicators
        if combine_metadata_when_mult and view.metadata_is_needed:  # Check if view "contains multiple indicators"
            # TODO
            # view["metadata"] = build_view_metadata_multi(indicators, tables_by_uri)
            # log.info(
            #     f"View with multiple indicators detected. You should edit its `metadata` field to reflect that! This will be done programmatically in the future. Check view with dimensions {view.dimensions}"
            # )
            pass


@overload
def create_collection_from_config(
    config: Dict[str, Any],
    dependencies: Set[str],
    catalog_path: str,
    *,  # Force keyword-only arguments after this
    dependencies_combined: set[str] | None = None,
    validate_schema: bool = True,
    explorer: bool,
) -> Explorer: ...


@overload
def create_collection_from_config(
    config: Dict[str, Any],
    dependencies: Set[str],
    catalog_path: str,
    *,  # Force keyword-only arguments after this
    dependencies_combined: set[str] | None = None,
    validate_schema: bool = True,
    explorer: bool = False,
) -> Collection: ...


def create_collection_from_config(
    config: Dict[str, Any],
    dependencies: Set[str],
    catalog_path: str,
    *,  # Force keyword-only arguments after this
    dependencies_combined: set[str] | None = None,
    validate_schema: bool = True,
    explorer: bool = False,
) -> Union[Explorer, Collection]:
    """Create a Collection or Explorer instance from a configuration dictionary.

    config: Configuration of the collection.
    dependencies: Set of dependencies (dataset URIs) for the collection.
    catalog_path: Path to the step.
    dependencies_combined: Optional set of combined dependencies.
    validate_schema: Whether to validate the schema of the collection.
    explorer: Whether to create an Explorer instance instead of a Collection.
    """
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

    # Add dependencies to collection
    c.dependencies = dependencies_combined or dependencies

    return c
