"""TODO:

- Structure MDIM config and related objects in a more pythonic way (e.g. dataclass).
- Need to be able to quickly validate the configs against schemas.
- We should try to keep explorers in mind, and see this tooling as something we may want to use for them, too.
"""

import inspect
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union

from owid.catalog import Table
from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from etl.collections.common import (
    combine_config_dimensions,
    create_mdim_or_explorer,
    expand_config,
    map_indicator_path_to_id,
)
from etl.collections.model import Collection, pruned_json
from etl.collections.utils import camelize, has_duplicate_table_names
from etl.config import OWIDEnv

# Initialize logger.
log = get_logger()
# Dimensions: These are the expected possible dimensions
CHART_DIMENSIONS = ["y", "x", "size", "color"]


__all__ = [
    "expand_config",
    "combine_config_dimensions",
]


# mdim = Multidim.load_yaml("/home/lucas/repos/etl/etl/steps/export/multidim/covid/latest/covid.models.yml")
@pruned_json
@dataclass
class Multidim(Collection):
    """Model for MDIM configuration."""

    title: Dict[str, str]
    default_selection: List[str]
    topic_tags: Optional[List[str]] = None

    def __post_init__(self):
        """We set it here because of simplicity.

        Adding a class attribute like `_collection_type: Optional[str] = "explorer"` leads to error `TypeError: non-default argument 'config' follows default argument`.
        Alternative would be to define the class attribute like `_collection_type: Optional[str] = field(init=False, default="explorer")` but feels a bit redundant with parent definition.
        """
        self._collection_type = "multidim"

    def upsert_to_db(self, owid_env: OWIDEnv):
        # Replace especial fields URIs with IDs (e.g. sortColumnSlug).
        # TODO: I think we could move this to the Grapher side.
        config = replace_catalog_paths_with_ids(self.to_dict())

        # Convert config from snake_case to camelCase
        config = camelize(config, exclude_keys={"dimensions"})

        # Upsert config via Admin API
        admin_api = AdminAPI(owid_env)
        admin_api.put_mdim_config(self.catalog_path, config)


class MultidimSet:
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
            mdim = Multidim.load(str(path))
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


def create_mdim(
    config: dict,
    dependencies: Set[str],
    catalog_path: str,
) -> Multidim:
    mdim = create_mdim_or_explorer(
        Multidim,
        config,
        dependencies,
        catalog_path,
    )
    return mdim


def create_mdim_v2(
    config_yaml: Dict[str, Any],
    dependencies: Set[str],
    catalog_path: str,
    tb: Optional[Table] = None,
    indicator_names: Optional[Union[str, List[str]]] = None,
    dimensions: Optional[Union[List[str], Dict[str, Union[List[str], str]]]] = None,
    common_view_config: Optional[Dict[str, Any]] = None,
    indicators_slug: Optional[str] = None,
    indicator_as_dimension: bool = False,
    explorer_name: Optional[str] = None,
    choice_renames: Optional[Dict[str, Union[Dict[str, str], Callable]]] = None,
    catalog_path_full: bool = False,
) -> Multidim:
    config = deepcopy(config_yaml)

    # Read from table (programatically expand)
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
        )

        # Add views
        config["views"] += config_auto["views"]

        # Default explorer name is table name
        if explorer_name is None:
            explorer_name = tb.m.short_name
    elif explorer_name is None:
        explorer_name = "unknown"
        log.info(f"No table provided. Explorer name is not set. Using '{explorer_name}'.")

    # Create actual explorer
    mdim = create_mdim_or_explorer(
        Multidim,
        config,
        dependencies,
        catalog_path,
    )

    # Prune unused dimensions
    # mdim.prune_dimension_choices()

    # Rename choice names if given
    _rename_choices(mdim, choice_renames)

    return mdim


def _get_expand_path_mode(dependencies, catalog_path_full):
    # TODO: We should do this at indicator level. Default to 'table' for all indicators, except when there is a collision, then go to 'dataset', otherwise go to 'full'
    expand_path_mode = "table"
    if catalog_path_full:
        expand_path_mode = "full"
    elif has_duplicate_table_names(dependencies):
        expand_path_mode = "dataset"
    return expand_path_mode


def _rename_choices(mdim: Multidim, choice_renames: Optional[Dict[str, Union[Dict[str, str], Callable]]] = None):
    if choice_renames is not None:
        for dim in mdim.dimensions:
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


def build_view_metadata_multi(indicators: List[Dict[str, str]], tables_by_uri: Dict[str, Table]):
    """TODO: Combine the metadata from the indicators in the view.

    Ideas:
    -----
    `indicators` contains the URIs of all indicators used in the view. `tables_by_uri` contains the table objects (and therefore their metadata) easily accessible by their URI. With this information, we can get the metadata of each indicator as:

    ```
    for indicator in indicators:
        # Get indicator metadata
        table_uri, indicator_name = indicator["path"].split("#)
        metadata = tables_by_uri[table_uri][indicator_name].metadata

        # We also have access on how this indicator was in use (was it for dimension 'y', or 'x'?)
        dimension = indicator["dimension"] # This can be 'y', 'x', 'size', 'color', etc.
    ```

    Arguments:
    ----------

    indicators : List[Dict[str, str]]
        List of indicators in the view. Each element comes as a record {"path": "...", "dimension": "..."}. The path is the complete URI of the indicator.
    tables_by_uri : Dict[str, Table]
        Mapping of table URIs to table objects.
    """
    raise NotImplementedError("This function is not yet implemented.")


def get_tables_by_uri_mapping(tables_by_name: Dict[str, List[Table]]) -> Dict[str, Table]:
    """Mapping table URIs (complete) to table objects."""
    mapping = {}
    for table_name, tables in tables_by_name.items():
        for table in tables:
            uri = table.dataset.uri + "/" + table_name
            mapping[uri] = table
    return mapping


def replace_catalog_paths_with_ids(config):
    """Replace special metadata fields with their corresponding IDs in the database.

    In ETL, we allow certain fields in the config file to reference indicators by their catalog path. However, this is not yet supported in the Grapher API, so we need to replace these fields with the corresponding indicator IDs.

    NOTE: I think this is something that we should discuss changing on the Grapher side. So I see this function as a temporary workaround.

    Currently, affected fields are:

    - views[].config.sortColumnSlug

    These fields above are treated like fields in `dimensions`, and also accessed from:
    - `expand_catalog_paths`: To expand the indicator URI to be in its complete form.
    - `validate_multidim_config`: To validate that the indicators exist in the database.

    TODO: There might be other fields which might make references to indicators:
        - config.map.columnSlug
        - config.focusedSeriesNames
    """
    if "views" in config:
        views = config["views"]
        for view in views:
            if "config" in view:
                # Update sortColumnSlug
                if "sortColumnSlug" in view["config"]:
                    # Check if catalogPath
                    # Map to variable ID
                    view["config"]["sortColumnSlug"] = str(map_indicator_path_to_id(view["config"]["sortColumnSlug"]))
                # Update map.columnSlug
                if "map" in view["config"]:
                    if "columnSlug" in view["config"]["map"]:
                        view["config"]["map"]["columnSlug"] = str(
                            map_indicator_path_to_id(view["config"]["map"]["columnSlug"])
                        )

    return config


def group_views(views: list[dict[str, Any]], by: list[str]) -> list[dict[str, Any]]:
    """
    Group views by the specified dimensions. Concatenate indicators for the same group.

    :param views: List of views dictionaries.
    :param by: List of dimensions to group by.
    """
    views = deepcopy(views)

    grouped_views = {}
    for view in views:
        # Group key
        key = tuple(view["dimensions"][dim] for dim in by)

        if key not in grouped_views:
            # Ensure 'y' is a single indicator before turning it into a list
            assert not isinstance(view["indicators"]["y"], list), "Expected 'y' to be a single indicator, not a list"

            if set(view["indicators"].keys()) != {"y"}:
                raise NotImplementedError(
                    "Only 'y' indicator is supported in groupby. Adapt the code for other fields."
                )

            view["indicators"]["y"] = [view["indicators"]["y"]]

            # Add to dictionary
            grouped_views[key] = view
        else:
            grouped_views[key]["indicators"]["y"].append(view["indicators"]["y"])

    return list(grouped_views.values())
