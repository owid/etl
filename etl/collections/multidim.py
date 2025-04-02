"""TODO:

- Structure MDIM config and related objects in a more pythonic way (e.g. dataclass).
- Need to be able to quickly validate the configs against schemas.
- We should try to keep explorers in mind, and see this tooling as something we may want to use for them, too.
"""

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from owid.catalog import Table
from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from etl.collections.common import combine_config_dimensions, expand_config, map_indicator_path_to_id
from etl.collections.model import Collection, Definitions, MDIMView, pruned_json
from etl.collections.utils import (
    camelize,
    get_tables_by_name_mapping,
    validate_indicators_in_db,
)
from etl.config import OWID_ENV, OWIDEnv
from etl.files import yaml_dump
from etl.paths import EXPORT_DIR, SCHEMAS_DIR

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

    views: List[MDIMView]
    title: Dict[str, str]
    default_selection: List[str]
    topic_tags: Optional[List[str]] = None
    definitions: Optional[Definitions] = None

    # Internal use. For save() method.
    _catalog_path: Optional[str] = None

    @property
    def catalog_path(self) -> Optional[str]:
        return self._catalog_path

    @catalog_path.setter
    def catalog_path(self, value: str) -> None:
        assert "#" in value, "Catalog path should be in the format `path#name`."
        self._catalog_path = value

    @property
    def local_config_path(self) -> Path:
        # energy/latest/energy_prices#energy_prices -> export/multidim/energy/latest/energy_prices/config.yml
        assert self.catalog_path
        return EXPORT_DIR / "multidim" / self.catalog_path.split("#")[0] / "config.yml"

    def save(self, owid_env: Optional[OWIDEnv] = None, tolerate_extra_indicators: bool = False):
        # Ensure we have an environment set
        if owid_env is None:
            owid_env = OWID_ENV

        if self.catalog_path is None:
            raise ValueError("Catalog path is not set. Please set it before saving.")

        # Check that all indicators in mdim exist
        indicators = self.indicators_in_use(tolerate_extra_indicators)
        validate_indicators_in_db(indicators, owid_env.engine)

        # Replace especial fields URIs with IDs (e.g. sortColumnSlug).
        # TODO: I think we could move this to the Grapher side.
        config = replace_catalog_paths_with_ids(self.to_dict())

        # Export config to local directory in addition to uploading it to MySQL for debugging.
        log.info(f"Exporting config to {self.local_config_path}")
        with open(self.local_config_path, "w") as f:
            yaml_dump(config, f)

        # Convert config from snake_case to camelCase
        config = camelize(config, exclude_keys={"dimensions"})

        # Upsert config via Admin API
        admin_api = AdminAPI(owid_env)
        admin_api.put_mdim_config(self.catalog_path, config)


def create_mdim(
    config: dict,
    dependencies: Set[str],
) -> Multidim:
    # Read config as structured object
    mdim = Multidim.from_dict(config)

    # Edit views
    process_views(mdim, dependencies=dependencies)

    # Validate config
    mdim.validate_schema(SCHEMAS_DIR / "multidim-schema.json")

    # Ensure that all views are in choices
    mdim.validate_views_with_dimensions()

    # Validate duplicate views
    mdim.check_duplicate_views()

    return mdim


def process_views(mdim: Multidim, dependencies: Set[str]):
    """Process views in MDIM configuration.

    This includes:
        - Make sure that catalog paths for indicators are complete.
        - TODO: Process views with multiple indicators to have adequate metadata
    """
    # Get table information by table name, and table URI
    tables_by_name = get_tables_by_name_mapping(dependencies)
    # tables_by_uri = get_tables_by_uri_mapping(tables_by_name)  # This is to be used when processing views with multiple indicators

    # Go through all views and expand catalog paths
    for view in mdim.views:
        # Update indicators for each dimension, making sure they have the complete URI
        view.expand_paths(tables_by_name)

        # Combine metadata/config with definitions.common_views
        if (mdim.definitions is not None) and (mdim.definitions.common_views is not None):
            view.combine_with_common(mdim.definitions.common_views)

        # Combine metadata in views which contain multiple indicators
        if view.metadata_is_needed:  # Check if view "contains multiple indicators"
            # TODO
            # view["metadata"] = build_view_metadata_multi(indicators, tables_by_uri)
            log.info(
                f"View with multiple indicators detected. You should edit its `metadata` field to reflect that! This will be done programmatically in the future. Check view with dimensions {view.dimensions}"
            )


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
