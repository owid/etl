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
from etl.collections.common import (
    combine_config_dimensions,
    create_mdim_or_explorer,
    expand_config,
    map_indicator_path_to_id,
)
from etl.collections.model import Collection, pruned_json
from etl.collections.utils import camelize
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

        # Export config to local directory in addition to uploading it to MySQL for debugging.
        log.info(f"Exporting config to {self.local_config_path}")
        Path(self.local_config_path).parent.mkdir(parents=True, exist_ok=True)
        with open(self.local_config_path, "w") as f:
            yaml_dump(config, f)

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
