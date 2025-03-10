"""TODO:

- Structure MDIM config and related objects in a more pythonic way (e.g. dataclass).
- Need to be able to quickly validate the configs against schemas.
- We should try to keep explorers in mind, and see this tooling as something we may want to use for them, too.
"""

from copy import deepcopy
from itertools import product
from typing import Any, Dict, List, Optional, Set

from deprecated import deprecated
from owid.catalog import Table
from structlog import get_logger

from apps.chart_sync.admin_api import AdminAPI
from etl.collections.common import expand_config, map_indicator_path_to_id, validate_collection_config
from etl.collections.model import Multidim
from etl.collections.utils import (
    get_tables_by_name_mapping,
    records_to_dictionary,
)
from etl.config import OWID_ENV, OWIDEnv
from etl.grapher.io import trim_long_variable_name
from etl.helpers import PathFinder
from etl.paths import SCHEMAS_DIR

# Initialize logger.
log = get_logger()
# Dimensions: These are the expected possible dimensions
CHART_DIMENSIONS = ["y", "x", "size", "color"]


__all__ = ["expand_config"]


def upsert_multidim_data_page(
    config: dict,
    paths: PathFinder,
    mdim_name: Optional[str] = None,
    tolerate_extra_indicators: bool = False,
    owid_env: Optional[OWIDEnv] = None,
) -> None:
    """Import MDIM config to DB.

    Args:
    -----

    slug: str
        Slug of the MDIM page. MDIM will be published at /slug
    config: dict
        MDIM configuration.
    paths: PathFinder
        Pass `paths = PathFinder(__file__)` from the script where this function is called.
    mdim_name: str
        Name of the MDIM page. Default is short_name from mdim catalog path.
    owid_env: Optional[OWIDEnv]
        Environment where to publish the MDIM page.
    """
    dependencies = paths.dependencies
    mdim_catalog_path = f"{paths.namespace}/{paths.version}/{paths.short_name}#{mdim_name or paths.short_name}"

    # Read config as structured object
    mdim = Multidim.from_dict(config)

    # Edit views
    process_views(mdim, dependencies=dependencies)

    # TODO: Possibly add other edits (to dimensions?)

    # Upsert to DB
    _upsert_multidim_data_page(mdim_catalog_path, mdim, tolerate_extra_indicators, owid_env)


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


def _upsert_multidim_data_page(
    mdim_catalog_path: str, mdim: Multidim, tolerate_extra_indicators: bool, owid_env: Optional[OWIDEnv] = None
) -> None:
    """Actual upsert to DB."""
    # Ensure we have an environment set
    if owid_env is None:
        owid_env = OWID_ENV

    # Validate config
    mdim.validate_schema(SCHEMAS_DIR / "multidim-schema.json")
    validate_collection_config(mdim, owid_env.engine, tolerate_extra_indicators)

    # Replace especial fields URIs with IDs (e.g. sortColumnSlug).
    # TODO: I think we could move this to the Grapher side.
    config = replace_catalog_paths_with_ids(mdim.to_dict())

    # Upsert config via Admin API
    admin_api = AdminAPI(owid_env)
    admin_api.put_mdim_config(mdim_catalog_path, config)


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
    pass


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


def combine_config_dimensions(
    config_dimensions: List[Dict[str, Any]],
    config_dimensions_yaml: List[Dict[str, Any]],
    choices_top: bool = False,
    dimensions_top: bool = False,
):
    """Combine the dimension configuration from the YAML file with the one generated programmatically.

    There are various strategies that we could follow here, but currently:

    - We consider the union of config_dimensions (returned by expander.build_dimensions) nad config_dimensions_yaml.
    - These are kept as-is, unless they are in the YML config, in which case they are overwritten.

    Other possible strategies:

    - We could do the reverse, and only consider the fields from config_dimensions_yaml. I'm personally unsure when this could be valuable.


    Arguments
    ---------
    config_dimensions: List[Dict[str, Any]]
        Generated by expander.build_dimensions.
    config_dimensions_yaml:  List[Dict[str, Any]]
        From the YAML file.
    choices_top: bool
        Set to True to place the choices from `config_dimensions` first.
    dimensions_top: bool
        Set to True to place the dimensions from `config_dimensions` first.

    TODO:

        - I think we need to add more checks to ensure that there is nothing weird being produced here.
    """

    config_dimensions_combined = deepcopy(config_dimensions)
    dims_overwrite = records_to_dictionary(config_dimensions_yaml, "slug")

    # Overwrite dimensions
    for dim in config_dimensions_combined:
        slug_dim = dim["slug"]
        if slug_dim in dims_overwrite:
            # Get dimension data to overwrite, remove it from dictionary
            dim_overwrite = dims_overwrite.pop(slug_dim)

            # Overwrite dimension name
            dim["name"] = dim_overwrite.get("name", dim["name"])

            # Overwrite presentation
            if "presentation" in dim_overwrite:
                dim["presentation"] = dim_overwrite["presentation"]

            # Overwrite choices
            if "choices" in dim_overwrite:
                choices_overwrite = records_to_dictionary(
                    dim_overwrite["choices"],
                    "slug",
                )
                assert (
                    "choices" in dim
                ), f"Choices not found in dimension: {dim}! This is rare, please report this issue!"
                for choice in dim["choices"]:
                    slug_choice = choice["slug"]
                    if slug_choice in choices_overwrite:
                        # Get dimension data to overwrite, remove it from dictionary
                        choice_overwrite = choices_overwrite.pop(slug_choice)

                        # Overwrite choice name
                        choice["name"] = choice_overwrite.get("name", dim["name"])
                        # Overwrite choice description
                        choice["description"] = choice_overwrite.get("description", choice["description"])

                # Handle choices from YAML not present in config_dimensions
                if choices_overwrite:
                    missing_choices = []
                    for slug, values in choices_overwrite.items():
                        choice = {"slug": slug, **values}
                        missing_choices.append(choice)

                    if choices_top:
                        dim["choices"] += missing_choices
                    else:
                        dim["choices"] = missing_choices + dim["choices"]

                # Sort choices based on how these appear in the YAML file (only if dimensions_top is False)
                if not choices_top:
                    dim["choices"] = _order(dim_overwrite["choices"], dim["choices"])

    # Handle dimensions from YAML not present in config_dimensions
    if dims_overwrite:
        missing_dims = []
        for slug, values in dims_overwrite.items():
            dim = {"slug": slug, **values}
            missing_dims.append(dim)

        if dimensions_top:
            config_dimensions_combined += missing_dims
        else:
            config_dimensions_combined = missing_dims + config_dimensions_combined

    # Sort dimensions based on how these appear in the YAML file (only if dimensions_top is False)
    if not dimensions_top:
        config_dimensions_combined = _order(config_dimensions_yaml, config_dimensions_combined)

    return config_dimensions_combined


def _order(config_yaml, config_combined):
    # Build score
    score = {record["slug"]: i for i, record in enumerate(config_yaml)}
    # Split: those that need ordering, those that don't
    config_sort = [record for record in config_combined if record["slug"] in score]
    config_others = [record for record in config_combined if record["slug"] not in score]

    # Order if applicable
    config_sort = sorted(
        config_sort,
        key=lambda x: score.get(x["slug"], 100),
    )

    return config_sort + config_others


####################################################################################################
# DEPRECATED FUNCTIONS
####################################################################################################
@deprecated("This function relies on specific column naming convention. Use `expand_config` instead.")
def generate_views_for_dimensions(
    dimensions, tables, dimensions_order_in_slug=None, additional_config=None, warn_on_missing_combinations=True
):
    """Generate individual views for all possible combinations of dimensions in a list of flattened tables.

    Parameters
    ----------
    dimensions : List[Dict[str, Any]]
        Dimensions, as given in the configuration of the multidim step, e.g.
        [
            {'slug': 'frequency', 'name': 'Frequency', 'choices': [{'slug': 'annual','name': 'Annual'}, {'slug': 'monthly', 'name': 'Monthly'}]},
            {'slug': 'source', 'name': 'Energy source', 'choices': [{'slug': 'electricity', 'name': 'Electricity'}, {'slug': 'gas', 'name': 'Gas'}]},
            ...
        ]
    tables : List[Table]
        Tables whose indicator views will be generated.
    dimensions_order_in_slug : Tuple[str], optional
        Dimension names, as they appear in "dimensions", and in the order in which they are spelled out in indicator names. For example, if indicator names are, e.g. annual_electricity_euros, then dimensions_order_in_slug would be ("frequency", "source", "unit").
    additional_config : _type_, optional
        Additional config fields to add to each view, e.g.
        {"chartTypes": ["LineChart"], "hasMapTab": True, "tab": "map"}
    warn_on_missing_combinations : bool, optional
        True to warn if any combination of dimensions is not found among the indicators in the given tables.

    Returns
    -------
    results : List[Dict[str, Any]]
        Views configuration, e.g.
        [
            {'dimensions': {'frequency': 'annual', 'source': 'electricity', 'unit': 'euro'}, 'indicators': {'y': 'grapher/energy/2024-11-20/energy_prices/energy_prices_annual#annual_electricity_household_total_price_including_taxes_euro'},
            {'dimensions': {'frequency': 'annual', 'source': 'electricity', 'unit': 'pps'}, 'indicators': {'y': 'grapher/energy/2024-11-20/energy_prices/energy_prices_annual#annual_electricity_household_total_price_including_taxes_pps'},
            ...
        ]

    """
    # Extract all choices for each dimension as (slug, choice_slug) pairs.
    choices = {dim["slug"]: [choice["slug"] for choice in dim["choices"]] for dim in dimensions}
    dimension_slugs_in_config = set(choices.keys())

    # Sanity check for dimensions_order_in_slug.
    if dimensions_order_in_slug:
        dimension_slugs_in_order = set(dimensions_order_in_slug)

        # Check if any slug in the order is missing from the config.
        missing_slugs = dimension_slugs_in_order - dimension_slugs_in_config
        if missing_slugs:
            raise ValueError(
                f"The following dimensions are in 'dimensions_order_in_slug' but not in the config: {missing_slugs}"
            )

        # Check if any slug in the config is missing from the order.
        extra_slugs = dimension_slugs_in_config - dimension_slugs_in_order
        if extra_slugs:
            log.warning(
                f"The following dimensions are in the config but not in 'dimensions_order_in_slug': {extra_slugs}"
            )

        # Reorder choices to match the specified order.
        choices = {dim: choices[dim] for dim in dimensions_order_in_slug if dim in choices}

    # Generate all combinations of the choices.
    all_combinations = list(product(*choices.values()))

    # Create the views.
    results = []
    for combination in all_combinations:
        # Map dimension slugs to the chosen values.
        dimension_mapping = {dim_slug: choice for dim_slug, choice in zip(choices.keys(), combination)}
        slug_combination = "_".join(combination)

        # Find relevant tables for the current combination.
        relevant_table = []
        for table in tables:
            if slug_combination in table:
                relevant_table.append(table)

        # Handle missing or multiple table matches.
        if len(relevant_table) == 0:
            if warn_on_missing_combinations:
                log.warning(f"Combination {slug_combination} not found in tables")
            continue
        elif len(relevant_table) > 1:
            log.warning(f"Combination {slug_combination} found in multiple tables: {relevant_table}")

        # Construct the indicator path.
        indicator_path = f"{relevant_table[0].metadata.dataset.uri}/{relevant_table[0].metadata.short_name}#{trim_long_variable_name(slug_combination)}"
        indicators = {
            "y": indicator_path,
        }
        # Append the combination to results.
        results.append({"dimensions": dimension_mapping, "indicators": indicators})

    if additional_config:
        # Include additional fields in all results.
        for result in results:
            result.update({"config": additional_config})

    return results


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
