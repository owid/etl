"""
Graph step logic for creating and updating charts in the grapher database.

This module handles the creation and updating of charts (graphs) as part of the ETL pipeline.
Charts can either inherit metadata from a single indicator or have explicit metadata defined
in a .meta.yml file.

Key features:
- Single charts: Use upsert_graph() to create/update individual charts
- Multidimensional collections: Delegate to etl.collection.core.create.create_collection()
- Short indicator names: Auto-expand using DAG dependencies (no hardcoded versions)
- Dynamic metadata: Support yaml_params for runtime value substitution

TODO: Consider refactoring _expand_indicator_path() to share more code with
      CollectionConfigExpander._expand_indicator_path() from etl/collection/core/expand.py.
      Current duplication exists because collections work with Table objects while
      graphs work with URI strings from DAG dependencies.

TODO: Consider loading field categorizations from the grapher schema (GrapherInterface)
      instead of hardcoding them to avoid maintenance burden and version drift.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog
import yaml
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

from apps.chart_sync.admin_api import AdminAPI
from etl import paths
from etl.config import GRAPHER_USER_ID, OWID_ENV
from etl.db import get_engine
from etl.files import ruamel_dump
from etl.grapher.model import Chart, Variable

log = structlog.get_logger()


# Field categorizations for chart config
# ----------------------------------------
# These categorize GrapherInterface fields for different purposes:
# - pull_graph(): Which fields to exclude when pulling from DB to YAML
# - _check_manual_overrides(): Which fields don't count as "manual edits"
#
# Maintaining these manually is brittle but necessary since we need to make decisions
# about field semantics (e.g., is this field auto-managed? ETL-managed? User content?).
#
# TODO: Consider loading these from grapher schema metadata or annotations
#       instead of hardcoding to reduce maintenance burden.
# See: owid-grapher/packages/@ourworldindata/types/dist/grapherTypes/GrapherTypes.d.ts

# Fields that are auto-managed by the database
DB_MANAGED_FIELDS = {
    "id",
    "version",
    "createdAt",
    "updatedAt",
    "publishedAt",
    "lastEditedAt",
    "lastEditedByUserId",
    "publishedByUserId",
}

# Fields managed by ETL (not manual overrides)
ETL_MANAGED_FIELDS = {
    "dimensions",  # Variable IDs managed from catalogPath
    "isInheritanceEnabled",  # Controlled by ETL config
}

# Schema and metadata fields
METADATA_FIELDS = {
    "$schema",
    "slug",
    "isPublished",
    "isIndexable",
    "relatedQuestions",
}

# User-editable content fields to check for conflicts
# These are the fields that users typically edit in the admin UI
# and that we want to detect when checking for manual overrides
USER_CONTENT_FIELDS = {
    "title",
    "subtitle",
    "note",
    "chartTypes",
    "hasMapTab",
    "tab",
    "selectedEntityNames",
}


def calculate_source_checksum(
    dependencies: List[str],
    metadata_file: Optional[Path],
) -> str:
    """
    Calculate checksum for graph step inputs (dependencies + metadata file).

    This replicates the logic from GraphStep.checksum_input() without requiring
    a full Step object.

    Args:
        dependencies: List of dataset URIs from DAG (e.g., ["data://grapher/..."])
        metadata_file: Optional path to .meta.yml file

    Returns:
        MD5 checksum hex string
    """
    import hashlib

    from owid.catalog import Dataset

    from etl import files

    checksums = {}

    # Include dependency checksums
    for dep_uri in dependencies:
        # Convert URI to path (e.g., "data://grapher/..." -> "data/grapher/...")
        dep_path = dep_uri.split("://", 1)[1] if "://" in dep_uri else dep_uri
        # Load dataset and get its source_checksum
        ds = Dataset(f"{paths.DATA_DIR}/{dep_path}")
        checksums[dep_uri] = ds.metadata.source_checksum

    # Include metadata file checksum if it exists
    if metadata_file and metadata_file.exists():
        checksums["metadata_file"] = files.checksum_file(str(metadata_file))

    # Sort and hash
    in_order = [v for _, v in sorted(checksums.items())]
    return hashlib.md5(",".join(in_order).encode("utf8")).hexdigest()


def upsert_graph(
    slug: str,
    metadata_file: Optional[Path],
    dependencies: List[str],
    source_checksum: str,
    graph_push: bool = False,
    yaml_params: Optional[dict] = None,
) -> int:
    """
    Create or update a chart in the grapher database.

    Args:
        slug: Chart slug (e.g., "fur-farming-ban")
        metadata_file: Optional path to .meta.yml file with chart metadata
        dependencies: List of dataset URIs from DAG (e.g., ["data://grapher/..."])
        source_checksum: Checksum of inputs (metadata file + dependencies) for dirty detection
        graph_push: If True, overwrite manual edits made in Admin UI
        yaml_params: Optional dict for string substitution in YAML file (e.g., {threshold: 4.73})

    Returns:
        Chart ID

    Raises:
        ValueError: If no metadata file or indicators can't be resolved
    """
    log.info("graph.upsert", slug=slug, metadata_file=metadata_file, dependencies=dependencies)

    engine = get_engine()

    with Session(engine) as session:
        # 1. Load metadata file if it exists
        if metadata_file and metadata_file.exists():
            metadata = _load_metadata_file(metadata_file, yaml_params=yaml_params)
        else:
            metadata = {}

        # 2. Get indicator catalog paths from metadata dimensions (not DAG)
        # DAG dependencies are now dataset-level, indicators are in metadata
        indicator_paths = []
        if "dimensions" in metadata:
            # Extract catalogPath from dimensions
            for dim in metadata["dimensions"]:
                if "catalogPath" in dim:
                    catalog_path = dim["catalogPath"]
                    # Expand short indicator names to full paths
                    catalog_path = _expand_indicator_path(catalog_path, dependencies)
                    indicator_paths.append(catalog_path)

        if not indicator_paths:
            # Fallback: if no dimensions in metadata, try old format from DAG
            # This maintains backward compatibility
            for dep_uri in dependencies:
                if "#" in dep_uri:
                    catalog_path = _uri_to_catalog_path(dep_uri)
                    indicator_paths.append(catalog_path)

        log.info("graph.indicators", paths=indicator_paths, source="metadata" if "dimensions" in metadata else "dag")

        # 3. Resolve indicator catalog paths to variable IDs
        variable_ids = _resolve_indicator_paths(session, indicator_paths)
        log.info("graph.dependencies_resolved", variable_ids=variable_ids)

        # 4. Check if chart already exists
        try:
            chart = Chart.load_chart(session, slug=slug)
            chart_exists = True
            log.info("graph.chart_exists", chart_id=chart.id)
        except NoResultFound:
            chart = None
            chart_exists = False
            log.info("graph.chart_new")

        # 5. Build chart config based on indicator count and metadata
        if len(variable_ids) == 1:
            # Single indicator - can use inheritance if no explicit config provided
            # If metadata file exists and has any keys, consider it explicit config
            has_explicit_config = bool(metadata)

            if has_explicit_config:
                # User provided explicit metadata - use it
                log.info("graph.explicit_metadata", file=str(metadata_file))
                config = {k: v for k, v in metadata.items() if k != "indicators"}
                config["slug"] = slug
                config.setdefault("$schema", "https://files.ourworldindata.org/schemas/grapher-schema.009.json")
                # Preserve isPublished from existing chart if not explicitly set in metadata
                if chart_exists and "isPublished" not in config:
                    config["isPublished"] = chart.config.get("isPublished", False)
                is_inheritance_enabled = False
            else:
                # Pure inheritance from indicator
                log.info("graph.auto_inherit")
                config = {
                    "slug": slug,
                    "$schema": "https://files.ourworldindata.org/schemas/grapher-schema.009.json",
                }
                is_inheritance_enabled = True
        else:
            # Multi-indicator - must have explicit config
            has_explicit_config = any(key in metadata for key in ["title", "subtitle", "note", "chartTypes"])
            if not has_explicit_config:
                raise ValueError(
                    f"Multi-indicator graph '{slug}' requires explicit chart config "
                    f"(title, subtitle, etc.) in {metadata_file}"
                )
            log.info("graph.multi_indicator", file=str(metadata_file))
            config = {k: v for k, v in metadata.items() if k != "indicators"}
            config["slug"] = slug
            config.setdefault("$schema", "https://files.ourworldindata.org/schemas/grapher-schema.009.json")
            # Preserve isPublished from existing chart if not explicitly set in metadata
            if chart_exists and "isPublished" not in config:
                config["isPublished"] = chart.config.get("isPublished", False)
            is_inheritance_enabled = False

        # 6. Check for manual overrides if chart exists and not forcing
        if chart and not graph_push:
            _check_manual_overrides(session, chart, config, is_inheritance_enabled, source_checksum)

        # 7. Add dimensions (variable IDs) to config
        config["dimensions"] = [{"variableId": vid, "property": "y", "display": {}} for vid in variable_ids]

        # 8. Create or update chart via Admin API
        admin_api = AdminAPI(OWID_ENV)
        user_id = int(GRAPHER_USER_ID) if GRAPHER_USER_ID else None

        if chart_exists:
            log.info("graph.update_chart", chart_id=chart.id, inheritance=is_inheritance_enabled)
            config["isInheritanceEnabled"] = is_inheritance_enabled
            result = admin_api.update_chart(
                chart_id=chart.id,
                chart_config=config,
                user_id=user_id,
            )
            chart_id = chart.id
        else:
            log.info("graph.create_chart", inheritance=is_inheritance_enabled)
            config["isInheritanceEnabled"] = is_inheritance_enabled
            # New charts default to unpublished (draft)
            config.setdefault("isPublished", False)
            result = admin_api.create_chart(
                chart_config=config,
                user_id=user_id,
            )
            chart_id = result["chartId"]

        log.info(
            "graph.upsert_success",
            chart_id=chart_id,
            slug=slug,
            admin_url=f"{OWID_ENV.admin_site}/admin/charts/{chart_id}/edit",
        )

        # Save checksum and config to local file for dirty detection and conflict detection
        _save_graph_metadata(slug, source_checksum, config)

        return chart_id


def pull_graph(slug: str, metadata_file: Path, dependencies: List[str]) -> None:
    """
    Pull chart configuration from the database and write to .meta.yml file.

    This fetches the current chart configuration from the database and converts it back
    to the .meta.yml format used by graph steps. Useful for syncing changes made via
    Admin UI back to ETL.

    Note: Currently only supports simple charts (with dimensions), not multidimensional
    charts (with views).

    Args:
        slug: Chart slug (must exist in database)
        metadata_file: Path where .meta.yml will be written
        dependencies: List of dataset URIs from DAG (for converting variable IDs to short names)

    Raises:
        ValueError: If chart not found in database or if step doesn't exist in DAG
    """

    log.info("graph.pull", slug=slug, metadata_file=metadata_file)

    engine = get_engine()

    with Session(engine) as session:
        # 1. Load chart from database
        try:
            chart = Chart.load_chart(session, slug=slug)
            log.info("graph.chart_found", chart_id=chart.id)
        except NoResultFound:
            raise ValueError(
                f"Chart '{slug}' not found in database. " f"Use Admin UI or create a new graph step to create it first."
            )

        # 2. Get chart config from database
        config = dict(chart.config)

        # 3. Convert variable IDs back to short indicator names
        short_paths = []
        if "dimensions" in config:
            variable_ids = [dim["variableId"] for dim in config["dimensions"] if "variableId" in dim]

            if variable_ids:
                # Load variable catalog paths from database
                from sqlalchemy import text

                placeholders = ",".join([":id" + str(i) for i in range(len(variable_ids))])
                query = text(f"""
                    SELECT id, catalogPath
                    FROM variables
                    WHERE id IN ({placeholders})
                """)
                params = {f"id{i}": vid for i, vid in enumerate(variable_ids)}
                result = session.execute(query, params)
                rows = result.fetchall()

                # Convert to dict
                var_id_to_path = {row[0]: row[1] for row in rows}

                # Convert full catalog paths to short names
                for var_id in variable_ids:
                    full_path = var_id_to_path.get(var_id)
                    if not full_path:
                        log.warning("graph.pull.variable_not_found", variable_id=var_id)
                        short_paths.append(f"<unknown_variable_{var_id}>")
                        continue

                    # Try to shorten the path using dependencies
                    short_path = _shorten_indicator_path(full_path, dependencies)
                    short_paths.append(short_path)

        # 4. Build metadata dict for YAML
        metadata = {}

        # Copy all fields except database-managed and ETL-managed fields
        # Keep $schema but exclude other metadata fields
        skip_fields = DB_MANAGED_FIELDS | ETL_MANAGED_FIELDS | (METADATA_FIELDS - {"$schema"})

        for key, value in config.items():
            if key not in skip_fields and key != "dimensions":
                metadata[key] = value

        # 5. Add dimensions section with short indicator names
        if short_paths:
            metadata["dimensions"] = [{"property": "y", "catalogPath": path} for path in short_paths]

        # 6. Write to .meta.yml file
        metadata_file.parent.mkdir(parents=True, exist_ok=True)
        with open(metadata_file, "w") as f:
            f.write(ruamel_dump(metadata))

        log.info("graph.pull.success", file=str(metadata_file))


def _shorten_indicator_path(full_path: str, dependencies: List[str]) -> str:
    """
    Convert a full catalog path to the shortest unambiguous form.

    Args:
        full_path: Full path like "grapher/covid/latest/cases_deaths/cases_deaths#weekly_cases"
        dependencies: List of dataset URIs from DAG

    Returns:
        Shortest unambiguous path (indicator, table#indicator, or dataset/table#indicator)

    Examples:
        >>> _shorten_indicator_path(
        ...     "grapher/covid/latest/cases_deaths/cases_deaths#weekly_cases",
        ...     ["data://grapher/covid/latest/cases_deaths"]
        ... )
        "weekly_cases"
    """
    if "#" not in full_path:
        return full_path

    path_part, indicator = full_path.rsplit("#", 1)
    parts = path_part.split("/")

    if len(parts) < 5:
        # Not a full path, return as-is
        return full_path

    # Extract components: grapher/namespace/version/dataset/table
    table = parts[-1]
    dataset = parts[-2]

    # Check if this is the only dependency
    if len(dependencies) == 1:
        # If table name == dataset name, we can use just indicator
        if table == dataset:
            return indicator
        else:
            return f"{table}#{indicator}"

    # Multiple dependencies - need to check for ambiguity
    # For now, use table#indicator format
    return f"{table}#{indicator}"


def _save_graph_metadata(slug: str, source_checksum: str, config: Dict[str, Any], to_db: bool = True) -> None:
    """
    Save graph metadata to local index.json file for dirty detection and conflict detection.

    Args:
        slug: Chart slug
        source_checksum: Checksum of inputs (metadata file + dependencies)
        config: The config that was written to the database (or local config if to_db=False)
        to_db: If True, this was written to database (update db_checksum). If False, only local (update local_checksum).
    """
    graph_dir = paths.DATA_DIR / "graph" / slug
    graph_dir.mkdir(parents=True, exist_ok=True)

    # Load existing metadata to preserve checksums
    index_path = graph_dir / "index.json"
    if index_path.exists():
        with open(index_path) as f:
            metadata = json.load(f)
    else:
        metadata = {
            "channel": "graph",
            "short_name": slug,
        }

    # Store key fields for conflict detection
    last_config = {
        k: config.get(k)
        for k in ["title", "subtitle", "note", "chartTypes", "hasMapTab", "tab", "selectedEntityNames"]
        if k in config
    }

    # Always update local_checksum (step ran successfully)
    metadata["local_checksum"] = source_checksum

    # Additionally, if written to database, update db_checksum and last_config
    if to_db:
        metadata["db_checksum"] = source_checksum
        metadata["last_config"] = last_config

    with open(index_path, "w") as f:
        json.dump(metadata, f, indent=2)

    log.debug("graph.metadata_saved", path=str(index_path), checksum=source_checksum, to_db=to_db)


def _load_graph_metadata(slug: str) -> Dict[str, Any]:
    """
    Load graph metadata from local index.json file.

    Args:
        slug: Chart slug

    Returns:
        Dictionary with metadata (empty dict if file doesn't exist)
    """
    index_path = paths.DATA_DIR / "graph" / slug / "index.json"
    if not index_path.exists():
        return {}

    with open(index_path) as f:
        return json.load(f)


def fetch_graph_db_checksum(slug: str) -> Optional[str]:
    """
    Fetch the checksum of what was last written to the database for a chart.

    This is stored in the local index.json after each successful upsert with --graph.
    Used to detect if database needs updating when --graph flag is used.

    Args:
        slug: Chart slug

    Returns:
        Checksum from the last database write, or None if chart never uploaded
    """
    metadata = _load_graph_metadata(slug)
    return metadata.get("db_checksum") if metadata else None


def has_db_divergence(slug: str) -> bool:
    """
    Check if the database chart has diverged from what ETL last wrote.

    This is used in dirty detection to determine if the chart needs to be checked
    for manual edits. Returns True if:
    - Chart doesn't exist in DB yet (needs creation)
    - Chart config in DB differs from what we last wrote (potential manual edit)

    Args:
        slug: Chart slug

    Returns:
        True if DB might have diverged, False if DB matches what we last wrote
    """
    metadata = _load_graph_metadata(slug)
    if not metadata or not metadata.get("last_config"):
        # No tracking yet - either first run or chart never uploaded
        return False

    engine = get_engine()
    with Session(engine) as session:
        try:
            chart = Chart.load_chart(session, slug=slug)
        except NoResultFound:
            # Chart doesn't exist in DB yet
            return True

        # Only check for explicit config charts (not inheritance-enabled)
        if chart.isInheritanceEnabled:
            # For inheritance charts, check patch config
            if chart.chart_config.patch:
                # Has patch config = has manual overrides
                system_fields = DB_MANAGED_FIELDS | ETL_MANAGED_FIELDS | METADATA_FIELDS
                overridden_fields = [f for f in chart.chart_config.patch.keys() if f not in system_fields]
                return len(overridden_fields) > 0
            return False

        # For explicit config, compare with last written config
        stored_config = metadata.get("last_config", {})
        current_config = chart.config

        # Compare user-editable content fields
        for field in USER_CONTENT_FIELDS:
            stored_value = stored_config.get(field)
            current_value = current_config.get(field)

            # If DB value differs from what we last wrote → divergence
            if stored_value is not None and current_value != stored_value:
                return True

        return False


def _resolve_indicator_paths(session: Session, catalog_paths: List[str]) -> List[int]:
    """
    Convert indicator catalog paths to variable IDs.

    Args:
        session: Database session
        catalog_paths: List of catalog paths like "grapher/namespace/version/dataset/table#indicator"

    Returns:
        List of variable IDs

    Raises:
        ValueError: If any indicator doesn't exist in database
    """
    variable_ids = []

    for catalog_path in catalog_paths:
        log.debug("graph.resolve_indicator", catalog_path=catalog_path)

        try:
            variable = Variable.from_catalog_path(session, catalog_path)
            variable_ids.append(variable.id)
            log.debug("graph.indicator_found", variable_id=variable.id, name=variable.name)
        except NoResultFound:
            # Try to extract dataset path for helpful error message
            dataset_path = catalog_path.split("#")[0] if "#" in catalog_path else catalog_path
            raise ValueError(
                f"Indicator not found in database: {catalog_path}\n"
                f"Make sure to run the grapher step first: etlr data://{dataset_path}"
            )

    return variable_ids


def _expand_indicator_path(catalog_path: str, dependencies: List[str]) -> str:
    """
    Expand a short indicator name to a full catalog path using DAG dependencies.

    Automatically determines the minimal specificity needed (matching collections behavior):
    1. Just indicator name if unambiguous across all dependencies
    2. table#indicator if table name is needed to disambiguate
    3. dataset/table#indicator if dataset name is also needed
    4. Full path channel/namespace/version/dataset/table#indicator as fallback

    Args:
        catalog_path: Short or full catalog path (indicator, table#indicator, dataset/table#indicator, or full path)
        dependencies: List of dataset URIs from DAG (e.g., ["data://grapher/covid/latest/cases_deaths"])

    Returns:
        Full catalog path in format: channel/namespace/version/dataset/table#indicator

    Examples:
        >>> # If cases_deaths dataset has only one table "cases_deaths" with indicator "weekly_cases"
        >>> _expand_indicator_path("weekly_cases", ["data://grapher/covid/latest/cases_deaths"])
        "grapher/covid/latest/cases_deaths/cases_deaths#weekly_cases"

        >>> # If table name differs from dataset name
        >>> _expand_indicator_path("monthly#price", ["data://grapher/energy/2025/energy_prices"])
        "grapher/energy/2025/energy_prices/monthly#price"

        >>> # Full paths are returned as-is
        >>> _expand_indicator_path("grapher/covid/latest/cases_deaths/cases_deaths#weekly_cases", [])
        "grapher/covid/latest/cases_deaths/cases_deaths#weekly_cases"

    TODO: This function duplicates logic from etl.collection.core.expand.CollectionConfigExpander._expand_indicator_path()
    and etl.collection.core.create._get_expand_path_mode(). The key difference is:
    - Collections: Have loaded Table objects with .m.dataset.uri, so they use expand_path_mode ("table", "dataset", "full")
      to construct paths at runtime based on has_duplicate_table_names() check
    - Graph steps: Only have dependency URIs from DAG (no loaded tables), so must expand paths at YAML load time

    To unify these approaches:
    1. Extract common path expansion logic to etl.collection.utils
    2. Make _get_expand_path_mode() work with just dependency URIs (no need to load tables)
    3. Both graph steps and collections call the same utility with their respective inputs

    The challenge is that collections determine mode once per collection, while graph steps need to expand
    each indicator path individually. Consider whether graph steps should also determine mode once upfront.
    """
    from etl.collection.utils import get_tables_by_name_mapping

    # If it's already a full path (3+ slashes before #), return as-is
    if "#" in catalog_path:
        path_part = catalog_path.split("#")[0]
        if path_part.count("/") >= 3:
            return catalog_path

    if not dependencies:
        raise ValueError(f"Cannot expand indicator path '{catalog_path}' - no dataset dependencies found in DAG")

    # Get mapping of table names to their full URIs from all dependencies
    table_name_to_uris = get_tables_by_name_mapping(set(dependencies))

    # Parse the catalog_path to extract table_name and indicator
    if "#" not in catalog_path:
        # Just indicator name - need to find which table(s) contain it
        indicator = catalog_path
        table_name = None
        # For now, assume first dependency and that table name == dataset name
        # TODO: Could scan all tables to find the one with this indicator
        dep_uri = dependencies[0]
        catalog_base = dep_uri.split("://", 1)[1] if "://" in dep_uri else dep_uri
        catalog_base = catalog_base.split("#")[0]  # Remove any trailing #indicator
        dataset_name = catalog_base.split("/")[-1]
        return f"{catalog_base}/{dataset_name}#{indicator}"

    # Has # separator
    path_part, indicator = catalog_path.rsplit("#", 1)

    if "/" in path_part:
        # Format: dataset/table#indicator or fuller path
        slash_count = path_part.count("/")
        if slash_count >= 3:
            # Full path already
            return catalog_path
        elif slash_count == 1:
            # dataset/table#indicator - need to prepend channel/namespace/version
            dataset_name, table_name = path_part.split("/", 1)
            # Find dependency matching this dataset
            for dep_uri in dependencies:
                catalog_base = dep_uri.split("://", 1)[1] if "://" in dep_uri else dep_uri
                if catalog_base.endswith(f"/{dataset_name}"):
                    return f"{catalog_base}/{table_name}#{indicator}"
            # Fallback to first dependency if no match
            dep_uri = dependencies[0]
            catalog_base = dep_uri.split("://", 1)[1] if "://" in dep_uri else dep_uri
            channel_ns_ver = "/".join(catalog_base.split("/")[:3])
            return f"{channel_ns_ver}/{path_part}#{indicator}"
        else:
            # slash_count == 2: namespace/version/dataset or similar partial path
            dep_uri = dependencies[0]
            catalog_base = dep_uri.split("://", 1)[1] if "://" in dep_uri else dep_uri
            channel = catalog_base.split("/")[0]
            return f"{channel}/{path_part}#{indicator}"
    else:
        # Format: table#indicator - need to find the full dataset path
        table_name = path_part

        # Check if this table name exists in our dependencies
        if table_name in table_name_to_uris:
            table_uris = table_name_to_uris[table_name]
            if len(table_uris) == 1:
                # Unambiguous - use this table's URI
                table_uri = table_uris[0]
                # Remove the data:// prefix if present
                if "://" in table_uri:
                    table_uri = table_uri.split("://", 1)[1]
                return f"{table_uri}#{indicator}"
            else:
                # Multiple tables with same name - need dataset qualifier
                # Use first matching one from dependencies order
                for dep_uri in dependencies:
                    catalog_base = dep_uri.split("://", 1)[1] if "://" in dep_uri else dep_uri
                    for table_uri in table_uris:
                        table_catalog = table_uri.split("://", 1)[1] if "://" in table_uri else table_uri
                        if table_catalog.startswith(catalog_base + "/"):
                            return f"{table_catalog}#{indicator}"
                # Fallback
                table_uri = table_uris[0]
                if "://" in table_uri:
                    table_uri = table_uri.split("://", 1)[1]
                return f"{table_uri}#{indicator}"
        else:
            # Table name not found in dependencies - assume it's in first dependency
            dep_uri = dependencies[0]
            catalog_base = dep_uri.split("://", 1)[1] if "://" in dep_uri else dep_uri
            catalog_base = catalog_base.split("#")[0]  # Remove any trailing #indicator
            return f"{catalog_base}/{table_name}#{indicator}"


def _uri_to_catalog_path(uri: str) -> str:
    """
    Convert a dependency URI to a catalog path by simple string manipulation.

    Args:
        uri: URI like "data://grapher/namespace/version/dataset/table#indicator"

    Returns:
        Catalog path like "grapher/namespace/version/dataset/table#indicator"
    """
    # Validate that indicator is specified
    if "#" not in uri:
        raise ValueError(
            f"Graph dependency URI must include #indicator: {uri}\n"
            f"Example: data://grapher/namespace/version/dataset/table#indicator_name"
        )

    # Remove data:// prefix
    if uri.startswith("data://"):
        catalog_path = uri[7:]  # Remove "data://"
    else:
        catalog_path = uri

    return catalog_path


def _load_metadata_file(metadata_file: Path, yaml_params: Optional[dict] = None) -> Dict[str, Any]:
    """
    Load chart metadata from a .meta.yml file.

    Args:
        metadata_file: Path to .meta.yml file
        yaml_params: Optional dict for string substitution in YAML (e.g., {threshold: 4.73})

    Returns:
        Dictionary with chart configuration
    """
    with open(metadata_file, "r") as f:
        content = f.read()

    # Apply string substitution if yaml_params provided
    if yaml_params:
        content = content.format(**yaml_params)

    metadata = yaml.safe_load(content)

    # Empty files (only comments) are valid - return empty dict
    if not metadata:
        metadata = {}

    log.debug("graph.metadata_loaded", file=str(metadata_file), keys=list(metadata.keys()))

    return metadata


def _check_manual_overrides(
    session: Session,
    chart: Chart,
    expected_config: Dict[str, Any],
    is_inheritance_enabled: bool,
    current_checksum: str,
) -> None:
    """
    Check if chart has manual overrides from Admin UI that would be lost.

    Only raises error if BOTH of these are true:
    1. ETL metadata has changed (current_checksum != stored_checksum)
    2. Database config has diverged from what ETL last wrote

    This prevents false positives when you update .meta.yml but DB hasn't been touched.

    Args:
        session: Database session
        chart: Chart object
        expected_config: The config we're about to write (from ETL metadata)
        is_inheritance_enabled: Whether the new config will have inheritance enabled
        current_checksum: Current checksum of ETL inputs

    Raises:
        ValueError: If chart has manual overrides and graph_push=False
    """
    # System fields that don't count as "manual overrides"
    system_fields = DB_MANAGED_FIELDS | ETL_MANAGED_FIELDS | METADATA_FIELDS

    # For inheritance-enabled charts, check patch config
    if chart.isInheritanceEnabled and chart.chart_config.patch:
        overridden_fields = [field for field in chart.chart_config.patch.keys() if field not in system_fields]

        if overridden_fields:
            raise ValueError(
                f"Chart '{chart.slug}' has manual overrides from Admin UI in these fields: {overridden_fields}\n"
                f"These changes would be lost if you continue.\n"
                f"Either:\n"
                f"  1. Use --graph-pull to pull database edits to your .meta.yml file, or\n"
                f"  2. Manually copy the overrides to your .meta.yml file, or\n"
                f"  3. Use --graph-push to overwrite database with ETL metadata (WARNING: loses manual edits)"
            )

    # For explicit config charts, detect divergence between ETL and DB
    # Only do this if chart is using explicit config (not inheritance)
    if not chart.isInheritanceEnabled and not is_inheritance_enabled:
        # Load what we last wrote to this chart
        metadata = _load_graph_metadata(chart.slug)
        db_checksum = metadata.get("db_checksum")

        # If no DB checksum, this is the first upload to DB - allow it
        if not db_checksum:
            log.info("graph.first_db_upload", slug=chart.slug)
            return

        # Check if DB was manually edited by comparing current DB config with what we last wrote
        stored_config = metadata.get("last_config", {})
        current_config = chart.config

        # Compare key fields that users might edit manually
        db_changed_fields = []
        for field in USER_CONTENT_FIELDS:
            stored_value = stored_config.get(field)
            current_value = current_config.get(field)

            # If DB value differs from what we last wrote → manual edit
            if stored_value is not None and current_value != stored_value:
                db_changed_fields.append(field)

        # If DB has manual edits, raise error
        if db_changed_fields:
            etl_changed = db_checksum != current_checksum

            if etl_changed:
                # Both ETL and DB changed (divergence)
                raise ValueError(
                    f"Chart '{chart.slug}' has diverged: both ETL metadata and database were modified.\n"
                    f"Manually edited fields in database: {db_changed_fields}\n"
                    f"Last ETL values: {[f'{k}={stored_config.get(k)}' for k in db_changed_fields]}\n"
                    f"Current DB values: {[f'{k}={current_config.get(k)}' for k in db_changed_fields]}\n"
                    f"New ETL values: {[f'{k}={expected_config.get(k)}' for k in db_changed_fields]}\n"
                    f"Either:\n"
                    f"  1. Use --graph-pull to pull database edits to your .meta.yml file, or\n"
                    f"  2. Manually update your .meta.yml file to match the database edits, or\n"
                    f"  3. Use --graph-push to overwrite database with ETL metadata (WARNING: loses manual edits)"
                )
            else:
                # Only DB changed (no ETL changes)
                raise ValueError(
                    f"Chart '{chart.slug}' has manual edits from Admin UI that would be overwritten.\n"
                    f"Manually edited fields in database: {db_changed_fields}\n"
                    f"Last values: {[f'{k}={stored_config.get(k)}' for k in db_changed_fields]}\n"
                    f"Current DB values: {[f'{k}={current_config.get(k)}' for k in db_changed_fields]}\n"
                    f"Either:\n"
                    f"  1. Use --graph-pull to pull database edits to your .meta.yml file, or\n"
                    f"  2. Keep the database edits (do nothing and don't run this step), or\n"
                    f"  3. Use --graph-push to overwrite database with ETL metadata (WARNING: loses manual edits)"
                )
