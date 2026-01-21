"""
Graph step logic for creating and updating charts in the grapher database.

This module handles the creation and updating of charts (graphs) as part of the ETL pipeline.
Charts can either inherit metadata from a single indicator or have explicit metadata defined
in a .meta.yml file.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog
import yaml
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

from apps.chart_sync.admin_api import AdminAPI
from etl import paths
from etl.config import GRAPHER_USER_ID, OWID_ENV
from etl.db import get_engine
from etl.grapher.model import Chart, Variable

log = structlog.get_logger()


def upsert_graph(
    slug: str,
    metadata_file: Optional[Path],
    dependencies: List[str],
    source_checksum: str,
    force: bool = False,
) -> int:
    """
    Create or update a chart in the grapher database.

    Args:
        slug: Chart slug (e.g., "fur-farming-ban")
        metadata_file: Optional path to .meta.yml file with chart metadata
        dependencies: List of dataset URIs from DAG (e.g., ["data://grapher/..."])
        source_checksum: Checksum of inputs (metadata file + dependencies) for dirty detection
        force: If True, overwrite manual edits made in Admin UI

    Returns:
        Chart ID

    Raises:
        ValueError: If no metadata file or indicators can't be resolved
    """
    log.info("graph.upsert", slug=slug, metadata_file=metadata_file, dependencies=dependencies)

    engine = get_engine()

    with Session(engine) as session:
        # 1. Get indicator catalog paths from DAG dependencies
        # Dependencies are URIs like: data://grapher/namespace/version/dataset/table#indicator
        indicator_paths = []
        for dep_uri in dependencies:
            # Convert URI to catalog path (strip data:// prefix)
            catalog_path = _uri_to_catalog_path(dep_uri)
            indicator_paths.append(catalog_path)

        log.info("graph.indicators_from_dag", paths=indicator_paths)

        # 2. Load metadata file if it exists (for overriding inherited config)
        if metadata_file and metadata_file.exists():
            metadata = _load_metadata_file(metadata_file)
        else:
            metadata = {}

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

        # 6. Check for manual overrides if chart exists, not forcing, and we're keeping inheritance enabled
        # (If we're switching to explicit config, the overrides will be replaced anyway)
        if chart and not force and is_inheritance_enabled:
            _check_manual_overrides(session, chart)

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

        # Save checksum to local file for dirty detection
        _save_graph_metadata(slug, source_checksum)

        return chart_id


def _save_graph_metadata(slug: str, source_checksum: str) -> None:
    """
    Save graph metadata to local index.json file for dirty detection.

    Args:
        slug: Chart slug
        source_checksum: Checksum of inputs (metadata file + dependencies)
    """
    graph_dir = paths.DATA_DIR / "graphs" / slug
    graph_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "channel": "graph",
        "short_name": slug,
        "source_checksum": source_checksum,
    }

    index_path = graph_dir / "index.json"
    with open(index_path, "w") as f:
        json.dump(metadata, f, indent=2)

    log.debug("graph.metadata_saved", path=str(index_path), checksum=source_checksum)


def _load_graph_metadata(slug: str) -> Dict[str, Any]:
    """
    Load graph metadata from local index.json file.

    Args:
        slug: Chart slug

    Returns:
        Dictionary with metadata (empty dict if file doesn't exist)
    """
    index_path = paths.DATA_DIR / "graphs" / slug / "index.json"
    if not index_path.exists():
        return {}

    with open(index_path) as f:
        return json.load(f)


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


def _load_metadata_file(metadata_file: Path) -> Dict[str, Any]:
    """
    Load chart metadata from a .meta.yml file.

    Args:
        metadata_file: Path to .meta.yml file

    Returns:
        Dictionary with chart configuration
    """
    with open(metadata_file, "r") as f:
        metadata = yaml.safe_load(f)

    # Empty files (only comments) are valid - return empty dict
    if not metadata:
        metadata = {}

    log.debug("graph.metadata_loaded", file=str(metadata_file), keys=list(metadata.keys()))

    return metadata


def _check_manual_overrides(session: Session, chart: Chart) -> None:
    """
    Check if chart has manual overrides from Admin UI.

    If the chart has isInheritanceEnabled=True but has a non-empty patch config,
    it means someone manually overrode fields in the Admin UI. We should warn
    the user rather than silently overwriting their work.

    Args:
        session: Database session
        chart: Chart object

    Raises:
        ValueError: If chart has manual overrides and force=False
    """
    # System fields that are always in patch config (not actual overrides)
    SYSTEM_FIELDS = {"id", "slug", "$schema", "version", "dimensions", "isPublished", "createdAt", "updatedAt"}

    if chart.isInheritanceEnabled and chart.chart_config.patch:
        # Filter out system fields to find actual user overrides
        overridden_fields = [field for field in chart.chart_config.patch.keys() if field not in SYSTEM_FIELDS]

        if overridden_fields:
            raise ValueError(
                f"Chart '{chart.slug}' has manual overrides from Admin UI in these fields: {overridden_fields}\n"
                f"These changes would be lost if you continue.\n"
                f"Either:\n"
                f"  1. Copy the overrides to your .meta.yml file, or\n"
                f"  2. Use --force to reset chart to ETL-managed state (WARNING: loses manual edits)"
            )

    # If inheritance is disabled, it means the chart has explicit config
    # In this case, we're intentionally managing it from ETL
    if not chart.isInheritanceEnabled:
        log.info("graph.explicit_config", slug=chart.slug)
