"""
Graph step logic for creating and updating charts in the grapher database.

This module handles the creation and updating of charts (graphs) as part of the ETL pipeline.
Charts can either inherit metadata from a single indicator or have explicit metadata defined
in a .meta.yml file.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog
import yaml
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

from apps.chart_sync.admin_api import AdminAPI
from etl.config import GRAPHER_USER_ID, OWID_ENV
from etl.db import get_engine
from etl.grapher.model import Chart, Variable

log = structlog.get_logger()


def upsert_graph(
    slug: str,
    metadata_file: Optional[Path],
    dependencies: List[str],
    force: bool = False,
) -> int:
    """
    Create or update a chart in the grapher database.

    Args:
        slug: Chart slug (e.g., "fur-farming-ban")
        metadata_file: Optional path to .meta.yml file with chart metadata
        dependencies: List of dataset URIs from DAG (e.g., ["data://grapher/..."])
        force: If True, overwrite manual edits made in Admin UI

    Returns:
        Chart ID

    Raises:
        ValueError: If no metadata file or indicators can't be resolved
    """
    log.info("graph.upsert", slug=slug, metadata_file=metadata_file, dependencies=dependencies)

    engine = get_engine()

    with Session(engine) as session:
        # 1. Load metadata file (now required for specifying indicators)
        if not metadata_file or not metadata_file.exists():
            raise ValueError(
                f"Graph '{slug}' requires a .meta.yml file to specify indicators. "
                f"Expected at: etl/steps/graphs/**/{slug}.meta.yml"
            )

        metadata = _load_metadata_file(metadata_file)

        # 2. Get indicator catalog paths from metadata
        indicator_paths = metadata.get("indicators", [])
        if not indicator_paths:
            raise ValueError(f"Graph metadata file must specify 'indicators' list: {metadata_file}")

        log.info("graph.indicators", paths=indicator_paths)

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
            has_explicit_config = any(key in metadata for key in ["title", "subtitle", "note", "chartTypes"])

            if has_explicit_config:
                # User provided explicit metadata - use it
                log.info("graph.explicit_metadata", file=str(metadata_file))
                config = {k: v for k, v in metadata.items() if k != "indicators"}
                config["slug"] = slug
                config.setdefault("$schema", "https://files.ourworldindata.org/schemas/grapher-schema.009.json")
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
            is_inheritance_enabled = False

        # 6. Check for manual overrides if chart exists and not forcing
        if chart and not force:
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

        return chart_id


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

    if not metadata:
        raise ValueError(f"Empty or invalid metadata file: {metadata_file}")

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
