"""Script to detect typos and semantic issues in explorer views.

This script uses a three-layer approach to find issues:
1. Codespell - fast dictionary-based typo detection
2. Claude AI - comprehensive detection of typos and semantic issues and absurdities
3. Claude AI - group similar issues and prune spurious ones

"""

import asyncio
import json
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import anthropic
import click
import pandas as pd
from anthropic.types import TextBlock
from rich import print as rprint
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeRemainingColumn
from rich_click.rich_command import RichCommand
from sqlalchemy.orm import Session
from structlog import get_logger

from etl import config
from etl.db import get_engine, read_sql
from etl.grapher.model import ChartConfig
from etl.paths import BASE_DIR

# Initialize logger
log = get_logger()
console = Console()

# Claude model configuration
# Choose model based on speed vs quality tradeoff:
# - Haiku: Fastest, cheapest (18% faster, 72% cheaper), excellent quality for this task
# - Sonnet: Higher quality but slower and more expensive
# - Opus: Highest quality but much slower and more expensive
#
# Testing showed Haiku finds same semantic issues as Sonnet with minimal quality difference,
# making it the best choice for large-scale analysis.

# Model for individual issue detection (used many times - cost matters)
CLAUDE_MODEL = "claude-3-5-haiku-20241022"  # Fast: $1/M in, $5/M out (RECOMMENDED)
# CLAUDE_MODEL = "claude-3-7-sonnet-20250219"  # Balanced: $3/M in, $15/M out
# CLAUDE_MODEL = "claude-opus-4-20250514"  # Quality: $15/M in, $75/M out

# Model for grouping/pruning (used once per collection - quality matters more than cost)
GROUPING_MODEL = "claude-3-7-sonnet-20250219"  # Better reasoning for filtering false positives
# GROUPING_MODEL = "claude-opus-4-20250514"  # Best quality if budget allows

# Batch size configuration
# Number of views to check per API call. Larger batches are faster and cheaper,
# but batch sizes >100 cause quality degradation (model misses issues).
#
# Testing results (100 views):
# - batch=10:  86s, $0.25 (baseline)
# - batch=30:  31s, $0.17 (2.8x faster, 33% cheaper)
# - batch=50:  18s, $0.15 (4.8x faster, 40% cheaper) ✓ OPTIMAL
# - batch=100: 20s, $0.16 (4.3x faster, 36% cheaper, slight variance)
# - batch=200: Quality degradation - misses semantic issues!
#
# Default of 50 provides best balance of speed, cost, and reliability.
DEFAULT_BATCH_SIZE = 20

# Concurrency limit for API requests
# Anthropic rate limits: typically 5-50 concurrent requests depending on tier
# Increase if you have higher tier access, decrease if you hit rate limits
MAX_CONCURRENT_REQUESTS = 25

# Model pricing (USD per million tokens)
# Source: https://www.anthropic.com/api-pricing (verified 2025-10-28)
# Note: We use the regular API, not Batch API (which offers 50% discount but is asynchronous)
MODEL_PRICING = {
    "claude-3-5-haiku-20241022": {"input": 1.0, "output": 5.0},
    "claude-3-7-sonnet-20250219": {"input": 3.0, "output": 15.0},
    "claude-opus-4-20250514": {"input": 15.0, "output": 75.0},
}

# Fields to inspect for typos and semantic issues
# Both codespell and Claude AI will check these fields
CHART_FIELDS_TO_CHECK = [
    "title",
    "subtitle",
    "note",
]

VARIABLE_FIELDS_TO_CHECK = [
    "variable_name",
    "variable_description",
    "variable_title_public",
    "variable_description_short",
    "variable_description_from_producer",
    "variable_description_key",
    "variable_description_processing",
]


def parse_multidim_view_id(view_id: str, mdim_config: str | None) -> dict[str, str]:
    """Parse multidim viewId into dimension values.

    ViewId format: choice values are ordered alphabetically by dimension slug.
    E.g., for dimensions [metric, antigen], viewId "comparison__vaccinated" maps to
    {antigen: comparison, metric: vaccinated} (alphabetical order: antigen, metric).

    Args:
        view_id: ViewId string like "level_side_by_side__number__both"
        mdim_config: JSON config string with dimension definitions

    Returns:
        Dict mapping dimension slugs to choice slugs
    """
    if not view_id or not mdim_config:
        return {}

    try:
        config = json.loads(mdim_config)
        dimensions = config.get("dimensions", [])

        if not dimensions:
            return {}

        # Split viewId by double underscore to get choice values
        parts = view_id.split("__")

        if len(parts) != len(dimensions):
            return {}

        # Sort dimension slugs alphabetically (this is the viewId ordering convention)
        dim_slugs_sorted = sorted([dim.get("slug", "") for dim in dimensions])

        # Map sorted dimension slugs to viewId parts
        result = {}
        for dim_slug, choice_slug in zip(dim_slugs_sorted, parts):
            if dim_slug:
                result[dim_slug] = choice_slug

        return result
    except (json.JSONDecodeError, AttributeError, KeyError):
        return {}


def parse_chart_config(chart_config_raw: Any) -> dict[str, Any]:
    """Parse chart_config field which can be a dict (chart views) or JSON string (explorer views).

    Args:
        chart_config_raw: Raw chart_config value - can be dict or JSON string

    Returns:
        Parsed chart config dict, or empty dict if not present
    """
    if isinstance(chart_config_raw, dict):
        return chart_config_raw
    elif chart_config_raw:
        try:
            return json.loads(chart_config_raw)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def parse_dimensions(dimensions_raw: Any, mdim_config: str | None = None) -> dict[str, Any]:
    """Parse dimensions field which can be JSON (explorers) or just a viewId (mdims).

    Args:
        dimensions_raw: Raw dimensions value from database
        mdim_config: For multidims, the JSON config string

    Returns:
        Parsed dimensions dict, or empty dict if not applicable
    """
    if not dimensions_raw:
        return {}
    if isinstance(dimensions_raw, dict):
        # Already parsed
        return dimensions_raw
    if isinstance(dimensions_raw, str) and dimensions_raw.startswith("{"):
        # Explorer: dimensions is a JSON string
        return json.loads(dimensions_raw)
    # Mdim: dimensions is a viewId - parse it with the config
    return parse_multidim_view_id(str(dimensions_raw), mdim_config)


def get_codespell_path() -> Path | None:
    """Get path to codespell binary.

    Returns:
        Path to codespell if available, None otherwise
    """
    # Check in venv first
    venv_codespell = BASE_DIR / ".venv" / "bin" / "codespell"
    if venv_codespell.exists():
        return venv_codespell
    return None


def build_explorer_url(
    explorer_slug: str,
    dimensions: dict[str, Any],
    view_type: str = "explorer",
    mdim_published: bool = True,
    mdim_catalog_path: str | None = None,
) -> str:
    """Build URL for explorer or multidim view with dimensions.

    Args:
        explorer_slug: Explorer slug (e.g., 'air-pollution')
        dimensions: Dictionary of dimension key-value pairs
        view_type: Type of view ('explorer' or 'multidim')
        mdim_published: Whether the multidim is published
        mdim_catalog_path: Catalog path for unpublished multidims

    Returns:
        Full URL to the view with properly encoded query parameters
    """
    from urllib.parse import quote, urlencode

    base_url = config.OWID_ENV.site or "https://ourworldindata.org"

    is_mdim = view_type == "multidim"

    if is_mdim:
        if mdim_published:
            # Published multidim: /grapher/{slug}?dimensions
            url = f"{base_url}/grapher/{explorer_slug}"
            if dimensions:
                params = {k: v for k, v in dimensions.items() if v}
                if params:
                    url += "?" + urlencode(params)
        else:
            # Unpublished multidim: /admin/grapher/{catalogPath}?dimensions#{slug}
            catalog_path = quote(mdim_catalog_path or "", safe="")
            url = f"{base_url}/admin/grapher/{catalog_path}"
            if dimensions:
                params = {k: v for k, v in dimensions.items() if v}
                if params:
                    url += "?" + urlencode(params)
            url += f"#{explorer_slug}"
    else:
        # Regular explorer: /explorers/{slug}?dimensions
        url = f"{base_url}/explorers/{explorer_slug}"
        if dimensions:
            params = {k: v for k, v in dimensions.items() if v}
            if params:
                url += "?" + urlencode(params)

    return url


def get_text_context(text: str, typo: str, context_words: int = 10) -> str:
    """Extract context around a typo in text.

    Args:
        text: Full text containing the typo
        typo: The typo to find
        context_words: Number of words to show before and after

    Returns:
        Context string with typo highlighted
    """
    # Find the typo in text as a whole word (case insensitive)
    import re

    # Search for typo as a whole word using word boundaries
    pattern = r"\b" + re.escape(typo) + r"\b"
    match = re.search(pattern, text, re.IGNORECASE)

    if not match:
        # If not found as whole word, try substring match
        text_lower = text.lower()
        typo_lower = typo.lower()
        pos = text_lower.find(typo_lower)
        if pos == -1:
            # If still not found, return first N chars
            return text[:200] + ("..." if len(text) > 200 else "")
    else:
        pos = match.start()

    # Split into words
    words = text.split()

    # Find which word contains the typo
    char_count = 0
    typo_word_idx = -1
    for i, word in enumerate(words):
        if char_count <= pos < char_count + len(word) + 1:  # +1 for space
            typo_word_idx = i
            break
        char_count += len(word) + 1

    if typo_word_idx == -1:
        return text[:200] + ("..." if len(text) > 200 else "")

    # Get context words
    start_idx = max(0, typo_word_idx - context_words)
    end_idx = min(len(words), typo_word_idx + context_words + 1)

    context_words_list = words[start_idx:end_idx]
    context = " ".join(context_words_list)

    # Add ellipsis if truncated
    if start_idx > 0:
        context = "..." + context
    if end_idx < len(words):
        context = context + "..."

    return context


def extract_human_readable_text(config_json: str) -> str:
    """Extract only human-readable fields from config JSON and strip template variables.

    Args:
        config_json: JSON string of explorer/multidim config

    Returns:
        Cleaned text with only human-readable content and templates removed
    """
    try:
        config = json.loads(config_json)
    except json.JSONDecodeError:
        # Fallback to full text if parse fails
        return config_json

    texts = []

    def extract(obj: Any, depth: int = 0) -> None:
        if depth > 10:  # Prevent infinite recursion
            return

        if isinstance(obj, dict):
            for key, value in obj.items():
                # Extract common human-readable field names
                if key in [
                    "title",
                    "title_variant",  # Multidim title variant
                    "subtitle",
                    "note",
                    "description",
                    "label",
                    "text",
                    "tooltip",
                    "name",  # Dimension/choice names
                    "group",  # Choice groups
                    "explorerTitle",
                    "explorerSubtitle",
                    "relatedQuestionText",  # Explorer schema
                    "sourceName",  # Display metadata
                    "sourceDesc",  # Source description (grapher config)
                    "additionalInfo",  # Display metadata
                    "dataPublishedBy",  # Display metadata
                ]:
                    if isinstance(value, str):
                        # Strip {template} patterns like {welfare['welfare_type'][wel]}
                        cleaned = re.sub(r"\{[^}]+\}", "", value)
                        if cleaned.strip():
                            texts.append(cleaned)
                extract(value, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, depth + 1)

    extract(config)
    return "\n".join(texts)


def check_typos(views: list[dict[str, Any]]) -> dict[int | str, list[dict[str, Any]]]:
    """Run codespell on all views at once for performance.

    Args:
        views: List of view dictionaries

    Returns:
        Dictionary mapping view_id (int or str for config pseudo-views) to list of typo issues
    """
    codespell_path = get_codespell_path()
    if not codespell_path:
        return {}

    # Create a temporary directory for all text files
    temp_dir = tempfile.mkdtemp()
    view_files = {}  # Map view_id to list of (field_name, file_path, text)

    try:
        # Write all texts to temporary files
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Preparing views for spell check", total=len(views))

            for view in views:
                # Skip config pseudo-views - they'll be added separately below
                if view.get("is_config_view"):
                    progress.update(task, advance=1)
                    continue

                # Handle chart_config - it's already a dict for chart views, but a JSON string for explorer views
                chart_config = parse_chart_config(view.get("chart_config"))

                # For chart views, dimensions come from chart_config, not from view directly
                if view.get("view_type") == "chart":
                    dimensions = []  # Chart views don't have variable dimensions like explorers
                else:
                    dimensions = parse_dimensions(view["dimensions"])
                view_id = view["id"]
                view_files[view_id] = []

                # Collect texts to check with field names
                texts_to_check = []

                # Add chart fields
                for field_name in CHART_FIELDS_TO_CHECK:
                    value = chart_config.get(field_name, "")
                    if value:
                        texts_to_check.append((field_name, value))

                # Add variable metadata fields
                for field_name in VARIABLE_FIELDS_TO_CHECK:
                    values = view.get(field_name, [])
                    if isinstance(values, list):
                        for i, value in enumerate(values):
                            if value:
                                texts_to_check.append((f"{field_name}_{i}", str(value)))
                    elif values:
                        texts_to_check.append((field_name, str(values)))

                # Write each text to a separate file
                for field_name, text in texts_to_check:
                    if text and text.strip():
                        file_path = Path(temp_dir) / f"view_{view_id}_{field_name}.txt"
                        file_path.write_text(text)
                        view_files[view_id].append((field_name, file_path, text))

                progress.update(task, advance=1)

        # Add collection configs as additional files to check
        # Config pseudo-views were already created in load_views()
        rprint("  [cyan]Adding collection configs for spell check...[/cyan]")
        for view in views:
            if view.get("is_config_view"):
                config_view_id = view["id"]
                config_text = view.get("config_text", "")

                if config_text and config_text.strip():
                    view_files[config_view_id] = []
                    file_path = Path(temp_dir) / f"view_{config_view_id}_collection_config.txt"
                    file_path.write_text(config_text)
                    view_files[config_view_id].append(("collection_config", file_path, config_text))

        # Run codespell once on the entire directory
        rprint("  [cyan]Running spell checker...[/cyan]")
        ignore_file = BASE_DIR / ".codespell-ignore.txt"
        cmd = [str(codespell_path), temp_dir]
        if ignore_file.exists():
            cmd.extend(["--ignore-words", str(ignore_file)])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        # Parse results and map back to views
        issues_by_view: dict[int | str, list[dict[str, Any]]] = {}
        result_lines = [line for line in result.stdout.strip().split("\n") if line and "==>" in line]

        if result_lines:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("Processing typos found", total=len(result_lines))

                for line in result_lines:
                    # Parse: /tmp/dir/view_123_field.txt:1: typo ==> correction
                    parts = line.split("==>")
                    if len(parts) != 2:
                        progress.update(task, advance=1)
                        continue

                    left = parts[0].strip()
                    correction = parts[1].strip()

                    # Extract file path, line number, and typo
                    file_parts = left.rsplit(":", 2)
                    if len(file_parts) < 3:
                        progress.update(task, advance=1)
                        continue

                    file_path = file_parts[0]
                    line_num = file_parts[1]
                    typo = file_parts[2].strip()

                    # Parse line number
                    try:
                        line_num_int = int(line_num)
                    except ValueError:
                        progress.update(task, advance=1)
                        continue

                    # Extract view_id and field from filename
                    filename = Path(file_path).name
                    if not filename.startswith("view_"):
                        progress.update(task, advance=1)
                        continue

                    # Parse filename: view_{view_id}_{field_name}.txt
                    # For mdim views, view_id contains underscores, so we need to find it in view_files
                    # CRITICAL: We must use the ACTUAL file path from codespell to find the right view_id
                    # Rather than parsing the filename, search view_files for the matching file path
                    filename_without_ext = filename.replace(".txt", "")
                    if not filename_without_ext.startswith("view_"):
                        progress.update(task, advance=1)
                        continue

                    # Search for the file path in view_files to find the correct view_id
                    view_id = None
                    field_name = None
                    text = ""
                    file_path_obj = Path(file_path)

                    for vid, file_list in view_files.items():
                        for fname, fpath, ftext in file_list:
                            if fpath == file_path_obj:
                                view_id = vid
                                text = ftext
                                # Use the original field name from fname, removing numeric suffix if present
                                if fname.rsplit("_", 1)[-1].isdigit():
                                    field_name = fname.rsplit("_", 1)[0]
                                else:
                                    field_name = fname
                                break
                        if view_id is not None:
                            break

                    if view_id is None:
                        progress.update(task, advance=1)
                        continue

                    # Verify the typo actually exists in the text
                    if typo.lower() not in text.lower():
                        log.warning(
                            f"Typo '{typo}' not found in text for view_id {view_id}, field {field_name}. "
                            f"Codespell reported it in {file_path}. Skipping."
                        )
                        progress.update(task, advance=1)
                        continue

                    # Get context from the specific line where the typo was found
                    text_lines = text.split("\n")
                    if 1 <= line_num_int <= len(text_lines):
                        typo_line = text_lines[line_num_int - 1]
                        context = get_text_context(typo_line, typo)
                    else:
                        # Fallback to searching entire text if line number is out of range
                        context = get_text_context(text, typo)

                    # Handle config pseudo-views differently from regular views
                    if isinstance(view_id, str) and view_id.startswith("config_"):
                        # This is a collection config pseudo-view
                        explorer_slug = view_id.replace("config_", "")
                        # Find any view from this explorer to get basic info
                        sample_view = next((v for v in views if v.get("explorerSlug") == explorer_slug), None)
                        if not sample_view:
                            progress.update(task, advance=1)
                            continue

                        view_title = f"Collection Config ({explorer_slug})"
                        view_url = f"https://ourworldindata.org/explorers/{explorer_slug}"

                        issue = {
                            "view_id": view_id,
                            "explorer_slug": explorer_slug,
                            "view_title": view_title,
                            "view_url": view_url,
                            "issue_type": "typo",
                            "field": field_name,
                            "context": context,
                            "typo": typo,
                            "correction": correction,
                            "explanation": f"Typo in {field_name}: '{typo}' → '{correction}'",
                            "source": "codespell",
                        }
                    else:
                        # Regular view
                        view = next((v for v in views if v["id"] == view_id), None)
                        if not view:
                            progress.update(task, advance=1)
                            continue

                        chart_config = parse_chart_config(view.get("chart_config"))
                        dimensions = parse_dimensions(view["dimensions"])
                        view_title = chart_config.get("title", "")
                        mdim_published = bool(view.get("mdim_published", True))
                        mdim_catalog_path = view.get("mdim_catalog_path")
                        view_url = build_explorer_url(
                            view["explorerSlug"],
                            dimensions,
                            view.get("view_type", "explorer"),
                            mdim_published,
                            mdim_catalog_path,
                        )

                        issue = {
                            "view_id": view_id,
                            "explorer_slug": view["explorerSlug"],
                            "view_title": view_title,
                            "view_url": view_url,
                            "issue_type": "typo",
                            "field": field_name,
                            "context": context,
                            "typo": typo,
                            "correction": correction,
                            "explanation": f"Typo in {field_name}: '{typo}' → '{correction}'",
                            "source": "codespell",
                        }

                    if view_id not in issues_by_view:
                        issues_by_view[view_id] = []
                    issues_by_view[view_id].append(issue)

                    progress.update(task, advance=1)

        return issues_by_view

    finally:
        # Clean up temp directory
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)


def fetch_explorer_data(explorer_slugs: list[str] | None = None) -> pd.DataFrame:
    """Fetch all explorer views with their metadata from the database.

    Args:
        explorer_slugs: Optional list of explorer slugs to filter by

    Returns:
        DataFrame with columns: id, explorerSlug, dimensions, chartConfigId,
                                chart_config, and multiple variable_* columns
    """
    where_clause = "WHERE ev.error IS NULL"
    if explorer_slugs:
        slugs_str = "', '".join(explorer_slugs)
        where_clause += f" AND ev.explorerSlug IN ('{slugs_str}')"

    # Fetch views without configs first
    query = f"""
        SELECT
            ev.id,
            'explorer' as view_type,
            ev.explorerSlug,
            ev.dimensions,
            ev.chartConfigId,
            cc.full as chart_config,
            v.id as variable_id,
            v.name as variable_name,
            v.unit as variable_unit,
            v.description as variable_description,
            v.shortUnit as variable_short_unit,
            v.shortName as variable_short_name,
            v.titlePublic as variable_title_public,
            v.titleVariant as variable_title_variant,
            v.descriptionShort as variable_description_short,
            v.descriptionFromProducer as variable_description_from_producer,
            v.descriptionKey as variable_description_key,
            v.descriptionProcessing as variable_description_processing
        FROM explorer_views ev
        LEFT JOIN chart_configs cc ON ev.chartConfigId = cc.id
        LEFT JOIN JSON_TABLE(
            cc.full,
            '$.dimensions[*]' COLUMNS(
                variableId INT PATH '$.variableId'
            )
        ) jt ON TRUE
        LEFT JOIN variables v ON jt.variableId = v.id
        {where_clause}
        ORDER BY ev.explorerSlug, ev.id
    """

    log.info("Fetching explorer data from database...")
    df = read_sql(query)
    log.info(f"Fetched {len(df)} explorer view records (views x variables, which will be aggregated)")

    # Fetch explorer configs separately (much more efficient)
    config_where = ""
    if explorer_slugs:
        slugs_str = "', '".join(explorer_slugs)
        config_where = f"WHERE slug IN ('{slugs_str}')"

    config_query = f"""
        SELECT slug, config
        FROM explorers
        {config_where}
    """
    log.info("Fetching explorer configs...")
    configs_df = read_sql(config_query)

    # Deduplicate configs by slug (keep first occurrence)
    # This prevents cartesian product issues with NULL slugs during merge
    configs_df = configs_df.drop_duplicates(subset=["slug"], keep="first")

    # Merge configs with views
    df = df.merge(
        configs_df.rename(columns={"slug": "explorerSlug", "config": "explorer_config"}), on="explorerSlug", how="left"
    )

    return df


def aggregate_explorer_views(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate explorer views by grouping variables for each view.

    Args:
        df: Raw dataframe from fetch_explorer_data

    Returns:
        Aggregated dataframe with one row per explorer view
    """
    # Group by explorer view and aggregate variable metadata
    # Use dropna=False to preserve rows with NULL explorerSlug (if any)
    agg_df = (
        df.groupby(
            ["id", "view_type", "explorerSlug", "dimensions", "chartConfigId", "chart_config", "explorer_config"],
            dropna=False,
        )
        .agg(
            {
                "variable_id": lambda x: list(x.dropna()),
                "variable_name": lambda x: list(x.dropna()),
                "variable_unit": lambda x: list(x.dropna()),
                "variable_description": lambda x: list(x.dropna()),
                "variable_short_unit": lambda x: list(x.dropna()),
                "variable_short_name": lambda x: list(x.dropna()),
                "variable_title_public": lambda x: list(x.dropna()),
                "variable_title_variant": lambda x: list(x.dropna()),
                "variable_description_short": lambda x: list(x.dropna()),
                "variable_description_from_producer": lambda x: list(x.dropna()),
                "variable_description_key": lambda x: list(x.dropna()),
                "variable_description_processing": lambda x: list(x.dropna()),
            }
        )
        .reset_index()
    )

    return agg_df


def fetch_multidim_data(slug_filters: list[str] | None = None) -> pd.DataFrame:
    """Fetch multidimensional indicator data from the database.

    Args:
        slug_filters: Optional list of multidim slugs to filter by

    Returns:
        DataFrame with columns matching explorer structure: id, explorerSlug, dimensions,
                                chartConfigId, chart_config, and variable_* columns
    """
    where_clause = ""
    if slug_filters:
        slugs_str = "', '".join(slug_filters)
        where_clause = f"WHERE md.slug IN ('{slugs_str}')"

    # Fetch views without configs first
    query = f"""
        SELECT
            mx.id as id,
            'multidim' as view_type,
            md.slug as explorerSlug,
            mx.viewId as dimensions,
            md.published as mdim_published,
            md.catalogPath as mdim_catalog_path,
            mx.chartConfigId,
            cc.full as chart_config,
            v.id as variable_id,
            v.name as variable_name,
            v.unit as variable_unit,
            v.description as variable_description,
            v.shortUnit as variable_short_unit,
            v.shortName as variable_short_name,
            v.titlePublic as variable_title_public,
            v.titleVariant as variable_title_variant,
            v.descriptionShort as variable_description_short,
            v.descriptionFromProducer as variable_description_from_producer,
            v.descriptionKey as variable_description_key,
            v.descriptionProcessing as variable_description_processing
        FROM multi_dim_data_pages md
        JOIN multi_dim_x_chart_configs mx ON md.id = mx.multiDimId
        LEFT JOIN chart_configs cc ON mx.chartConfigId = cc.id
        LEFT JOIN variables v ON mx.variableId = v.id
        {where_clause}
        ORDER BY md.slug, mx.viewId
    """

    log.info("Fetching multidimensional indicator data from database...")
    df = read_sql(query)
    log.info(f"Fetched {len(df)} multidim view records")

    # Fetch multidim configs separately (much more efficient)
    config_where = ""
    if slug_filters:
        slugs_str = "', '".join(slug_filters)
        config_where = f"WHERE slug IN ('{slugs_str}')"

    config_query = f"""
        SELECT slug, config
        FROM multi_dim_data_pages
        {config_where}
    """
    log.info("Fetching multidim configs...")
    configs_df = read_sql(config_query)

    # Deduplicate configs by slug (keep first occurrence)
    # This prevents cartesian product issues with NULL slugs during merge
    configs_df = configs_df.drop_duplicates(subset=["slug"], keep="first")

    # Merge configs with views
    df = df.merge(
        configs_df.rename(columns={"slug": "explorerSlug", "config": "explorer_config"}), on="explorerSlug", how="left"
    )

    return df


def aggregate_multidim_views(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate multidim views by grouping variables for each view.

    Args:
        df: Raw dataframe from fetch_multidim_data

    Returns:
        Aggregated dataframe with one row per multidim view
    """
    # Group by multidim view and aggregate variable metadata (same as explorers)
    # Use dropna=False to preserve rows with NULL explorerSlug
    agg_df = (
        df.groupby(
            [
                "id",
                "view_type",
                "explorerSlug",
                "dimensions",
                "mdim_published",
                "mdim_catalog_path",
                "chartConfigId",
                "chart_config",
                "explorer_config",
            ],
            dropna=False,
        )
        .agg(
            {
                "variable_id": lambda x: list(x.dropna()),
                "variable_name": lambda x: list(x.dropna()),
                "variable_unit": lambda x: list(x.dropna()),
                "variable_description": lambda x: list(x.dropna()),
                "variable_short_unit": lambda x: list(x.dropna()),
                "variable_short_name": lambda x: list(x.dropna()),
                "variable_title_public": lambda x: list(x.dropna()),
                "variable_title_variant": lambda x: list(x.dropna()),
                "variable_description_short": lambda x: list(x.dropna()),
                "variable_description_from_producer": lambda x: list(x.dropna()),
                "variable_description_key": lambda x: list(x.dropna()),
                "variable_description_processing": lambda x: list(x.dropna()),
            }
        )
        .reset_index()
    )

    return agg_df


def fetch_chart_configs(chart_slugs: list[str] | None = None) -> list[dict[str, Any]]:
    """Fetch chart configs from database by slug.

    Args:
        chart_slugs: Optional list of chart slugs to filter by

    Returns:
        List of chart config dictionaries with flattened human-readable fields
    """
    log.info("Fetching chart configs from database...")
    engine = get_engine()
    with Session(engine) as session:
        query = session.query(ChartConfig)

        # Filter by slugs if provided
        if chart_slugs:
            query = query.filter(ChartConfig.slug.in_(chart_slugs))
        else:
            # By default, only get published charts with slugs
            # Note: Explorer views use unpublished charts (without slugs), so there's no duplication
            query = query.filter(ChartConfig.slug.isnot(None))

        charts = query.all()

        if not charts:
            log.info("Fetched 0 chart configs")
            return []

        # Convert to format similar to explorer views
        result = []
        for chart in charts:
            chart_dict = {
                "id": str(chart.id),  # Chart config ID (UUID)
                "slug": chart.slug,
                "view_type": "chart",  # Mark as chart (not explorer view)
                "chart_config": chart.full,
            }
            result.append(chart_dict)

        log.info(f"Fetched {len(result)} chart configs")
        return result


def extract_chart_fields(chart_config: dict[str, Any]) -> list[tuple[str, Any]]:
    """Extract human-readable fields from chart config JSON.

    Args:
        chart_config: Chart configuration dictionary from chart_configs.full

    Returns:
        List of (field_name, field_value) tuples for all text fields
    """
    fields = []

    # Top-level text fields (excluding internalNotes as requested)
    for field_name in ["title", "subtitle", "note", "sourceDesc"]:
        if field_name in chart_config and chart_config[field_name]:
            fields.append((field_name, chart_config[field_name]))

    # Extract display names and other text from dimensions
    if "dimensions" in chart_config and chart_config["dimensions"]:
        for dim_idx, dimension in enumerate(chart_config["dimensions"]):
            if "display" in dimension and dimension["display"]:
                for display_key, display_value in dimension["display"].items():
                    # Only include string fields (names, labels, tooltips, etc.)
                    if isinstance(display_value, str) and display_value.strip():
                        field_key = f"dimension_{dim_idx}_display_{display_key}"
                        fields.append((field_key, display_value))

    return fields


def calculate_cost(
    input_tokens: int, output_tokens: int, cache_creation_tokens: int = 0, cache_read_tokens: int = 0
) -> float:
    """Calculate cost in USD for given token counts including cache costs.

    Args:
        input_tokens: Number of regular input tokens (non-cached)
        output_tokens: Number of output tokens
        cache_creation_tokens: Number of tokens written to cache
        cache_read_tokens: Number of tokens read from cache

    Returns:
        Cost in USD
    """
    if CLAUDE_MODEL not in MODEL_PRICING:
        raise ValueError(f"Unknown model '{CLAUDE_MODEL}'. Please add pricing to MODEL_PRICING dictionary.")
    pricing = MODEL_PRICING[CLAUDE_MODEL]

    # Regular input tokens
    regular_input_tokens = input_tokens - cache_creation_tokens - cache_read_tokens
    input_cost = (regular_input_tokens / 1_000_000) * pricing["input"]

    # Cache creation cost (25% premium over regular input)
    cache_write_cost = (cache_creation_tokens / 1_000_000) * pricing["input"] * 1.25

    # Cache read cost (90% discount, so 10% of regular price)
    cache_read_cost = (cache_read_tokens / 1_000_000) * pricing["input"] * 0.01

    # Output cost
    output_cost = (output_tokens / 1_000_000) * pricing["output"]

    return input_cost + cache_write_cost + cache_read_cost + output_cost


def estimate_tokens(text: str) -> int:
    """Estimate number of tokens in text.

    Uses a simple heuristic: ~4 characters per token for English text.
    This is reasonably accurate for cost estimation purposes.

    Args:
        text: Text to estimate tokens for

    Returns:
        Estimated number of tokens
    """
    return len(text) // 4


def extract_json_array(content: str) -> str:
    """Extract JSON array from Claude response, handling markdown and extra text.

    Args:
        content: Raw response text from Claude

    Returns:
        Extracted JSON array as string
    """
    import re

    # Handle markdown code blocks
    if "```json" in content:
        return content.split("```json")[1].split("```")[0].strip()
    elif "```" in content:
        return content.split("```")[1].split("```")[0].strip()

    # Try to parse as-is first
    content = content.strip()
    try:
        json.loads(content)
        return content
    except json.JSONDecodeError:
        pass

    # Find JSON array in text (e.g., after explanation text)
    # Look for pattern: [ followed by { or ] (start of array)
    match = re.search(r"(\[(?:\s*\{|\s*\]))", content)
    if match:
        start = match.start(1)
        # Find the matching closing bracket
        bracket_count = 0
        for i in range(start, len(content)):
            if content[i] == "[":
                bracket_count += 1
            elif content[i] == "]":
                bracket_count -= 1
                if bracket_count == 0:
                    return content[start : i + 1]

    # Fallback: return original content
    return content


def call_claude(
    client: anthropic.Anthropic, model: str, max_tokens: int, prompt: str, max_retries: int = 3
) -> anthropic.types.Message:
    """Call Claude API with exponential backoff retry logic.

    Args:
        client: Anthropic client instance
        model: Model name to use
        max_tokens: Maximum tokens in response
        prompt: Prompt text to send
        max_retries: Maximum number of retry attempts

    Returns:
        API response message

    Raises:
        Exception: If all retries fail
    """
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            return response
        except anthropic.APIError as e:
            if attempt < max_retries - 1:
                # Exponential backoff: 2^attempt seconds (2s, 4s, 8s)
                wait_time = 2 ** (attempt + 1)
                log.warning(f"API error (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                log.error(f"API error after {max_retries} attempts: {e}")
                raise
        except Exception as e:
            log.error(f"Unexpected error calling Claude API: {e}")
            raise

    # This should never be reached due to raise in the loop, but satisfies type checker
    raise RuntimeError("All retry attempts failed")


async def check_view_async(
    client: anthropic.AsyncAnthropic,
    view: dict[str, Any],
) -> tuple[list[dict[str, Any]], int, int, int, int]:
    """Check a single view for issues asynchronously.

    Returns:
        Tuple of (issues, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens)
    """
    # Handle config pseudo-views differently
    if view.get("is_config_view"):
        config_text = view.get("config_text", "")
        if not config_text or not config_text.strip():
            return [], 0, 0, 0, 0

        # For configs, check the extracted human-readable text
        fields_to_check = [("collection_config", config_text)]
    elif view.get("view_type") == "chart":
        # Chart config handling
        chart_config = view.get("chart_config", {})
        if not chart_config:
            return [], 0, 0, 0, 0

        # Extract fields from chart config using helper function
        fields_to_check = extract_chart_fields(chart_config)
    else:
        # Regular explorer view handling
        chart_config = parse_chart_config(view.get("chart_config"))

        # Build field list for prompt (only include non-empty fields)
        fields_to_check = []

        # Add chart fields
        for field_name in CHART_FIELDS_TO_CHECK:
            value = chart_config.get(field_name, "")
            if value and str(value).strip():
                fields_to_check.append((field_name, str(value)))

        # Add variable metadata fields
        for field_name in VARIABLE_FIELDS_TO_CHECK:
            values = view.get(field_name, [])
            if isinstance(values, list):
                for i, value in enumerate(values):
                    if value and str(value).strip():
                        # Include index in field name for multiple variables
                        indexed_field_name = f"{field_name}_{i}" if len(values) > 1 else field_name
                        fields_to_check.append((indexed_field_name, str(value)))
            elif values and str(values).strip():
                fields_to_check.append((field_name, str(values)))

    # Skip if no substantial content (need at least 1 non-empty field)
    if len(fields_to_check) < 1:
        return [], 0, 0, 0, 0

    # Build prompt text
    fields_text = "\n".join([f"{name.replace('_', ' ').title()}: {value}" for name, value in fields_to_check])

    # Split prompt into cacheable (instructions) and variable (data) parts
    # The instructions are cached across all requests, saving ~62% on costs
    instructions = """You are checking metadata for an Our World in Data chart. Find ONLY mistakes that would damage OWID's reputation.

Report ONLY critical errors:
1. **Spelling errors** (e.g., "recieve" → "receive", "governemnt" → "government")
2. **Obvious nonsensical mistakes** (e.g., title speaks about CO2 emissions but ALL other fields speak about vaccines)

DO NOT report:
- Style suggestions or writing improvements
- Minor inconsistencies or nitpicks
- Issues that say "everything looks fine" - if there are NO errors, return []

Return ONLY JSON array with format: issue_type (either "typo" or "semantic") field (where the issue occurs), context (phrase containing the issue), and explanation (why that's an issue):
[{"issue_type": "typo", "typo": "recieve", "correction": "receive", "field": "title", "context": "People recieve money"}]
[{"issue_type": "semantic", "field": "title", "context": "CO2 emissions", "explanation": "Title is about CO2 emissions but descriptions are about vaccines"}]
[]

Here is the metadata to check:"""

    raw_text = ""  # Initialize for type checker
    response = None  # Initialize for type checker

    # Retry logic for transient API errors (rate limits, overloaded, etc.)
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = await client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1024,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": instructions,
                                "cache_control": {"type": "ephemeral"},
                            },
                            {
                                "type": "text",
                                "text": fields_text,
                            },
                        ],
                    }
                ],
            )
            break  # Success, exit retry loop
        except Exception as e:
            # Check if this is a retryable error
            error_msg = str(e)
            is_retryable = any(
                keyword in error_msg.lower()
                for keyword in ["overloaded", "rate_limit", "rate limit", "529", "429", "timeout"]
            )

            if is_retryable and attempt < max_retries - 1:
                # Exponential backoff: 2^attempt seconds
                wait_time = 2**attempt
                log.debug(
                    f"View {view['id']}: Retryable error (attempt {attempt + 1}/{max_retries}), waiting {wait_time}s: {e}"
                )
                await asyncio.sleep(wait_time)
            else:
                # Non-retryable error or max retries exceeded
                if is_retryable:
                    log.warning(f"View {view['id']}: Max retries exceeded for API error: {e}")
                raise  # Re-raise to be caught by outer exception handler

    # Check if we have a response (should always be true if no exception was raised)
    if response is None:
        log.warning(f"View {view['id']}: No response received after retries")
        return [], 0, 0, 0, 0

    try:
        content_block = response.content[0]
        if not isinstance(content_block, TextBlock):
            return [], 0, 0, 0, 0

        raw_text = content_block.text.strip()

        # Extract usage information including cache stats
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens
        cache_creation_tokens = getattr(response.usage, "cache_creation_input_tokens", 0)
        cache_read_tokens = getattr(response.usage, "cache_read_input_tokens", 0)

        # Handle cases where Claude returns just "[]" or empty responses
        if not raw_text or raw_text == "[]":
            return [], input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens

        content = extract_json_array(raw_text)

        # Validate we have actual JSON content
        if not content or content == "[]":
            return [], input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens

        view_issues = json.loads(content)

        # Filter out non-issues (Claude sometimes says "no errors found")
        view_issues = [
            issue
            for issue in view_issues
            if issue.get("issue_type") in ["typo", "semantic"] and (issue.get("typo") or issue.get("explanation"))
        ]

        # Create a lookup dict for field values
        fields_dict = dict(fields_to_check)

        # Get view title for display
        if view.get("is_config_view"):
            # For config pseudo-views, use a descriptive title
            view_title = f"Collection Config ({view['explorerSlug']})"
        else:
            # For regular views, prefer title from chart, fall back to first variable title
            view_title = fields_dict.get("title", "")
            if not view_title:
                # Try variable_title_public or variable_name as fallback
                for field_name, value in fields_to_check:
                    if field_name.startswith("variable_title_public") or field_name.startswith("variable_name"):
                        view_title = value
                        break

        # Enrich each issue with view metadata
        for issue in view_issues:
            issue["view_id"] = view["id"]
            # For charts, use 'slug' instead of 'explorerSlug'
            issue["explorer_slug"] = view.get("slug") if view.get("view_type") == "chart" else view.get("explorerSlug")
            issue["view_title"] = view_title
            issue["source"] = "ai"  # Mark as AI-detected

            # Build URL
            if view.get("view_type") == "chart":
                # Chart configs use direct chart URL
                chart_slug = view.get("slug", "")
                issue["view_url"] = f"https://ourworldindata.org/grapher/{chart_slug}" if chart_slug else ""
            elif view.get("is_config_view"):
                # For config pseudo-views, build base URL without dimension parameters
                view_type = view.get("view_type", "explorer")
                mdim_published = bool(view.get("mdim_published", True))
                mdim_catalog_path = view.get("mdim_catalog_path")

                issue["view_url"] = build_explorer_url(
                    view["explorerSlug"],
                    {},  # No dimensions for config view
                    view_type,
                    mdim_published,
                    mdim_catalog_path,
                )
            else:
                # Regular explorer views
                dimensions = parse_dimensions(view.get("dimensions"))
                mdim_published = bool(view.get("mdim_published", True))
                mdim_catalog_path = view.get("mdim_catalog_path")

                issue["view_url"] = build_explorer_url(
                    view["explorerSlug"],
                    dimensions,
                    view.get("view_type", "explorer"),
                    mdim_published,
                    mdim_catalog_path,
                )

            # Add context from the actual field content
            field = issue.get("field", "")
            # Look up the field value (handle both exact match and indexed fields like "variable_name_0")
            for field_name, value in fields_to_check:
                if field_name == field or field_name.startswith(field + "_"):
                    issue["context"] = value[:200]
                    break

        return view_issues, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens

    except json.JSONDecodeError as e:
        # Log the actual response to help debug the issue
        log.warning(
            f"View {view['id']}: Claude returned invalid JSON (length: {len(raw_text)}). "
            f"Response preview: {raw_text[:200]}... Error: {e}"
        )
        return [], 0, 0, 0, 0
    except Exception as e:
        log.warning(f"Error processing view {view['id']}: {e}")
        return [], 0, 0, 0, 0


def check_issues(
    views: list[dict[str, Any]], api_key: str | None, batch_size: int = 10, dry_run: bool = False
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Check for semantic inconsistencies and absurdities using Claude API.

    Args:
        views: List of view dictionaries
        api_key: Anthropic API key
        batch_size: Ignored (kept for compatibility)
        dry_run: If True, estimate costs without making API calls

    Returns:
        Tuple of (list of semantic issues found, usage stats dict)
    """
    if not api_key:
        log.warning("No Claude API key provided, skipping issue checks")
        return [], {}

    if dry_run:
        # Estimate costs by simulating what would be checked
        total_input_tokens = 0
        total_output_tokens = 0
        views_to_check = 0

        for view in views:
            # Build field list (same logic as check_view_async)
            fields_to_check = []

            # Handle config pseudo-views differently
            if view.get("is_config_view"):
                config_text = view.get("config_text", "")
                if config_text and config_text.strip():
                    fields_to_check.append(("collection_config", config_text))
            else:
                # Regular view handling
                chart_config = parse_chart_config(view.get("chart_config"))

                # Add chart fields
                for field_name in CHART_FIELDS_TO_CHECK:
                    value = chart_config.get(field_name, "")
                    if value and str(value).strip():
                        fields_to_check.append((field_name, str(value)))

                # Add variable metadata fields
                for field_name in VARIABLE_FIELDS_TO_CHECK:
                    values = view.get(field_name, [])
                    if isinstance(values, list):
                        for i, value in enumerate(values):
                            if value and str(value).strip():
                                indexed_field_name = f"{field_name}_{i}" if len(values) > 1 else field_name
                                fields_to_check.append((indexed_field_name, str(value)))
                    elif values and str(values).strip():
                        fields_to_check.append((field_name, str(values)))

            # Skip if no substantial content (need at least 1 non-empty field)
            if len(fields_to_check) < 1:
                continue

            # Estimate prompt size (includes prompt template + field content)
            prompt_text = "\n".join([f"{name}: {value}" for name, value in fields_to_check])

            # Add ~250 tokens for the prompt template/instructions
            total_input_tokens += estimate_tokens(prompt_text) + 250
            # Average response is typically 50-150 tokens per view (more fields = potentially more issues)
            total_output_tokens += 100
            views_to_check += 1

        rprint(f"[yellow]Would check {views_to_check}/{len(views)} views[/yellow]")
        return [], {"input_tokens": total_input_tokens, "output_tokens": total_output_tokens}

    # Run async checks in parallel with concurrency limiting
    async def check_all_views():
        client = anthropic.AsyncAnthropic(api_key=api_key)
        try:
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

            async def check_with_semaphore(view):
                async with semaphore:
                    return await check_view_async(client, view)

            tasks = [check_with_semaphore(view) for view in views]

            all_issues = []
            total_input_tokens = 0
            total_output_tokens = 0
            total_cache_creation_tokens = 0
            total_cache_read_tokens = 0

            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                task_id = progress.add_task("Checking for semantic issues", total=len(views))

                for coro in asyncio.as_completed(tasks):
                    issues, input_tokens, output_tokens, cache_creation, cache_read = await coro
                    all_issues.extend(issues)
                    total_input_tokens += input_tokens
                    total_output_tokens += output_tokens
                    total_cache_creation_tokens += cache_creation
                    total_cache_read_tokens += cache_read
                    progress.update(task_id, advance=1)

            return (
                all_issues,
                total_input_tokens,
                total_output_tokens,
                total_cache_creation_tokens,
                total_cache_read_tokens,
            )
        finally:
            await client.close()

    all_issues, total_input_tokens, total_output_tokens, cache_creation_tokens, cache_read_tokens = asyncio.run(
        check_all_views()
    )

    usage_stats = {
        "input_tokens": total_input_tokens,
        "output_tokens": total_output_tokens,
        "cache_creation_tokens": cache_creation_tokens,
        "cache_read_tokens": cache_read_tokens,
    }
    return all_issues, usage_stats


def group_issues(issues: list[dict[str, Any]], api_key: str | None) -> tuple[list[dict[str, Any]], int]:
    """Group similar issues and prune spurious typos using Claude in a single API call.

    Uses Claude to:
    1. Filter typos to remove false positives based on context
    2. Group remaining typos by similarity
    3. Group semantic issues by similarity

    Args:
        issues: List of issue dictionaries
        api_key: Anthropic API key

    Returns:
        Tuple of (grouped/pruned issues, tokens used)
    """
    if not api_key or not issues:
        return issues, 0

    # Separate typos from semantic issues
    typo_issues = [i for i in issues if i.get("issue_type") == "typo"]
    other_issues = [i for i in issues if i.get("issue_type") != "typo"]

    # Prepare data for Claude - combine both typo pruning/grouping and semantic grouping
    typo_data = []
    for idx, issue in enumerate(typo_issues):
        typo_data.append(
            {
                "index": idx,
                "typo": issue.get("typo", ""),
                "correction": issue.get("correction", ""),
                "context": issue.get("context", "")[:300],
            }
        )

    semantic_data = []
    for idx, issue in enumerate(other_issues):
        semantic_data.append(
            {
                "index": idx,
                "type": issue.get("issue_type"),
                "explanation": issue.get("explanation", "")[:200],
            }
        )

    # Single combined prompt for grouping and pruning issues
    prompt = f"""You are reviewing issues found in OWID (Our World in Data) content. Your goal: REMOVE low-value issues from the list, KEEP only issues that pose reputational risk.

TASK 1: GROUP SIMILAR ISSUES
- Typos: Group by identical misspelling + correction pair
- Semantic issues: Group by identical problem description (across different views)

INPUT DATA:

Typos ({len(typo_data)} issues):
{json.dumps(typo_data, indent=2) if typo_data else "[]"}

Semantic issues ({len(semantic_data)} issues):
{json.dumps(semantic_data, indent=2) if semantic_data else "[]"}

TASK 2: DECIDE WHICH ISSUE GROUPS TO REMOVE

Review each group's context. REMOVE groups from the list if they are FALSE POSITIVES or LOW-VALUE:

TYPO GROUPS - REMOVE from list if:
- NOT actually typos: The flagged "typo" is correct in context (scientific names like "Escherichia coli", technical terms like "eigenvalue", proper nouns like "McDonald")
- Codespell false positives that are actually correct spellings

TYPO GROUPS - KEEP in list if:
- Real typos: Actual misspellings like "teh" → "the", "recieve" → "receive"

SEMANTIC ISSUE GROUPS - REMOVE from list if:
- Minor style suggestions that don't affect meaning
- Tiny inconsistencies (capitalization preferences, punctuation style)
- Subjective writing preferences

SEMANTIC ISSUE GROUPS - KEEP in list (reputational risk):
- Inappropriate/offensive language (bad words, slurs, profanity) - CRITICAL: ALWAYS KEEP
- Factual errors or contradictions
- Major inconsistencies that confuse readers
- Unclear or ambiguous statements
- Anything that could damage OWID's reputation or credibility

OUTPUT FORMAT:

Return ONLY valid JSON:
{{
  "typo_groups": {{"group_name": [0, 1, 2]}},
  "semantic_groups": {{"group_name": [0, 1, 2]}},
  "spurious_typo_groups": ["group_name1"],
  "spurious_semantic_groups": ["group_name2"]
}}

CRITICAL: When unsure about severity, KEEP the issue in the list. Missing a reputational risk is worse than a false positive.
Write arrays explicitly [0, 1, 2], NOT Python code."""

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = call_claude(
            client=client,
            model=GROUPING_MODEL,  # Use higher-quality model for grouping/pruning
            max_tokens=3072,
            prompt=prompt,
        )

        # Parse response
        content_block = response.content[0]
        if not isinstance(content_block, TextBlock):
            log.warning("Unexpected content block type, using simple grouping fallback")
            return group_typos(typo_issues) + other_issues, 0

        content = content_block.text.strip()
        # Extract JSON object - handle markdown blocks or find JSON in text
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        else:
            # Find JSON object in text (first { to last })
            start = content.find("{")
            end = content.rfind("}")
            if start != -1 and end != -1:
                content = content[start : end + 1]

        try:
            result = json.loads(content)
        except json.JSONDecodeError as e:
            log.warning(f"Failed to parse Claude response: {e}")
            log.warning(f"Response content (first 500 chars): {content[:500]}")
            raise

        tokens_used = response.usage.input_tokens + response.usage.output_tokens

        # Get grouping results
        typo_grouping = result.get("typo_groups", {})
        spurious_groups = set(result.get("spurious_typo_groups", []))

        # Group typos, excluding spurious groups
        grouped_typos = []
        pruned_count = 0

        for group_name, group_indices in typo_grouping.items():
            if not group_indices:
                continue

            # Skip entire group if marked as spurious
            if group_name in spurious_groups:
                pruned_count += len(group_indices)
                continue

            valid_indices = [i for i in group_indices if i < len(typo_issues)]
            if not valid_indices:
                continue

            representative = typo_issues[valid_indices[0]].copy()
            representative["similar_count"] = len(valid_indices)
            grouped_typos.append(representative)

        if pruned_count > 0:
            rprint(f"  [dim]Pruned {pruned_count} spurious typo(s)[/dim]")

        # Group semantic issues
        semantic_grouping = result.get("semantic_groups", {})
        spurious_semantic_groups = set(result.get("spurious_semantic_groups", []))
        grouped_semantic = []
        grouped_semantic_indices = set()
        semantic_pruned_count = 0

        for group_name, group_indices in semantic_grouping.items():
            if not group_indices:
                continue

            # Skip entire group if marked as spurious (low-value)
            if group_name in spurious_semantic_groups:
                semantic_pruned_count += len(group_indices)
                grouped_semantic_indices.update(group_indices)  # Mark as handled
                continue

            valid_indices = [i for i in group_indices if i < len(other_issues)]
            if not valid_indices:
                continue

            representative = other_issues[valid_indices[0]].copy()
            representative["similar_count"] = len(valid_indices)
            representative["group_views"] = [other_issues[i].get("view_title", "") for i in valid_indices]
            grouped_semantic.append(representative)
            # Track which indices were grouped
            grouped_semantic_indices.update(valid_indices)

        # Add ungrouped semantic issues (ones that weren't grouped)
        for idx, issue in enumerate(other_issues):
            if idx not in grouped_semantic_indices:
                ungrouped = issue.copy()
                ungrouped["similar_count"] = 1
                grouped_semantic.append(ungrouped)

        if semantic_pruned_count > 0:
            rprint(f"  [dim]Pruned {semantic_pruned_count} low-value semantic issue(s)[/dim]")

        # Validation: Warn if unexpected issue loss (beyond intentional pruning)
        expected_kept = len(other_issues) - semantic_pruned_count
        if len(grouped_semantic) != expected_kept:
            log.warning(
                f"Semantic issue count mismatch after grouping: "
                f"input={len(other_issues)}, pruned={semantic_pruned_count}, output={len(grouped_semantic)}, "
                f"expected={expected_kept}"
            )

        return grouped_typos + grouped_semantic, tokens_used

    except Exception as e:
        log.warning(f"Error in combined grouping/pruning: {e}, using fallback")
        return group_typos(typo_issues) + other_issues, 0


def group_typos(typo_issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group typos using simple string matching.

    Args:
        typo_issues: List of typo issue dictionaries

    Returns:
        List of grouped typo issues
    """
    from collections import defaultdict

    groups = defaultdict(list)
    for issue in typo_issues:
        # Group key for typos
        key = (
            issue.get("explorer_slug", ""),
            issue.get("field", ""),
            issue.get("typo", ""),
            issue.get("correction", ""),
            issue.get("context", "")[:200],
        )
        groups[key].append(issue)

    # Create grouped issues with count
    grouped_issues = []
    for group in groups.values():
        representative = group[0].copy()
        representative["similar_count"] = len(group)
        representative["group_views"] = [issue.get("view_title", "") for issue in group]
        grouped_issues.append(representative)

    return grouped_issues


def display_issues(
    issues: list[dict[str, Any]],
    all_explorers: list[str],
    mdim_slugs: set[str],
) -> None:
    """Display grouped issues.

    Args:
        issues: List of already-grouped issue dictionaries
        all_explorers: List of all explorer slugs that were analyzed
        mdim_slugs: Set of slugs that are multidimensional indicators
    """
    if not issues:
        rprint("[green]✓ No issues found![/green]")
        return

    # Group issues by explorer
    from collections import defaultdict

    issues_by_explorer = defaultdict(list)
    for issue in issues:
        explorer_slug = issue.get("explorer_slug", "unknown")
        issues_by_explorer[explorer_slug].append(issue)

    # Deduplicate codespell typos within each explorer by (typo, correction) pair
    # Keep track of how many duplicates were removed
    for explorer_slug in issues_by_explorer:
        seen_typos = {}  # Map (typo, correction) -> first issue
        deduplicated = []

        for issue in issues_by_explorer[explorer_slug]:
            # Only deduplicate codespell typos (not AI issues)
            if issue.get("source") == "codespell" and issue.get("issue_type") == "typo":
                typo_key = (issue.get("typo", ""), issue.get("correction", ""))
                if typo_key in seen_typos:
                    # Increment the count on the first occurrence
                    seen_typos[typo_key]["similar_count"] = seen_typos[typo_key].get("similar_count", 1) + 1
                else:
                    # First occurrence of this typo
                    issue["similar_count"] = 1
                    seen_typos[typo_key] = issue
                    deduplicated.append(issue)
            else:
                # Keep all non-typo issues
                deduplicated.append(issue)

        issues_by_explorer[explorer_slug] = deduplicated

    # Display summary
    total_unique = len(issues)
    # Calculate total original count from similar_count fields
    total_original = sum(issue.get("similar_count", 1) for issue in issues)
    num_collections = len(issues_by_explorer)
    rprint(
        f"\n[bold]Found {total_original} total issues ({total_unique} unique) across {num_collections} collection(s)[/bold]"
    )

    # Display issues grouped by explorer
    for explorer_slug in sorted(issues_by_explorer.keys()):
        explorer_issues = issues_by_explorer[explorer_slug]

        # Display header with appropriate label and color
        is_mdim = mdim_slugs and explorer_slug in mdim_slugs
        label = "Multidim" if is_mdim else "Explorer"
        color = "magenta" if is_mdim else "cyan"

        rprint(f"\n[bold {color}]{'=' * 80}[/bold {color}]")
        rprint(f"[bold {color}]{label}: {explorer_slug}[/bold {color}]")
        rprint(f"[bold {color}]{'=' * 80}[/bold {color}]")

        rprint("\n[bold]Issues:[/bold]")

        # Print each issue with full details
        for i, issue in enumerate(explorer_issues, 1):
            view_title = issue.get("view_title", "")
            view_url = issue.get("view_url", "")
            field = issue.get("field", "unknown")
            context = issue.get("context", "")
            similar_count = issue.get("similar_count", 1)
            source = issue.get("source", "unknown")

            # Print issue header with embedded clickable link
            view_id = issue.get("view_id", issue.get("id", "unknown"))

            if view_url:
                # Embed link in title for clickable terminal support
                rprint(f"\n[bold]{i}. [link={view_url}]{view_title}[/link][/bold] [dim](view_id: {view_id})[/dim]")
            else:
                rprint(f"\n[bold]{i}. {view_title}[/bold] [dim](view_id: {view_id})[/dim]")

            # Show group count if more than 1
            if similar_count > 1:
                rprint(f"   [dim]({similar_count} similar occurrences in this explorer)[/dim]")

            # Print issue details
            issue_type = issue.get("issue_type", "semantic")

            if issue_type == "typo":
                typo = issue.get("typo", "")
                correction = issue.get("correction", "")
                explanation = issue.get("explanation", "")

                # If typo/correction are properly filled, show them
                if typo and correction:
                    if source == "codespell":
                        rprint(f"   [yellow]Typo (codespell):[/yellow] '{typo}' → '{correction}'")
                    else:
                        rprint(f"   [yellow]Typo (AI):[/yellow] '{typo}' → '{correction}'")
                elif explanation:
                    # If no typo/correction but explanation exists, show the explanation
                    rprint(f"   [yellow]Typo (AI):[/yellow] {explanation}")
                else:
                    # Last resort: show context
                    rprint("   [yellow]Typo (AI):[/yellow] See context below")
            else:
                explanation = issue.get("explanation", "")
                rprint(f"   [yellow]Issue (AI):[/yellow] {explanation}")

            rprint(f"   [yellow]Field:[/yellow] {field}")
            if context:
                rprint(f"   [yellow]Context:[/yellow] {context}")

    # Show clean explorers/mdims if we have the full list
    if all_explorers and mdim_slugs is not None:
        explorers_with_issues = set(issues_by_explorer.keys())
        # Filter out NaN values (from NULL slugs) before sorting
        all_explorers_valid = {e for e in all_explorers if isinstance(e, str)}
        explorers_with_issues_valid = {e for e in explorers_with_issues if isinstance(e, str)}
        all_clean = sorted(all_explorers_valid - explorers_with_issues_valid)

        # Separate into explorers and mdims
        clean_explorers = [e for e in all_clean if e not in mdim_slugs]
        clean_mdims = [e for e in all_clean if e in mdim_slugs]

        if clean_explorers or clean_mdims:
            rprint(f"\n[bold green]{'=' * 80}[/bold green]")

            if clean_explorers:
                rprint(f"[bold cyan]✓ No issues found in {len(clean_explorers)} explorer(s):[/bold cyan]")
                rprint(f"[cyan]{', '.join(clean_explorers)}[/cyan]")

            if clean_mdims:
                if clean_explorers:
                    rprint()  # Add spacing between the two lists
                rprint(f"[bold magenta]✓ No issues found in {len(clean_mdims)} multidim(s):[/bold magenta]")
                rprint(f"[magenta]{', '.join(clean_mdims)}[/magenta]")


def load_views(
    slug_list: list[str] | None, chart_slug_list: list[str] | None, limit: int | None
) -> list[dict[str, Any]]:
    """Load views from database and aggregate by view ID.

    Args:
        slug_list: List of explorer/multidim slugs to filter by
        chart_slug_list: List of chart slugs to filter by
        limit: Maximum number of views to return

    Returns:
        List of view dictionaries (explorers, multidims, and charts)
    """
    # Fetch data from explorers, multidimensional indicators, and charts
    # Determine what to fetch based on provided filters
    if slug_list is not None or chart_slug_list is None:
        # Fetch explorers/multidims if: (1) slug_list provided, OR (2) no filters at all
        df_explorers = fetch_explorer_data(explorer_slugs=slug_list)
        df_mdims = fetch_multidim_data(slug_filters=slug_list)
    else:
        # Only chart slugs provided - skip explorer/multidim data for performance
        df_explorers = pd.DataFrame()
        df_mdims = pd.DataFrame()

    # Fetch charts if: (1) chart_slug_list provided, OR (2) no filters at all
    if chart_slug_list is not None or slug_list is None:
        chart_configs = fetch_chart_configs(chart_slugs=chart_slug_list)
    else:
        # Only explorer/multidim slugs provided - skip charts for performance
        chart_configs = []

    # Check if we got any results
    if df_explorers.empty and df_mdims.empty and not chart_configs:
        if slug_list or chart_slug_list:
            all_slugs = []
            if slug_list:
                all_slugs.extend(slug_list)
            if chart_slug_list:
                all_slugs.extend([f"chart:{s}" for s in chart_slug_list])
            slugs_str = ", ".join(all_slugs)
            rprint(f"[red]Error: No views found for slug(s) '{slugs_str}'[/red]")
        else:
            rprint("[red]Error: No explorer, multidim, or chart views found in database[/red]")
        return []

    # Aggregate views
    agg_df_explorers = aggregate_explorer_views(df_explorers) if not df_explorers.empty else pd.DataFrame()
    agg_df_mdims = aggregate_multidim_views(df_mdims) if not df_mdims.empty else pd.DataFrame()
    agg_df = pd.concat([agg_df_explorers, agg_df_mdims], ignore_index=True)

    # Sort by explorerSlug to group views from same collection together
    # This ensures batches don't mix different collections, which would confuse Claude
    if not agg_df.empty:
        agg_df = agg_df.sort_values("explorerSlug").reset_index(drop=True)

    views: list[dict[str, Any]] = agg_df.to_dict("records")  # type: ignore

    # Add chart configs as views (charts are treated as single "views" without collections)
    views.extend(chart_configs)

    # Apply limit if specified
    if limit is not None and limit > 0:
        views = views[:limit]
        rprint(f"[yellow]Limiting to first {limit} views (for testing)[/yellow]")

    # Add collection config pseudo-views for AI semantic checking
    # These will be checked alongside regular views
    configs_by_slug = {}
    for view in views:
        slug = view.get("explorerSlug")
        config = view.get("explorer_config")
        view_type = view.get("view_type", "explorer")
        if slug and config and slug not in configs_by_slug:
            # Create a pseudo-view for this collection's config
            config_view = {
                "id": f"config_{slug}",
                "view_type": view_type,
                "explorerSlug": slug,
                "config_text": extract_human_readable_text(config),
                "is_config_view": True,
                # Copy multidim-specific fields for URL building
                "mdim_published": view.get("mdim_published", True),
                "mdim_catalog_path": view.get("mdim_catalog_path"),
                # Add empty lists for expected fields
                "variable_name": [],
                "variable_description": [],
                "variable_title_public": [],
                "variable_description_short": [],
                "variable_description_from_producer": [],
                "variable_description_key": [],
                "variable_description_processing": [],
            }
            configs_by_slug[slug] = config_view
            views.append(config_view)

    if slug_list:
        slugs_str = ", ".join(slug_list)
        rprint(f"[cyan]Filtering to slug(s): {slugs_str}[/cyan]")

    config_count = len(configs_by_slug)
    view_count = len(views) - config_count

    # Count charts separately (they have view_type="chart")
    chart_count = len([v for v in views if v.get("view_type") == "chart"])

    breakdown = f"{len(agg_df_explorers)} explorers + {len(agg_df_mdims)} multidims"
    if chart_count > 0:
        breakdown += f" + {chart_count} charts"

    rprint(
        f"[cyan]Aggregated to {view_count} unique views + {config_count} collection configs "
        f"({breakdown})...[/cyan]\n"
    )

    return views


def estimate_check_costs(
    views: list[dict[str, Any]],
    skip_typos: bool,
    skip_issues: bool,
    batch_size: int,
) -> tuple[int, int]:
    """Estimate token costs for checks without running them.

    Returns:
        Tuple of (input_tokens, output_tokens)
    """
    total_input_tokens = 0
    total_output_tokens = 0

    if not skip_issues:
        rprint("[yellow]Estimating issue check costs (dry run)...[/yellow]")
        _, usage_stats = check_issues(views, config.ANTHROPIC_API_KEY, batch_size, dry_run=True)
        total_input_tokens += usage_stats.get("input_tokens", 0)
        total_output_tokens += usage_stats.get("output_tokens", 0)

    return total_input_tokens, total_output_tokens


def run_checks(
    views: list[dict[str, Any]],
    skip_typos: bool,
    skip_issues: bool,
    batch_size: int,
) -> tuple[list[dict[str, Any]], int, int, int, int]:
    """Run all enabled checks and return issues and token usage.

    Returns:
        Tuple of (all_issues, total_input_tokens, total_output_tokens, cache_creation_tokens, cache_read_tokens)
    """
    all_issues = []
    total_input_tokens = 0
    total_output_tokens = 0
    cache_creation_tokens = 0
    cache_read_tokens = 0

    # Check typos with codespell
    if not skip_typos:
        rprint("[cyan]Checking for typos with codespell...[/cyan]")
        issues_by_view = check_typos(views)
        for view_issues in issues_by_view.values():
            all_issues.extend(view_issues)
        codespell_typos = len([i for i in all_issues if i["issue_type"] == "typo"])
        rprint(f"[green]✓ Found {codespell_typos} typos with codespell[/green]\n")

    # Check for all issues with Claude (typos + semantic)
    if not skip_issues:
        rprint("[bold]Checking for typos and semantic issues with Claude...[/bold]")
        issues, usage_stats = check_issues(views, config.ANTHROPIC_API_KEY, batch_size, dry_run=False)
        all_issues.extend(issues)
        total_input_tokens += usage_stats.get("input_tokens", 0)
        total_output_tokens += usage_stats.get("output_tokens", 0)
        cache_creation_tokens += usage_stats.get("cache_creation_tokens", 0)
        cache_read_tokens += usage_stats.get("cache_read_tokens", 0)
        claude_typos = len([i for i in issues if i.get("issue_type") == "typo"])
        semantic_issues = len([i for i in issues if i.get("issue_type") == "semantic"])
        rprint(f"[green]✓ Found {claude_typos} typos and {semantic_issues} semantic issues with Claude[/green]\n")

    return all_issues, total_input_tokens, total_output_tokens, cache_creation_tokens, cache_read_tokens


def display_cost_estimate(
    total_input_tokens: int, total_output_tokens: int, skip_issues: bool, num_views: int = 0
) -> None:
    """Display cost estimate in dry-run mode with prompt caching."""
    # Estimate grouping/pruning tokens if issue checks are enabled
    grouping_tokens_estimate = 0
    if not skip_issues:
        grouping_tokens_estimate = 500
        total_input_tokens += grouping_tokens_estimate // 2
        total_output_tokens += grouping_tokens_estimate // 2

    rprint("\n[bold yellow]DRY RUN - Cost Estimate:[/bold yellow]")
    if total_input_tokens > 0 or total_output_tokens > 0:
        # Calculate cost WITHOUT caching (baseline)
        estimated_cost_no_cache = calculate_cost(total_input_tokens, total_output_tokens)

        # Calculate cost WITH prompt caching
        # Instructions are ~250 tokens and are cached after first request
        instruction_tokens = 250
        if num_views > 0 and CLAUDE_MODEL in MODEL_PRICING:
            # Get pricing for the current model
            pricing = MODEL_PRICING[CLAUDE_MODEL]
            input_price = pricing["input"]
            output_price = pricing["output"]

            # Calculate cache pricing (cache write = base + 25%, cache read = 10% of base)
            cache_write_price = input_price * 1.25  # 25% premium over regular input
            cache_read_price = input_price * 0.01  # 90% discount (10% of regular price)

            # Calculate instruction token costs with caching
            # First request: cache write
            cache_write_cost = instruction_tokens * cache_write_price / 1_000_000

            # Remaining requests: cache reads
            cache_read_cost = instruction_tokens * (num_views - 1) * cache_read_price / 1_000_000

            # Data tokens (non-instruction) still cost regular input price
            # total_input_tokens includes instruction tokens, so subtract them to get data tokens
            data_tokens = total_input_tokens - (instruction_tokens * num_views)
            regular_input_cost = data_tokens * input_price / 1_000_000

            # Output tokens always cost regular output price
            output_cost = total_output_tokens * output_price / 1_000_000

            estimated_cost_with_cache = cache_write_cost + cache_read_cost + regular_input_cost + output_cost
        else:
            estimated_cost_with_cache = estimated_cost_no_cache

        # Add variance
        lower_cost = estimated_cost_with_cache * 0.9
        upper_cost = estimated_cost_with_cache * 1.5

        rprint(f"  Total views to check: {num_views:,}")
        rprint(f"  Estimated input tokens:  {total_input_tokens:,}")
        rprint(f"  Estimated output tokens: {total_output_tokens:,}")
        if grouping_tokens_estimate > 0:
            rprint(f"  [dim](Includes ~{grouping_tokens_estimate} tokens for grouping and pruning)[/dim]")

        rprint("\n  [bold]WITH prompt caching (69% of prompt is cached):[/bold]")
        rprint(f"  [bold green]Estimated cost: ${lower_cost:.4f} - ${upper_cost:.4f}[/bold green]")
        rprint(f"  [dim](Most likely: ${estimated_cost_with_cache:.4f})[/dim]")

        savings = estimated_cost_no_cache - estimated_cost_with_cache
        if savings > 0:
            savings_percent = (savings / estimated_cost_no_cache * 100) if estimated_cost_no_cache > 0 else 0
            rprint(f"\n  [dim]Savings vs no caching: ~${savings:.4f} ({savings_percent:.0f}% reduction)[/dim]")
            rprint(f"  [dim](Without caching would cost: ${estimated_cost_no_cache:.4f})[/dim]")
    else:
        rprint("  No API calls needed for selected checks.")


def display_results(
    grouped_issues: list[dict[str, Any]],
    views: list[dict[str, Any]],
    total_input_tokens: int,
    total_output_tokens: int,
    grouping_tokens: int,
) -> None:
    """Display grouped issues and cost."""
    # Extract unique explorer slugs and identify mdims
    # Charts use 'slug', explorers use 'explorerSlug'
    all_slugs = []
    for view in views:
        if view.get("view_type") == "chart":
            slug = view.get("slug")
        else:
            slug = view.get("explorerSlug")
        if slug:
            all_slugs.append(slug)
    all_explorers_analyzed = list(set(all_slugs))

    mdim_slugs_list = [view.get("explorerSlug") for view in views if view.get("view_type") == "multidim"]
    mdim_slugs = set(s for s in mdim_slugs_list if s is not None)

    # Display grouped issues
    display_issues(grouped_issues, all_explorers_analyzed, mdim_slugs)

    # Display API usage and cost
    if total_input_tokens > 0 or total_output_tokens > 0 or grouping_tokens > 0:
        # Calculate cost for issue detection (uses CLAUDE_MODEL)
        detection_cost = calculate_cost(total_input_tokens, total_output_tokens)

        # Calculate cost for grouping/pruning (uses GROUPING_MODEL)
        grouping_cost = 0.0
        if grouping_tokens > 0:
            # Assume roughly 50/50 split between input and output tokens
            grouping_input = grouping_tokens // 2
            grouping_output = grouping_tokens - grouping_input
            grouping_pricing = MODEL_PRICING.get(GROUPING_MODEL, MODEL_PRICING[CLAUDE_MODEL])
            grouping_cost = (grouping_input / 1_000_000) * grouping_pricing["input"] + (
                grouping_output / 1_000_000
            ) * grouping_pricing["output"]

        total_cost = detection_cost + grouping_cost

        rprint("\n[bold cyan]API Usage:[/bold cyan]")
        rprint(f"  Detection ({CLAUDE_MODEL}):")
        rprint(f"    Input tokens:  {total_input_tokens:,}")
        rprint(f"    Output tokens: {total_output_tokens:,}")
        rprint(f"    Cost: ${detection_cost:.4f}")

        if grouping_tokens > 0:
            rprint(f"  Grouping ({GROUPING_MODEL}):")
            rprint(f"    Total tokens: {grouping_tokens:,}")
            rprint(f"    Cost: ${grouping_cost:.4f}")

        rprint(f"  [bold]Total cost: ${total_cost:.4f}[/bold]")


def get_completed_collections(output_file: str) -> set[str]:
    """Read CSV file and return set of collection slugs already processed.

    Args:
        output_file: Path to CSV file with results

    Returns:
        Set of explorer slugs that have been completed
    """
    output_path = Path(output_file)
    if not output_path.exists():
        return set()

    try:
        df = pd.read_csv(output_path)
        if "explorer_slug" in df.columns:
            completed = set(df["explorer_slug"].dropna().unique())
            return completed
        return set()
    except Exception as e:
        log.warning(f"Failed to read checkpoint from {output_file}: {e}")
        return set()


def save_collection_results(output_file: str, issues: list[dict[str, Any]]) -> None:
    """Append collection results to CSV file.

    Args:
        output_file: Path to CSV file
        issues: List of issues to append
    """
    if not issues:
        return

    # Define expected column order for CSV output
    expected_columns = ["explorer_slug", "view_id", "issue_type", "field", "context", "explanation"]

    # Filter issues to only include expected columns
    filtered_issues = []
    for issue in issues:
        filtered_issue = {col: issue.get(col, "") for col in expected_columns}
        filtered_issues.append(filtered_issue)

    df = pd.DataFrame(filtered_issues, columns=expected_columns)
    output_path = Path(output_file)

    # Append to existing file or create new one
    if output_path.exists():
        df.to_csv(output_path, mode="a", header=False, index=False)
    else:
        df.to_csv(output_path, index=False)


@click.command(cls=RichCommand)
@click.option(
    "--slug",
    multiple=True,
    help="Filter by specific explorer or multidim slug. Can be specified multiple times (e.g., '--slug global-food --slug covid-boosters')",
)
@click.option(
    "--chart-slug",
    multiple=True,
    help="Filter by specific chart slug. Can be specified multiple times (e.g., '--chart-slug co2-emissions-per-capita')",
)
@click.option(
    "--skip-typos",
    is_flag=True,
    help="Skip typo checking (codespell)",
)
@click.option(
    "--skip-issues",
    is_flag=True,
    help="Skip semantic issue checking (Claude API)",
)
@click.option(
    "--skip-grouping",
    is_flag=True,
    help="Skip grouping and pruning of similar issues",
)
@click.option(
    "--output-file",
    type=click.Path(),
    help="Save issues to CSV file",
)
@click.option(
    "--batch-size",
    type=int,
    default=DEFAULT_BATCH_SIZE,
    help=f"Number of views to check per API call (default: {DEFAULT_BATCH_SIZE}, safe range: 30-100)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Limit number of views to analyze (useful for testing to reduce API costs)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Estimate API costs without making actual API calls",
)
def run(
    slug: tuple[str, ...],
    chart_slug: tuple[str, ...],
    skip_typos: bool,
    skip_issues: bool,
    skip_grouping: bool,
    output_file: str | None,
    batch_size: int,
    limit: int | None,
    dry_run: bool,
) -> None:
    """Check explorer, multidim views, and chart configs for typos and semantic issues.

    Examples:
        python metadata_inspector.py --skip-issues
        python metadata_inspector.py --slug global-food
        python metadata_inspector.py --chart-slug co2-emissions-per-capita
        python metadata_inspector.py --slug global-food --slug covid-boosters
        python metadata_inspector.py --slug animal-welfare --limit 5
        python metadata_inspector.py --output-file issues.csv
    """
    # Validate prerequisites
    if not skip_typos and not get_codespell_path():
        rprint("[yellow]Warning: codespell not found. Install with: uv add codespell[/yellow]")
        skip_typos = True

    if not config.ANTHROPIC_API_KEY and not skip_issues:
        rprint("[red]Error: ANTHROPIC_API_KEY not found. Add to .env file or use --skip-issues[/red]")
        raise click.ClickException("Missing ANTHROPIC_API_KEY")

    # Load views (explorers and multidims)
    slug_list = list(slug) if slug else None
    chart_slug_list = list(chart_slug) if chart_slug else None

    # Load both explorer views and chart configs
    views = load_views(slug_list, chart_slug_list, limit)
    if not views:
        return

    # Auto-resume: filter out completed collections if output file exists
    completed_collections = set()
    if output_file and Path(output_file).exists():
        completed_collections = get_completed_collections(output_file)
        if completed_collections:
            original_count = len(views)
            # Filter out completed collections - charts use 'slug', explorers use 'explorerSlug'
            views = [
                v
                for v in views
                if (v.get("slug") if v.get("view_type") == "chart" else v.get("explorerSlug"))
                not in completed_collections
            ]
            skipped_count = original_count - len(views)
            if skipped_count > 0:
                rprint(
                    f"[yellow]Resuming from checkpoint: skipping {len(completed_collections)} "
                    f"already-processed collection(s) ({skipped_count} views)[/yellow]"
                )
            if not views:
                rprint("[green]✓ All collections already processed![/green]")
                return

    # Dry-run: estimate costs and exit
    if dry_run:
        total_input_tokens, total_output_tokens = estimate_check_costs(views, skip_typos, skip_issues, batch_size)
        display_cost_estimate(total_input_tokens, total_output_tokens, skip_issues, len(views))
        return

    # Group views by collection (explorerSlug for explorers, slug for charts) for incremental processing
    collections: dict[str, list[dict[str, Any]]] = {}
    for view in views:
        # Charts use 'slug', explorers use 'explorerSlug'
        collection_slug = view.get("slug") if view.get("view_type") == "chart" else view.get("explorerSlug")
        if collection_slug and collection_slug not in collections:
            collections[collection_slug] = []
        if collection_slug:
            collections[collection_slug].append(view)

    # Process collections one by one, saving after each
    all_issues = []
    total_input_tokens = 0
    total_output_tokens = 0
    total_cache_creation_tokens = 0
    total_cache_read_tokens = 0
    total_cost = 0.0

    rprint(f"\n[bold cyan]Processing {len(collections)} collection(s)...[/bold cyan]\n")

    for collection_num, (collection_slug, collection_views) in enumerate(collections.items(), 1):
        rprint(f"[bold]Collection {collection_num}/{len(collections)}: {collection_slug}[/bold]")

        # Run checks for this collection
        collection_issues, input_tokens, output_tokens, cache_creation, cache_read = run_checks(
            collection_views, skip_typos, skip_issues, batch_size
        )

        # Calculate cost for this collection
        collection_cost = calculate_cost(input_tokens, output_tokens, cache_creation, cache_read)
        total_cost += collection_cost

        total_input_tokens += input_tokens
        total_output_tokens += output_tokens
        total_cache_creation_tokens += cache_creation
        total_cache_read_tokens += cache_read

        # Save this collection's results immediately (even if no issues, for checkpoint tracking)
        if output_file:
            if collection_issues:
                save_collection_results(output_file, collection_issues)
                rprint(f"  [green]✓ Saved {len(collection_issues)} issue(s) to {output_file}[/green]")
            else:
                # Save a marker row to track that this collection was processed
                marker = {
                    "explorer_slug": collection_slug,
                    "view_id": None,
                    "issue_type": "checkpoint",
                    "field": "completed",
                    "context": "",
                    "explanation": "Collection processed with no issues found",
                }
                save_collection_results(output_file, [marker])
                rprint("  [green]✓ Checkpoint saved (no issues found)[/green]")

        all_issues.extend(collection_issues)

        # Display cost for this collection
        rprint(f"  [dim]Cost: ${collection_cost:.4f} (running total: ${total_cost:.4f})[/dim]")
        rprint()

    # Group and prune issues (across all collections)
    grouping_tokens = 0
    if all_issues and not skip_grouping:
        rprint("  [cyan]Grouping similar issues and pruning false positives...[/cyan]")
        grouped_issues, grouping_tokens = group_issues(all_issues, config.ANTHROPIC_API_KEY)
        # Note: grouping tokens are tracked separately since they use a different model
    else:
        grouped_issues = all_issues

    # Display results (grouping tokens tracked separately for cost calculation)
    display_results(grouped_issues, views, total_input_tokens, total_output_tokens, grouping_tokens)


if __name__ == "__main__":
    run()
