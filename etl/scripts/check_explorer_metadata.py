"""Script to detect typos, semantic inconsistencies, and quality issues in explorer views.

This script analyzes explorer views from the database and detects:
1. Typos using codespell (if available)
2. Semantic inconsistencies (e.g., "bees slaughtered for meat")
3. Poorly written sentences

The script can use Claude API for semantic validation when an API key is provided.
"""

import json
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
from structlog import get_logger

from etl import config
from etl.db import read_sql
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

CLAUDE_MODEL = "claude-3-5-haiku-20241022"  # Fast: $1/M in, $5/M out (RECOMMENDED)
# CLAUDE_MODEL = "claude-3-7-sonnet-20250219"  # Balanced: $3/M in, $15/M out
# CLAUDE_MODEL = "claude-opus-4-20250514"  # Quality: $15/M in, $75/M out

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
DEFAULT_BATCH_SIZE = 50

# Model pricing (USD per million tokens)
# Source: https://www.anthropic.com/api-pricing (verified 2025-10-28)
# Note: We use the regular API, not Batch API (which offers 50% discount but is asynchronous)
MODEL_PRICING = {
    "claude-3-5-haiku-20241022": {"input": 1.0, "output": 5.0},
    "claude-3-7-sonnet-20250219": {"input": 3.0, "output": 15.0},
    "claude-opus-4-20250514": {"input": 15.0, "output": 75.0},
}


def parse_dimensions(dimensions_raw: Any) -> dict[str, Any]:
    """Parse dimensions field which can be JSON (explorers) or just a viewId (mdims).

    Args:
        dimensions_raw: Raw dimensions value from database

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
    # Mdim: dimensions is just a viewId (string or number) - not relevant for our checks
    return {}


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


def build_explorer_url(explorer_slug: str, dimensions: dict[str, Any]) -> str:
    """Build URL for explorer view with dimensions.

    Args:
        explorer_slug: Explorer slug (e.g., 'air-pollution')
        dimensions: Dictionary of dimension key-value pairs

    Returns:
        Full URL to the explorer view with properly encoded query parameters
    """
    from urllib.parse import urlencode

    base_url = config.OWID_ENV.site or "https://ourworldindata.org"
    url = f"{base_url}/explorers/{explorer_slug}"

    if dimensions:
        # Filter out empty values and build query string with proper encoding
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


def run_codespell_batch(views: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    """Run codespell on all views at once for performance.

    Args:
        views: List of view dictionaries

    Returns:
        Dictionary mapping view_id to list of typo issues
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
                chart_config = json.loads(view["chart_config"]) if view["chart_config"] else {}
                dimensions = parse_dimensions(view["dimensions"])
                view_id = view["id"]
                view_files[view_id] = []

                # Collect texts to check with field names
                texts_to_check = [
                    ("title", chart_config.get("title", "")),
                    ("subtitle", chart_config.get("subtitle", "")),
                    ("note", chart_config.get("note", "")),
                ]

                # Add variable metadata fields
                variable_fields = [
                    ("variable_name", view.get("variable_name", [])),
                    ("variable_description", view.get("variable_description", [])),
                    ("variable_title_public", view.get("variable_title_public", [])),
                    ("variable_description_short", view.get("variable_description_short", [])),
                    ("variable_description_from_producer", view.get("variable_description_from_producer", [])),
                    ("variable_description_key", view.get("variable_description_key", [])),
                    ("variable_description_processing", view.get("variable_description_processing", [])),
                ]

                for field_name, values in variable_fields:
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
        issues_by_view: dict[int, list[dict[str, Any]]] = {}
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
                    # Extract everything between "view_" and "_{field_name}.txt"
                    filename_without_ext = filename.replace(".txt", "")
                    if not filename_without_ext.startswith("view_"):
                        progress.update(task, advance=1)
                        continue

                    # Remove "view_" prefix
                    rest = filename_without_ext[5:]  # len("view_") = 5

                    # The view_id is everything up to the last underscore (which separates field name)
                    # But we need to find which view_id matches from our view_files dict
                    view_id = None
                    field_name = None
                    for vid in view_files.keys():
                        vid_str = str(vid)
                        if rest.startswith(vid_str + "_"):
                            view_id = vid
                            field_name = rest[len(vid_str) + 1 :]  # Everything after view_id and underscore
                            break

                    if view_id is None:
                        progress.update(task, advance=1)
                        continue

                    # Find the original text and get the correct field name
                    text = ""
                    for fname, fpath, ftext in view_files.get(view_id, []):
                        if fpath == Path(file_path):
                            text = ftext
                            # Use the original field name from fname, removing numeric suffix if present
                            # fname could be like "variable_description_from_producer_0"
                            if fname.rsplit("_", 1)[-1].isdigit():
                                # Has numeric suffix, remove it
                                field_name = fname.rsplit("_", 1)[0]
                            else:
                                field_name = fname
                            break

                    # Get context from the specific line where the typo was found
                    text_lines = text.split("\n")
                    if 1 <= line_num_int <= len(text_lines):
                        typo_line = text_lines[line_num_int - 1]
                        context = get_text_context(typo_line, typo)
                    else:
                        # Fallback to searching entire text if line number is out of range
                        context = get_text_context(text, typo)

                    # Get view details
                    view = next((v for v in views if v["id"] == view_id), None)
                    if not view:
                        progress.update(task, advance=1)
                        continue

                    chart_config = json.loads(view["chart_config"]) if view["chart_config"] else {}
                    dimensions = parse_dimensions(view["dimensions"])
                    view_title = chart_config.get("title", "")
                    view_url = build_explorer_url(view["explorerSlug"], dimensions)

                    # Create issue
                    issue = {
                        "view_id": view_id,
                        "explorer_slug": view["explorerSlug"],
                        "view_title": view_title,
                        "view_url": view_url,
                        "issue_type": "typo",
                        "severity": "warning",
                        "field": field_name,
                        "text": text[:100],
                        "context": context,
                        "typo": typo,
                        "correction": correction,
                        "explanation": f"Typo in {field_name}: '{typo}' → '{correction}'",
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

    query = f"""
        SELECT
            ev.id,
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
    log.info(f"Fetched {len(df)} explorer view records")

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
        df.groupby(["id", "explorerSlug", "dimensions", "chartConfigId", "chart_config"], dropna=False)
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

    query = f"""
        SELECT
            CONCAT('mdim_', md.id, '_', mx.viewId) as id,
            md.slug as explorerSlug,
            mx.viewId as dimensions,
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
        df.groupby(["id", "explorerSlug", "dimensions", "chartConfigId", "chart_config"], dropna=False)
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


def calculate_cost(input_tokens: int, output_tokens: int) -> float:
    """Calculate cost in USD for given token counts.

    Args:
        input_tokens: Number of input tokens
        output_tokens: Number of output tokens

    Returns:
        Cost in USD
    """
    if CLAUDE_MODEL not in MODEL_PRICING:
        raise ValueError(f"Unknown model '{CLAUDE_MODEL}'. Please add pricing to MODEL_PRICING dictionary.")
    pricing = MODEL_PRICING[CLAUDE_MODEL]
    input_cost = (input_tokens / 1_000_000) * pricing["input"]
    output_cost = (output_tokens / 1_000_000) * pricing["output"]
    return input_cost + output_cost


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


def call_claude_api_with_retry(
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


def check_semantic_issues_batch(
    views: list[dict[str, Any]], api_key: str | None, batch_size: int = 10, dry_run: bool = False
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Check for semantic inconsistencies using Claude API in batches.

    Args:
        views: List of view dictionaries
        api_key: Anthropic API key
        batch_size: Number of views to check in each API call

    Returns:
        Tuple of (list of semantic issues found, usage stats dict)
    """
    if not api_key:
        log.warning("No Claude API key provided, skipping semantic checks")
        return [], {}

    client = anthropic.Anthropic(api_key=api_key)
    all_issues = []
    total_input_tokens = 0
    total_output_tokens = 0

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Checking semantic issues", total=len(views))

        for i in range(0, len(views), batch_size):
            batch = views[i : i + batch_size]

            # Prepare batch context
            batch_context = []
            for view in batch:
                chart_config = json.loads(view["chart_config"]) if view["chart_config"] else {}
                dimensions = parse_dimensions(view["dimensions"])

                # Build comprehensive variable metadata
                variables = []
                for i, var_name in enumerate(view.get("variable_name", [])):
                    var_info = {"name": var_name}
                    # Add other metadata if available at the same index
                    if i < len(view.get("variable_title_public", [])):
                        var_info["title_public"] = view["variable_title_public"][i]
                    if i < len(view.get("variable_unit", [])):
                        var_info["unit"] = view["variable_unit"][i]
                    if i < len(view.get("variable_description_short", [])):
                        var_info["description_short"] = view["variable_description_short"][i]
                    variables.append(var_info)

                context = {
                    "view_id": view["id"],
                    "explorer_slug": view["explorerSlug"],
                    "dimensions": dimensions,
                    "title": chart_config.get("title", ""),
                    "subtitle": chart_config.get("subtitle", ""),
                    "variables": variables,
                }
                batch_context.append(context)

            # Call Claude API
            prompt = f"""Analyze these {len(batch)} explorer view configurations for semantic inconsistencies and absurdities.

For each view, check if the title, subtitle, and dimensions make semantic sense together.

Flag issues like:
- Absurd combinations (e.g., "bees slaughtered for meat")
- Misleading titles that don't match the dimensions
- Contradictory information
- Illogical metric-entity combinations

For each issue found, respond with a JSON object:
{{
    "view_id": <int>,
    "severity": "critical|warning|info",
    "explanation": "<clear description of the issue>"
}}

If no issues found in a view, don't include it in the response.

Views to analyze:
{json.dumps(batch_context, indent=2)}

Respond ONLY with a JSON array of issues, or an empty array [] if no issues found."""

            # If dry run, estimate tokens and skip API call
            if dry_run:
                estimated_input = estimate_tokens(prompt)
                # Estimate output: typically 30-60 tokens per issue, assume ~0.3 issues per view
                estimated_output = len(batch) * 20
                total_input_tokens += estimated_input
                total_output_tokens += estimated_output
                continue

            content = ""  # Initialize to avoid unbound variable
            try:
                response = call_claude_api_with_retry(
                    client=client,
                    model=CLAUDE_MODEL,
                    max_tokens=4096,
                    prompt=prompt,
                )

                # Parse response
                content_block = response.content[0]
                if not isinstance(content_block, TextBlock):
                    log.error(f"Unexpected content block type: {type(content_block)}")
                    continue
                content = content_block.text.strip()

                # Extract JSON from response (handle markdown code blocks and extra text)
                if "```json" in content:
                    content = content.split("```json")[1].split("```")[0].strip()
                elif "```" in content:
                    content = content.split("```")[1].split("```")[0].strip()
                else:
                    # Try to find JSON array in the content
                    # Look for the first '[' and last ']'
                    start = content.find("[")
                    end = content.rfind("]")
                    if start != -1 and end != -1 and start < end:
                        content = content[start : end + 1]

                # Try to parse JSON, with fallback for common issues
                try:
                    batch_issues = json.loads(content)
                except json.JSONDecodeError:
                    # Try to fix common JSON issues - replace literal newlines in strings
                    import re

                    content_fixed = re.sub(r'"\s*\n\s*', '" ', content)
                    batch_issues = json.loads(content_fixed)

                # Track token usage
                total_input_tokens += response.usage.input_tokens
                total_output_tokens += response.usage.output_tokens

                # Enrich issues with view metadata
                for issue in batch_issues:
                    view_id = issue["view_id"]
                    view = next((v for v in batch if v["id"] == view_id), None)
                    if view:
                        issue["issue_type"] = "semantic"
                        issue["explorer_slug"] = view["explorerSlug"]
                        dimensions = parse_dimensions(view["dimensions"])
                        chart_config = json.loads(view["chart_config"]) if view["chart_config"] else {}
                        issue["dimensions"] = dimensions
                        issue["title"] = chart_config.get("title", "")
                        # Add view title and URL for display
                        view_title = chart_config.get("title", "")
                        view_url = build_explorer_url(view["explorerSlug"], dimensions)
                        issue["view_title"] = view_title
                        issue["view_url"] = view_url
                        all_issues.append(issue)

            except json.JSONDecodeError as e:
                log.error(f"JSON parsing error for batch: {e}")
                if content:
                    log.error(f"Content that failed to parse: {content[:500]}...")
                continue
            except Exception as e:
                log.error(f"Error calling Claude API for batch: {e}")
                continue
            finally:
                # Update progress after processing batch
                progress.update(task, advance=len(batch))

    usage_stats = {
        "input_tokens": total_input_tokens,
        "output_tokens": total_output_tokens,
    }
    return all_issues, usage_stats


def check_writing_quality_batch(
    views: list[dict[str, Any]], api_key: str | None, batch_size: int = 10, dry_run: bool = False
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Check for writing quality issues using Claude API in batches.

    Args:
        views: List of view dictionaries
        api_key: Anthropic API key
        batch_size: Number of views to check in each API call

    Returns:
        Tuple of (list of writing quality issues found, usage stats dict)
    """
    if not api_key:
        return [], {}

    client = anthropic.Anthropic(api_key=api_key)
    all_issues = []
    total_input_tokens = 0
    total_output_tokens = 0

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Checking writing quality", total=len(views))

        for i in range(0, len(views), batch_size):
            batch = views[i : i + batch_size]

            # Prepare batch context
            batch_context = []
            for view in batch:
                chart_config = json.loads(view["chart_config"]) if view["chart_config"] else {}

                context = {
                    "view_id": view["id"],
                    "title": chart_config.get("title", ""),
                    "subtitle": chart_config.get("subtitle", ""),
                    "note": chart_config.get("note", ""),
                }
                # Skip if all fields are empty
                if not any([context["title"], context["subtitle"], context["note"]]):
                    continue
                batch_context.append(context)

            if not batch_context:
                continue

            # Call Claude API
            prompt = f"""Review these {len(batch_context)} chart titles, subtitles, and notes for writing quality.

Flag issues like:
- Grammatical errors
- Unclear or confusing phrasing
- Overly complex sentences
- Missing punctuation or capitalization issues
- Inconsistent tone
- Passive voice where active would be clearer

Only flag significant issues that affect clarity or professionalism. Don't flag stylistic preferences.

For each issue found, respond with a JSON object:
{{
    "view_id": <int>,
    "field": "title|subtitle|note",
    "text": "<the problematic text>",
    "explanation": "<description of the writing issue>",
    "suggestion": "<optional: suggested improvement>"
}}

IMPORTANT: Ensure valid JSON formatting:
- Use proper escaping for quotes within strings
- Keep all content on single lines (no literal line breaks in strings)
- Avoid using 'or' in suggestion fields - pick one option

If no significant issues found, respond with an empty array.

Texts to review:
{json.dumps(batch_context, indent=2)}

Respond ONLY with a JSON array of issues, or an empty array [] if no issues found."""

            # If dry run, estimate tokens and skip API call
            if dry_run:
                estimated_input = estimate_tokens(prompt)
                # Estimate output: typically 40-80 tokens per issue, assume ~0.2 issues per view
                estimated_output = len(batch) * 15
                total_input_tokens += estimated_input
                total_output_tokens += estimated_output
                continue

            content = ""  # Initialize to avoid unbound variable
            try:
                response = call_claude_api_with_retry(
                    client=client,
                    model=CLAUDE_MODEL,
                    max_tokens=4096,
                    prompt=prompt,
                )

                # Parse response
                content_block = response.content[0]
                if not isinstance(content_block, TextBlock):
                    log.error(f"Unexpected content block type: {type(content_block)}")
                    continue
                content = content_block.text.strip()

                # Extract JSON from response (handle markdown code blocks and extra text)
                if "```json" in content:
                    content = content.split("```json")[1].split("```")[0].strip()
                elif "```" in content:
                    content = content.split("```")[1].split("```")[0].strip()
                else:
                    # Try to find JSON array in the content
                    # Look for the first '[' and last ']'
                    start = content.find("[")
                    end = content.rfind("]")
                    if start != -1 and end != -1 and start < end:
                        content = content[start : end + 1]

                # Try to parse JSON, with fallback for common issues
                try:
                    batch_issues = json.loads(content)
                except json.JSONDecodeError:
                    # Try to fix common JSON issues - replace literal newlines in strings
                    import re

                    content_fixed = re.sub(r'"\s*\n\s*', '" ', content)
                    batch_issues = json.loads(content_fixed)

                # Track token usage
                total_input_tokens += response.usage.input_tokens
                total_output_tokens += response.usage.output_tokens

                # Enrich issues with view metadata
                for issue in batch_issues:
                    view_id = issue["view_id"]
                    view = next((v for v in batch if v["id"] == view_id), None)
                    if view:
                        issue["issue_type"] = "writing_quality"
                        issue["severity"] = "info"
                        issue["explorer_slug"] = view["explorerSlug"]
                        # Add view title and URL for display
                        dimensions = parse_dimensions(view["dimensions"])
                        chart_config = json.loads(view["chart_config"]) if view["chart_config"] else {}
                        view_title = chart_config.get("title", "")
                        view_url = build_explorer_url(view["explorerSlug"], dimensions)
                        issue["view_title"] = view_title
                        issue["view_url"] = view_url
                        all_issues.append(issue)

            except json.JSONDecodeError as e:
                log.error(f"JSON parsing error for batch: {e}")
                if content:
                    log.error(f"Content that failed to parse: {content[:500]}...")
                continue
            except Exception as e:
                log.error(f"Error calling Claude API for batch: {e}")
                continue
            finally:
                # Update progress after processing batch
                progress.update(task, advance=len(batch))

    usage_stats = {
        "input_tokens": total_input_tokens,
        "output_tokens": total_output_tokens,
    }
    return all_issues, usage_stats


def group_issues_with_claude(
    issues: list[dict[str, Any]], api_key: str | None, dry_run: bool = False
) -> tuple[list[dict[str, Any]], int]:
    """Use Claude to intelligently group similar issues.

    Args:
        issues: List of issue dictionaries
        api_key: Anthropic API key
        dry_run: If True, skip API call and return ungrouped issues

    Returns:
        Tuple of (grouped issues, tokens used for grouping)
    """
    if not api_key or dry_run or not issues:
        return issues, 0

    # Separate typos from semantic/quality issues
    typo_issues = [i for i in issues if i.get("issue_type") == "typo"]
    other_issues = [i for i in issues if i.get("issue_type") != "typo"]

    # Group typos using simple string matching (fast and accurate)
    grouped_typos = group_typos_simple(typo_issues)

    # If no semantic/quality issues, return just the typos
    if not other_issues:
        return grouped_typos, 0

    # Use Claude to group semantic/quality issues
    rprint("  [cyan]Grouping similar issues intelligently...[/cyan]")

    # Prepare simplified issue list for Claude
    simplified_issues = []
    for idx, issue in enumerate(other_issues):
        simplified_issues.append(
            {
                "index": idx,
                "type": issue.get("issue_type"),
                "explanation": issue.get("explanation", "")[:200],  # Limit length
            }
        )

    prompt = f"""Group these {len(simplified_issues)} issues by semantic similarity.
Issues that describe essentially the same problem should be in the same group,
even if wording differs or they apply to different cases.

For example:
- "Spelling of 'sulfur' vs 'sulphur'" issues should be grouped together
- "Subtitle mentions X but title is about Y" issues with same pattern should be grouped
- Issues about same inconsistency applying to different sectors should be grouped

Issues:
{json.dumps(simplified_issues, indent=2)}

Respond ONLY with a JSON object mapping group IDs to lists of issue indices:
{{"group_0": [0, 3, 7], "group_1": [1, 2], "group_2": [4, 5, 6], ...}}

Issues in the same group will be displayed together with a count."""

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = call_claude_api_with_retry(
            client=client,
            model=CLAUDE_MODEL,
            max_tokens=2048,
            prompt=prompt,
        )

        # Parse response
        content_block = response.content[0]
        if not isinstance(content_block, TextBlock):
            log.warning("Unexpected content block type in grouping, using fallback")
            return grouped_typos + other_issues, 0

        content = content_block.text.strip()

        # Extract JSON
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        else:
            start = content.find("{")
            end = content.rfind("}")
            if start != -1 and end != -1:
                content = content[start : end + 1]

        grouping = json.loads(content)
        tokens_used = response.usage.input_tokens + response.usage.output_tokens

        # Apply grouping
        grouped_other = []
        for group_indices in grouping.values():
            if not group_indices:
                continue
            # Take first issue as representative
            representative = other_issues[group_indices[0]].copy()
            representative["group_count"] = len(group_indices)
            representative["group_views"] = [
                other_issues[i].get("view_title", "") for i in group_indices if i < len(other_issues)
            ]
            grouped_other.append(representative)

        return grouped_typos + grouped_other, tokens_used

    except Exception as e:
        log.warning(f"Error grouping with Claude: {e}, using fallback grouping")
        return grouped_typos + other_issues, 0


def group_typos_simple(typo_issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
        representative["group_count"] = len(group)
        representative["group_views"] = [issue.get("view_title", "") for issue in group]
        grouped_issues.append(representative)

    return grouped_issues


def display_issues(
    issues: list[dict[str, Any]],
    output_format: str = "table",
    api_key: str | None = None,
    dry_run: bool = False,
    all_explorers: list[str] | None = None,
    mdim_slugs: set[str] | None = None,
) -> int:
    """Display issues in specified format.

    Args:
        issues: List of issue dictionaries
        output_format: Output format (table, json, or csv)
        api_key: Anthropic API key for intelligent grouping
        dry_run: If True, skip Claude API calls for grouping
        all_explorers: List of all explorer slugs that were analyzed (for reporting clean explorers)
        mdim_slugs: Set of slugs that are multidimensional indicators (not explorers)

    Returns:
        Number of tokens used for grouping (0 for non-table formats)
    """
    if not issues:
        rprint("[green]✓ No issues found![/green]")
        return 0

    if output_format == "json":
        print(json.dumps(issues, indent=2))
        return 0

    if output_format == "csv":
        df = pd.DataFrame(issues)
        print(df.to_csv(index=False))
        return 0

    # Group similar issues using Claude
    grouped_issues, grouping_tokens = group_issues_with_claude(issues, api_key, dry_run)

    # Group issues by explorer
    from collections import defaultdict

    issues_by_explorer = defaultdict(list)
    for issue in grouped_issues:
        explorer_slug = issue.get("explorer_slug", "unknown")
        issues_by_explorer[explorer_slug].append(issue)

    # Display summary
    total_original = len(issues)
    total_grouped = len(grouped_issues)
    num_collections = len(issues_by_explorer)
    rprint(
        f"\n[bold]Found {total_original} total issues ({total_grouped} unique) across {num_collections} collection(s):[/bold]"
    )

    # Count by severity across all explorers
    all_critical = [i for i in grouped_issues if i.get("severity") == "critical"]
    all_warnings = [i for i in grouped_issues if i.get("severity") == "warning"]
    all_info = [i for i in grouped_issues if i.get("severity") == "info"]
    if all_critical:
        rprint(f"  [red]• {len(all_critical)} critical issues[/red]")
    if all_warnings:
        rprint(f"  [yellow]• {len(all_warnings)} warnings[/yellow]")
    if all_info:
        rprint(f"  [blue]• {len(all_info)} info[/blue]")

    # Display issues grouped by explorer
    for explorer_slug in sorted(issues_by_explorer.keys()):
        explorer_issues = issues_by_explorer[explorer_slug]

        # Group by severity for this explorer
        critical = [i for i in explorer_issues if i.get("severity") == "critical"]
        warnings = [i for i in explorer_issues if i.get("severity") == "warning"]
        info = [i for i in explorer_issues if i.get("severity") == "info"]

        # Display header with appropriate label and color
        is_mdim = mdim_slugs and explorer_slug in mdim_slugs
        label = "Multidim" if is_mdim else "Explorer"
        color = "magenta" if is_mdim else "cyan"

        rprint(f"\n[bold {color}]{'=' * 80}[/bold {color}]")
        rprint(f"[bold {color}]{label}: {explorer_slug}[/bold {color}]")
        rprint(f"[bold {color}]{'=' * 80}[/bold {color}]")

        # Display each category for this explorer
        for category, issues_list, color in [
            ("CRITICAL ISSUES", critical, "red"),
            ("WARNINGS", warnings, "yellow"),
            ("INFO", info, "blue"),
        ]:
            if not issues_list:
                continue

            rprint(f"\n[bold {color}]{category}:[/bold {color}]")

            # Print each issue with full details
            for i, issue in enumerate(issues_list, 1):
                issue_type = issue.get("issue_type", "unknown")
                view_title = issue.get("view_title", "Untitled")
                view_url = issue.get("view_url", "")
                field = issue.get("field", "unknown")
                context = issue.get("context", issue.get("text", ""))
                group_count = issue.get("group_count", 1)

                # Print issue header with embedded clickable link
                if view_url:
                    # Embed link in title for clickable terminal support
                    rprint(f"\n[bold]{i}. [link={view_url}]{view_title}[/link][/bold]")
                    # Also show the URL for copy-paste
                    # rprint(f"   [dim]{view_url}[/dim]")
                else:
                    rprint(f"\n[bold]{i}. {view_title}[/bold]")

                # Show group count if more than 1
                if group_count > 1:
                    rprint(f"   [dim]({group_count} similar occurrences in this explorer)[/dim]")

                # Print issue details
                if issue_type == "typo":
                    typo = issue.get("typo", "")
                    correction = issue.get("correction", "")
                    rprint(f"   [yellow]Field:[/yellow] {field}")
                    rprint(f"   [yellow]Typo:[/yellow] '{typo}' → '{correction}'")
                    rprint(f"   [yellow]Context:[/yellow] {context}")
                else:
                    explanation = issue.get("explanation", "")
                    rprint(f"   [yellow]Type:[/yellow] {issue_type}")
                    rprint(f"   [yellow]Issue:[/yellow] {explanation}")
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

    return grouping_tokens


@click.command(cls=RichCommand)
@click.option(
    "--slug",
    multiple=True,
    help="Filter by specific explorer or multidim slug. Can be specified multiple times (e.g., '--slug global-food --slug covid-boosters')",
)
@click.option(
    "--skip-typos",
    is_flag=True,
    help="Skip typo checking (codespell)",
)
@click.option(
    "--skip-semantic",
    is_flag=True,
    help="Skip semantic consistency checking",
)
@click.option(
    "--skip-quality",
    is_flag=True,
    help="Skip writing quality checking",
)
@click.option(
    "--output-format",
    type=click.Choice(["table", "json", "csv"]),
    default="table",
    help="Output format",
)
@click.option(
    "--output-file",
    type=click.Path(),
    help="Save issues to file (format inferred from extension or --output-format)",
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
def main(
    slug: tuple[str, ...],
    skip_typos: bool,
    skip_semantic: bool,
    skip_quality: bool,
    output_format: str,
    output_file: str | None,
    batch_size: int,
    limit: int | None,
    dry_run: bool,
) -> None:
    """Check explorer and multidim views for typos, semantic inconsistencies, and quality issues.

    This script analyzes explorer and multidimensional indicator (mdim) views from the database and detects:
    - Typos using codespell (if available)
    - Semantic inconsistencies using Claude API (requires ANTHROPIC_API_KEY in .env)
    - Writing quality issues using Claude API (requires ANTHROPIC_API_KEY in .env)

    To use semantic/quality checks, add ANTHROPIC_API_KEY to your .env file.

    Examples:
        # Check all explorers and mdims for typos only
        python check_explorer_metadata.py --skip-semantic --skip-quality

        # Check specific explorer with all checks (requires API key in .env)
        python check_explorer_metadata.py --slug global-food

        # Check specific multidim
        python check_explorer_metadata.py --slug covid-boosters

        # Check multiple explorers/mdims
        python check_explorer_metadata.py --slug global-food --slug covid-boosters --slug energy-prices

        # Test with limited views to minimize API costs
        python check_explorer_metadata.py --slug animal-welfare --limit 5

        # Export issues to JSON
        python check_explorer_metadata.py --output-format json --output-file issues.json
    """
    # Check codespell availability
    has_codespell = get_codespell_path() is not None
    if not skip_typos and not has_codespell:
        rprint("[yellow]Warning: codespell not found. Install with: uv add codespell[/yellow]")
        rprint("[yellow]Skipping typo checks...[/yellow]")
        skip_typos = True

    # Check API key for semantic/quality checks
    anthropic_api_key = config.ANTHROPIC_API_KEY
    if not anthropic_api_key and (not skip_semantic or not skip_quality):
        rprint("[red]Error: ANTHROPIC_API_KEY not found in configuration.[/red]")
        rprint("[yellow]Please add ANTHROPIC_API_KEY to your .env file to use semantic and quality checks.[/yellow]")
        rprint("[yellow]Alternatively, use --skip-semantic and --skip-quality flags to skip these checks.[/yellow]")
        raise click.ClickException("Missing ANTHROPIC_API_KEY in .env file")

    # Convert tuple to list (or None if empty)
    slug_list = list(slug) if slug else None

    # Fetch data from both explorers and multidimensional indicators
    df_explorers = fetch_explorer_data(explorer_slugs=slug_list)
    df_mdims = fetch_multidim_data(slug_filters=slug_list)

    # Combine both datasets
    df = pd.concat([df_explorers, df_mdims], ignore_index=True)

    # Check if we got any results
    if df.empty:
        if slug_list:
            slugs_str = ", ".join(slug_list)
            rprint(f"[red]Error: No views found for slug(s) '{slugs_str}' (checked both explorers and multidims)[/red]")
        else:
            rprint("[red]Error: No explorer or multidim views found in database[/red]")
        return

    # Report what was found
    explorer_count = len(df_explorers)
    mdim_count = len(df_mdims)
    if slug_list:
        slugs_str = ", ".join(slug_list)
        rprint(
            f"[cyan]Filtering to slug(s): {slugs_str} ({explorer_count} explorer records, {mdim_count} multidim records)[/cyan]"
        )
    else:
        rprint(f"[cyan]Fetched {explorer_count} explorer records and {mdim_count} multidim records[/cyan]")

    # Aggregate views (works for both explorers and mdims since structure is the same)
    # Note: "records" count is higher than "views" count because:
    # - Explorer records include multiple rows per view (one per variable in chart config)
    # - After aggregation, variables are grouped into lists per view
    agg_df_explorers = aggregate_explorer_views(df_explorers) if not df_explorers.empty else pd.DataFrame()
    agg_df_mdims = aggregate_multidim_views(df_mdims) if not df_mdims.empty else pd.DataFrame()
    agg_df = pd.concat([agg_df_explorers, agg_df_mdims], ignore_index=True)
    views: list[dict[str, Any]] = agg_df.to_dict("records")  # type: ignore

    # Apply limit if specified
    if limit is not None and limit > 0:
        views = views[:limit]
        rprint(f"[yellow]Limiting to first {limit} views (for testing)[/yellow]")

    explorer_view_count = len(agg_df_explorers)
    mdim_view_count = len(agg_df_mdims)
    rprint(
        f"[cyan]Aggregated to {len(views)} unique views "
        f"({explorer_view_count} explorers + {mdim_view_count} multidims)...[/cyan]\n"
    )

    all_issues = []

    # Track API usage for cost calculation
    total_input_tokens = 0
    total_output_tokens = 0

    # Check typos (skip in dry-run mode since codespell has no cost)
    if not skip_typos and not dry_run:
        rprint("[cyan]Checking for typos (running codespell)...[/cyan]")
        issues_by_view = run_codespell_batch(views)
        # Flatten the issues
        for view_issues in issues_by_view.values():
            all_issues.extend(view_issues)
        rprint(f"[green]✓ Found {len([i for i in all_issues if i['issue_type'] == 'typo'])} typo issues[/green]\n")

    # Check semantic issues
    if not skip_semantic:
        if dry_run:
            rprint("[yellow]Estimating semantic check costs (dry run)...[/yellow]")
        else:
            rprint("[bold]Checking for semantic inconsistencies...[/bold]")
        semantic_issues, usage_stats = check_semantic_issues_batch(views, anthropic_api_key, batch_size, dry_run)
        all_issues.extend(semantic_issues)
        total_input_tokens += usage_stats.get("input_tokens", 0)
        total_output_tokens += usage_stats.get("output_tokens", 0)

        if not dry_run:
            rprint(f"[green]✓ Found {len(semantic_issues)} semantic issues[/green]\n")

    # Check writing quality
    if not skip_quality:
        if dry_run:
            rprint("[yellow]Estimating quality check costs (dry run)...[/yellow]")
        else:
            rprint("[bold]Checking writing quality...[/bold]")
        quality_issues, usage_stats = check_writing_quality_batch(views, anthropic_api_key, batch_size, dry_run)
        all_issues.extend(quality_issues)
        total_input_tokens += usage_stats.get("input_tokens", 0)
        total_output_tokens += usage_stats.get("output_tokens", 0)

        if not dry_run:
            rprint(f"[green]✓ Found {len(quality_issues)} quality issues[/green]\n")

    # Display results
    if output_file:
        # Infer format from file extension if not explicitly set
        if output_file.endswith(".json"):
            file_format = "json"
        elif output_file.endswith(".csv"):
            file_format = "csv"
        else:
            file_format = output_format

        # Save to file
        with open(output_file, "w") as f:
            if file_format == "json":
                json.dump(all_issues, f, indent=2)
            elif file_format == "csv":
                df_issues = pd.DataFrame(all_issues)
                df_issues.to_csv(f, index=False)

        rprint(f"[green]✓ Issues saved to {output_file}[/green]")

    # Display results (skip in dry-run mode)
    if dry_run:
        # In dry run, estimate grouping tokens if we have issues
        grouping_tokens_estimate = 0
        if all_issues and not skip_semantic and not skip_quality:
            # Estimate ~500 tokens for grouping call (input + output)
            grouping_tokens_estimate = 500
            total_input_tokens += grouping_tokens_estimate // 2
            total_output_tokens += grouping_tokens_estimate // 2

        # In dry run, show cost estimate instead of issues
        rprint("\n[bold yellow]DRY RUN - Cost Estimate:[/bold yellow]")
        if total_input_tokens > 0 or total_output_tokens > 0:
            estimated_cost = calculate_cost(total_input_tokens, total_output_tokens)

            # Calculate range: conservative lower bound (-10%) and higher upper bound (+50%)
            # Upper bound is intentionally high to avoid unpleasant surprises
            lower_cost = estimated_cost * 0.9
            upper_cost = estimated_cost * 1.5

            rprint(f"  Estimated input tokens:  {total_input_tokens:,}")
            rprint(f"  Estimated output tokens: {total_output_tokens:,}")
            if grouping_tokens_estimate > 0:
                rprint(f"  [dim](Includes ~{grouping_tokens_estimate} tokens for intelligent grouping)[/dim]")
            rprint(f"  [bold]Estimated cost: ${lower_cost:.4f} - ${upper_cost:.4f}[/bold]")
            rprint(f"  [dim](Most likely: ${estimated_cost:.4f})[/dim]")
        else:
            rprint("  No API calls needed for selected checks.")
    else:
        # Extract unique explorer slugs from views for reporting
        all_explorers_analyzed = list(set(view["explorerSlug"] for view in views))

        # Identify which slugs are mdims vs explorers
        # Mdim views have IDs starting with 'mdim_'
        mdim_slugs = set(view["explorerSlug"] for view in views if str(view.get("id", "")).startswith("mdim_"))

        # Normal mode - display issues and get grouping tokens
        grouping_tokens = display_issues(
            all_issues, "table", anthropic_api_key, dry_run, all_explorers_analyzed, mdim_slugs
        )

        # Add grouping tokens to total
        total_input_tokens += grouping_tokens // 2  # Rough split
        total_output_tokens += grouping_tokens // 2

        # Display API usage and cost
        if total_input_tokens > 0 or total_output_tokens > 0:
            total_cost = calculate_cost(total_input_tokens, total_output_tokens)

            rprint("\n[bold cyan]API Usage:[/bold cyan]")
            rprint(f"  Input tokens:  {total_input_tokens:,}")
            rprint(f"  Output tokens: {total_output_tokens:,}")
            if grouping_tokens > 0:
                rprint(f"  [dim](Includes {grouping_tokens} tokens for intelligent grouping)[/dim]")
            rprint(f"  [bold]Total cost: ${total_cost:.4f}[/bold]")


if __name__ == "__main__":
    main()
