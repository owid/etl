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
from pathlib import Path
from typing import Any

import anthropic
import click
import pandas as pd
from anthropic.types import TextBlock
from rich import print as rprint
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich_click.rich_command import RichCommand
from structlog import get_logger

from etl.config import OWID_ENV
from etl.db import read_sql
from etl.paths import BASE_DIR

# Initialize logger
log = get_logger()
console = Console()


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

    base_url = OWID_ENV.site or "https://ourworldindata.org"
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
    # Find the typo in text (case insensitive)
    text_lower = text.lower()
    typo_lower = typo.lower()
    pos = text_lower.find(typo_lower)

    if pos == -1:
        # If not found, return first N chars
        return text[:200] + ("..." if len(text) > 200 else "")

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
        for view in views:
            chart_config = json.loads(view["chart_config"]) if view["chart_config"] else {}
            dimensions = json.loads(view["dimensions"]) if view["dimensions"] else {}
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

        # Run codespell once on the entire directory
        # Check if .codespell-ignore.txt exists
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

        for line in result.stdout.strip().split("\n"):
            if not line or "==>" not in line:
                continue

            # Parse: /tmp/dir/view_123_field.txt:1: typo ==> correction
            parts = line.split("==>")
            if len(parts) != 2:
                continue

            left = parts[0].strip()
            correction = parts[1].strip()

            # Extract file path and typo
            file_parts = left.rsplit(":", 2)
            if len(file_parts) < 3:
                continue

            file_path = file_parts[0]
            typo = file_parts[2].strip()

            # Extract view_id and field from filename
            filename = Path(file_path).name
            if not filename.startswith("view_"):
                continue

            # Parse filename: view_{view_id}_{field_name}.txt
            parts = filename.replace(".txt", "").split("_", 2)
            if len(parts) < 3:
                continue

            try:
                view_id = int(parts[1])
            except ValueError:
                continue

            field_name = parts[2]

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

            # Get context
            context = get_text_context(text, typo)

            # Get view details
            view = next((v for v in views if v["id"] == view_id), None)
            if not view:
                continue

            chart_config = json.loads(view["chart_config"]) if view["chart_config"] else {}
            dimensions = json.loads(view["dimensions"]) if view["dimensions"] else {}
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

        return issues_by_view

    finally:
        # Clean up temp directory
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)


def fetch_explorer_data(explorer_slug: str | None = None) -> pd.DataFrame:
    """Fetch all explorer views with their metadata from the database.

    Args:
        explorer_slug: Optional filter for specific explorer

    Returns:
        DataFrame with columns: id, explorerSlug, dimensions, chartConfigId,
                                chart_config, and multiple variable_* columns
    """
    where_clause = "WHERE ev.error IS NULL"
    if explorer_slug:
        where_clause += f" AND ev.explorerSlug = '{explorer_slug}'"

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
    agg_df = (
        df.groupby(["id", "explorerSlug", "dimensions", "chartConfigId", "chart_config"])
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


def check_semantic_issues_batch(
    views: list[dict[str, Any]], api_key: str | None, batch_size: int = 10
) -> list[dict[str, Any]]:
    """Check for semantic inconsistencies using Claude API in batches.

    Args:
        views: List of view dictionaries
        api_key: Anthropic API key
        batch_size: Number of views to check in each API call

    Returns:
        List of semantic issues found
    """
    if not api_key:
        log.warning("No Claude API key provided, skipping semantic checks")
        return []

    client = anthropic.Anthropic(api_key=api_key)
    all_issues = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(f"Checking semantic issues (batches of {batch_size})...", total=len(views))

        for i in range(0, len(views), batch_size):
            batch = views[i : i + batch_size]
            progress.update(task, advance=len(batch))

            # Prepare batch context
            batch_context = []
            for view in batch:
                chart_config = json.loads(view["chart_config"]) if view["chart_config"] else {}
                dimensions = json.loads(view["dimensions"]) if view["dimensions"] else {}

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

            try:
                response = client.messages.create(
                    model="claude-3-5-sonnet-20241022",
                    max_tokens=4096,
                    messages=[{"role": "user", "content": prompt}],
                )

                # Parse response
                content_block = response.content[0]
                if not isinstance(content_block, TextBlock):
                    log.error(f"Unexpected content block type: {type(content_block)}")
                    continue
                content = content_block.text
                # Extract JSON from response (handle markdown code blocks)
                if "```json" in content:
                    content = content.split("```json")[1].split("```")[0].strip()
                elif "```" in content:
                    content = content.split("```")[1].split("```")[0].strip()

                batch_issues = json.loads(content)

                # Enrich issues with view metadata
                for issue in batch_issues:
                    view_id = issue["view_id"]
                    view = next((v for v in batch if v["id"] == view_id), None)
                    if view:
                        issue["issue_type"] = "semantic"
                        issue["explorer_slug"] = view["explorerSlug"]
                        dimensions = json.loads(view["dimensions"]) if view["dimensions"] else {}
                        chart_config = json.loads(view["chart_config"]) if view["chart_config"] else {}
                        issue["dimensions"] = dimensions
                        issue["title"] = chart_config.get("title", "")
                        all_issues.append(issue)

            except Exception as e:
                log.error(f"Error calling Claude API for batch: {e}")
                continue

    return all_issues


def check_writing_quality_batch(
    views: list[dict[str, Any]], api_key: str | None, batch_size: int = 10
) -> list[dict[str, Any]]:
    """Check for writing quality issues using Claude API in batches.

    Args:
        views: List of view dictionaries
        api_key: Anthropic API key
        batch_size: Number of views to check in each API call

    Returns:
        List of writing quality issues found
    """
    if not api_key:
        return []

    client = anthropic.Anthropic(api_key=api_key)
    all_issues = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(f"Checking writing quality (batches of {batch_size})...", total=len(views))

        for i in range(0, len(views), batch_size):
            batch = views[i : i + batch_size]
            progress.update(task, advance=len(batch))

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

If no significant issues found, respond with an empty array.

Texts to review:
{json.dumps(batch_context, indent=2)}

Respond ONLY with a JSON array of issues, or an empty array [] if no issues found."""

            try:
                response = client.messages.create(
                    model="claude-3-5-sonnet-20241022",
                    max_tokens=4096,
                    messages=[{"role": "user", "content": prompt}],
                )

                # Parse response
                content_block = response.content[0]
                if not isinstance(content_block, TextBlock):
                    log.error(f"Unexpected content block type: {type(content_block)}")
                    continue
                content = content_block.text
                # Extract JSON from response
                if "```json" in content:
                    content = content.split("```json")[1].split("```")[0].strip()
                elif "```" in content:
                    content = content.split("```")[1].split("```")[0].strip()

                batch_issues = json.loads(content)

                # Enrich issues with view metadata
                for issue in batch_issues:
                    view_id = issue["view_id"]
                    view = next((v for v in batch if v["id"] == view_id), None)
                    if view:
                        issue["issue_type"] = "writing_quality"
                        issue["severity"] = "info"
                        issue["explorer_slug"] = view["explorerSlug"]
                        all_issues.append(issue)

            except Exception as e:
                log.error(f"Error calling Claude API for batch: {e}")
                continue

    return all_issues


def group_similar_issues(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group similar issues together (same explorer, field, and context).

    Args:
        issues: List of issue dictionaries

    Returns:
        List of grouped issues with count information
    """
    from collections import defaultdict

    # Group issues by (explorer_slug, field, typo, correction, context)
    groups = defaultdict(list)
    for issue in issues:
        if issue.get("issue_type") == "typo":
            # Group key for typos
            key = (
                issue.get("explorer_slug", ""),
                issue.get("field", ""),
                issue.get("typo", ""),
                issue.get("correction", ""),
                issue.get("context", "")[:200],  # First 200 chars of context
            )
        else:
            # For non-typo issues, use explanation
            key = (
                issue.get("explorer_slug", ""),
                issue.get("issue_type", ""),
                issue.get("explanation", ""),
            )

        groups[key].append(issue)

    # Create grouped issues with count
    grouped_issues = []
    for group in groups.values():
        # Take the first issue as representative
        representative = group[0].copy()
        # Add count information
        representative["group_count"] = len(group)
        representative["group_views"] = [issue.get("view_title", "") for issue in group]
        grouped_issues.append(representative)

    return grouped_issues


def display_issues(issues: list[dict[str, Any]], output_format: str = "table") -> None:
    """Display issues in specified format.

    Args:
        issues: List of issue dictionaries
        output_format: Output format (table, json, or csv)
    """
    if not issues:
        rprint("[green]✓ No issues found![/green]")
        return

    if output_format == "json":
        print(json.dumps(issues, indent=2))
        return

    if output_format == "csv":
        df = pd.DataFrame(issues)
        print(df.to_csv(index=False))
        return

    # Group similar issues
    grouped_issues = group_similar_issues(issues)

    # Group by severity and issue type
    critical = [i for i in grouped_issues if i.get("severity") == "critical"]
    warnings = [i for i in grouped_issues if i.get("severity") == "warning"]
    info = [i for i in grouped_issues if i.get("severity") == "info"]

    # Display summary
    total_original = len(issues)
    total_grouped = len(grouped_issues)
    rprint(f"\n[bold]Found {total_original} total issues ({total_grouped} unique):[/bold]")
    if critical:
        rprint(f"  [red]• {len(critical)} critical issues[/red]")
    if warnings:
        rprint(f"  [yellow]• {len(warnings)} warnings[/yellow]")
    if info:
        rprint(f"  [blue]• {len(info)} info[/blue]")

    # Display each category
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


@click.command(cls=RichCommand)
@click.option(
    "--explorer",
    help="Filter by specific explorer slug (e.g., 'global-food')",
    default=None,
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
    "--anthropic-api-key",
    envvar="ANTHROPIC_API_KEY",
    help="Anthropic API key for semantic and quality checks",
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
    default=10,
    help="Number of views to check per API call",
)
def main(
    explorer: str | None,
    skip_typos: bool,
    skip_semantic: bool,
    skip_quality: bool,
    anthropic_api_key: str | None,
    output_format: str,
    output_file: str | None,
    batch_size: int,
) -> None:
    """Check explorer views for typos, semantic inconsistencies, and quality issues.

    This script analyzes explorer views from the database and detects:
    - Typos using codespell (if available)
    - Semantic inconsistencies using Claude API (if API key provided)
    - Writing quality issues using Claude API (if API key provided)

    Examples:
        # Check all explorers for typos only
        python check_explorer_metadata.py --skip-semantic --skip-quality

        # Check specific explorer with all checks
        python check_explorer_metadata.py --explorer global-food --anthropic-api-key sk-...

        # Export issues to JSON
        python check_explorer_metadata.py --output-format json --output-file issues.json

        # Use environment variable for API key
        export ANTHROPIC_API_KEY=sk-...
        python check_explorer_metadata.py
    """
    # Check codespell availability
    has_codespell = get_codespell_path() is not None
    if not skip_typos and not has_codespell:
        rprint("[yellow]Warning: codespell not found. Install with: uv add codespell[/yellow]")
        rprint("[yellow]Skipping typo checks...[/yellow]")
        skip_typos = True

    # Check API key for semantic/quality checks
    if not anthropic_api_key and (not skip_semantic or not skip_quality):
        rprint("[yellow]Warning: No Anthropic API key provided.[/yellow]")
        rprint("[yellow]Set ANTHROPIC_API_KEY environment variable or use --anthropic-api-key[/yellow]")
        rprint("[yellow]Skipping semantic and quality checks...[/yellow]")
        skip_semantic = True
        skip_quality = True

    # Fetch data
    df = fetch_explorer_data(explorer_slug=explorer)

    # Check if we got any results
    if df.empty:
        if explorer:
            rprint(f"[red]Error: No views found for explorer '{explorer}'[/red]")
        else:
            rprint("[red]Error: No explorer views found in database[/red]")
        return

    if explorer:
        rprint(f"[cyan]Filtering to explorer: {explorer} ({len(df)} records)[/cyan]")

    # Aggregate views
    agg_df = aggregate_explorer_views(df)
    views: list[dict[str, Any]] = agg_df.to_dict("records")  # type: ignore

    rprint(f"[cyan]Analyzing {len(views)} explorer views...[/cyan]\n")

    all_issues = []

    # Check typos
    if not skip_typos:
        rprint("[cyan]Checking for typos (running codespell)...[/cyan]")
        issues_by_view = run_codespell_batch(views)
        # Flatten the issues
        for view_issues in issues_by_view.values():
            all_issues.extend(view_issues)
        rprint(f"[green]✓ Found {len([i for i in all_issues if i['issue_type'] == 'typo'])} typo issues[/green]\n")

    # Check semantic issues
    if not skip_semantic:
        rprint("[bold]Checking for semantic inconsistencies...[/bold]")
        semantic_issues = check_semantic_issues_batch(views, anthropic_api_key, batch_size)
        all_issues.extend(semantic_issues)
        rprint(f"[green]✓ Found {len(semantic_issues)} semantic issues[/green]\n")

    # Check writing quality
    if not skip_quality:
        rprint("[bold]Checking writing quality...[/bold]")
        quality_issues = check_writing_quality_batch(views, anthropic_api_key, batch_size)
        all_issues.extend(quality_issues)
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

    # Always display to console
    display_issues(all_issues, "table")


if __name__ == "__main__":
    main()
