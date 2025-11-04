"""Detection functions for typos and semantic issues."""

import asyncio
import json
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any

import anthropic
from anthropic.types import TextBlock
from structlog import get_logger

from apps.inspector.config import (
    CHART_FIELDS_TO_CHECK,
    CLAUDE_MODEL,
    GROUPING_MODEL,
    MAX_CONCURRENT_REQUESTS,
    MODEL_PRICING,
    VARIABLE_FIELDS_TO_CHECK,
)
from apps.inspector.db import build_explorer_url, parse_chart_config, parse_dimensions
from apps.inspector.utils import (
    call_claude,
    extract_chart_fields,
    extract_json_array,
    format_field_value,
    get_text_context,
)
from etl import config
from etl.paths import BASE_DIR

log = get_logger()


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


def check_typos(views: list[dict[str, Any]], progress_callback: Any = None) -> dict[int | str, list[dict[str, Any]]]:
    """Run codespell on all views at once for performance.

    Args:
        views: List of view dictionaries
        progress_callback: Optional callback for progress updates (callable with description and advance args)

    Returns:
        Dictionary mapping view_id (int or str for config pseudo-views) to list of typo issues
    """
    # Create a temporary directory for all text files
    temp_dir = tempfile.mkdtemp()
    view_files = {}  # Map view_id to list of (field_name, file_path, text)

    try:
        # Write all texts to temporary files
        if progress_callback:
            progress_callback(description="Preparing views for spell check", total=len(views))

        for view in views:
            # Skip config pseudo-views - they'll be added separately below
            if view.get("is_config_view"):
                if progress_callback:
                    progress_callback(advance=1)
                continue

            view_id = view["id"]
            view_files[view_id] = []

            # Collect texts to check with field names
            texts_to_check = []

            # Handle posts separately (articles, data insights, topic pages)
            if view.get("view_type") == "post":
                # For posts, check the markdown content
                markdown = view.get("markdown", "")
                if markdown:
                    texts_to_check.append(("markdown", markdown))
            else:
                # Handle chart_config - it's already a dict for chart views, but a JSON string for explorer views
                chart_config = parse_chart_config(view.get("chart_config"))

                # For chart views, dimensions come from chart_config, not from view directly
                if view.get("view_type") == "chart":
                    dimensions = []  # Chart views don't have variable dimensions like explorers
                else:
                    dimensions = parse_dimensions(view["dimensions"])

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

            if progress_callback:
                progress_callback(advance=1)

        # Add collection configs as additional files to check
        # Config pseudo-views were already created in load_views()
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
        ignore_file = BASE_DIR / ".codespell-ignore.txt"
        cmd = [str(BASE_DIR / ".venv" / "bin" / "codespell"), temp_dir]
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

        if result_lines and progress_callback:
            progress_callback(description="Processing typos found", total=len(result_lines))

        for line in result_lines:
            # Parse: /tmp/dir/view_123_field.txt:1: typo ==> correction
            parts = line.split("==>")
            if len(parts) != 2:
                if progress_callback:
                    progress_callback(advance=1)
                continue

            left = parts[0].strip()
            correction = parts[1].strip()

            # Extract file path, line number, and typo
            file_parts = left.rsplit(":", 2)
            if len(file_parts) < 3:
                if progress_callback:
                    progress_callback(advance=1)
                continue

            file_path = file_parts[0]
            line_num = file_parts[1]
            typo = file_parts[2].strip()

            # Parse line number
            try:
                line_num_int = int(line_num)
            except ValueError:
                if progress_callback:
                    progress_callback(advance=1)
                continue

            # Extract view_id and field from filename
            filename = Path(file_path).name
            if not filename.startswith("view_"):
                if progress_callback:
                    progress_callback(advance=1)
                continue

            # Parse filename: view_{view_id}_{field_name}.txt
            # For mdim views, view_id contains underscores, so we need to find it in view_files
            # CRITICAL: We must use the ACTUAL file path from codespell to find the right view_id
            # Rather than parsing the filename, search view_files for the matching file path
            filename_without_ext = filename.replace(".txt", "")
            if not filename_without_ext.startswith("view_"):
                if progress_callback:
                    progress_callback(advance=1)
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
                if progress_callback:
                    progress_callback(advance=1)
                continue

            # Verify the typo actually exists in the text
            if typo.lower() not in text.lower():
                log.warning(
                    f"Typo '{typo}' not found in text for view_id {view_id}, field {field_name}. "
                    f"Codespell reported it in {file_path}. Skipping."
                )
                if progress_callback:
                    progress_callback(advance=1)
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
                    if progress_callback:
                        progress_callback(advance=1)
                    continue

                view_title = f"Collection Config ({explorer_slug})"
                view_url = f"https://ourworldindata.org/explorers/{explorer_slug}"
                view_type = sample_view.get("view_type", "explorer")

                issue = {
                    "id": view_id,
                    "slug": explorer_slug,
                    "type": view_type,
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
                    if progress_callback:
                        progress_callback(advance=1)
                    continue

                # Build view_url and view_title based on view type
                if view.get("view_type") == "post":
                    # Post handling (articles, data insights, topic pages)
                    post_slug = view.get("slug", "")
                    base_url = config.OWID_ENV.site or "https://ourworldindata.org"
                    view_url = f"{base_url}/{post_slug}" if post_slug else ""
                    view_title = post_slug.replace("-", " ").title() if post_slug else "Post"
                elif view.get("view_type") == "chart":
                    # Chart handling
                    chart_config = parse_chart_config(view.get("chart_config"))
                    view_title = chart_config.get("title", "")
                    chart_slug = view.get("slug", "")
                    base_url = config.OWID_ENV.site or "https://ourworldindata.org"
                    view_url = f"{base_url}/grapher/{chart_slug}" if chart_slug else ""
                else:
                    # Explorer/multidim handling
                    chart_config = parse_chart_config(view.get("chart_config"))
                    dimensions = parse_dimensions(view.get("dimensions"))
                    view_title = chart_config.get("title", "")
                    mdim_published = bool(view.get("mdim_published", True))
                    mdim_catalog_path = view.get("mdim_catalog_path")
                    view_url = build_explorer_url(
                        view.get("explorerSlug", view.get("slug", "")),
                        dimensions,
                        view.get("view_type", "explorer"),
                        mdim_published,
                        mdim_catalog_path,
                    )

                issue = {
                    "id": view_id,
                    "slug": view.get("slug")
                    if view.get("view_type") in ["chart", "post"]
                    else view.get("explorerSlug", ""),
                    "type": view.get("view_type", "explorer"),
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

            if progress_callback:
                progress_callback(advance=1)

        return issues_by_view

    finally:
        # Clean up temp directory
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)


async def check_view_async(
    client: anthropic.AsyncAnthropic,
    view: dict[str, Any],
    display_prompt: bool = False,
) -> tuple[list[dict[str, Any]], int, int, int, int]:
    """Check a single view for issues asynchronously.

    Args:
        client: Async Anthropic client
        view: View dictionary to check
        display_prompt: If True, print prompt before sending

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
    elif view.get("view_type") == "post":
        # Post handling (articles, data insights, topic pages) - check the markdown content
        markdown = view.get("markdown", "")
        if not markdown or not markdown.strip():
            return [], 0, 0, 0, 0

        # For posts, check the markdown content
        fields_to_check = [("markdown", markdown)]
    elif view.get("view_type") == "chart":
        # Chart config handling - parse it first since it comes as JSON string from DB
        chart_config = parse_chart_config(view.get("chart_config"))
        if not chart_config:
            return [], 0, 0, 0, 0

        # Extract fields from chart config using helper function
        fields_to_check = extract_chart_fields(chart_config)

        # Add variable metadata fields (same as explorer views)
        for field_name in VARIABLE_FIELDS_TO_CHECK:
            values = view.get(field_name, [])
            if isinstance(values, list):
                for i, value in enumerate(values):
                    if value:
                        # Format the value (handles JSON arrays and lists nicely)
                        formatted = format_field_value(value)
                        if formatted.strip():
                            # Include index in field name for multiple variables
                            indexed_field_name = f"{field_name}_{i}" if len(values) > 1 else field_name
                            fields_to_check.append((indexed_field_name, formatted))
            elif values:
                formatted = format_field_value(values)
                if formatted.strip():
                    fields_to_check.append((field_name, formatted))

        # Handle variable_display specially - extract display name from JSON
        variable_display_values = view.get("variable_display", [])
        if isinstance(variable_display_values, list):
            for i, display_json in enumerate(variable_display_values):
                if display_json:
                    try:
                        # Parse the JSON if it's a string
                        if isinstance(display_json, str):
                            display_obj = json.loads(display_json)
                        else:
                            display_obj = display_json

                        # Extract the name field from the display object
                        if isinstance(display_obj, dict) and "name" in display_obj:
                            display_name = display_obj["name"]
                            if display_name and str(display_name).strip():
                                indexed_field_name = (
                                    f"variable_display_name_{i}"
                                    if len(variable_display_values) > 1
                                    else "variable_display_name"
                                )
                                fields_to_check.append((indexed_field_name, str(display_name)))
                    except (json.JSONDecodeError, TypeError):
                        # If parsing fails, skip this display value
                        pass
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
                    if value:
                        # Format the value (handles JSON arrays and lists nicely)
                        formatted = format_field_value(value)
                        if formatted.strip():
                            # Include index in field name for multiple variables
                            indexed_field_name = f"{field_name}_{i}" if len(values) > 1 else field_name
                            fields_to_check.append((indexed_field_name, formatted))
            elif values:
                formatted = format_field_value(values)
                if formatted.strip():
                    fields_to_check.append((field_name, formatted))

        # Handle variable_display specially - extract display name from JSON
        variable_display_values = view.get("variable_display", [])
        if isinstance(variable_display_values, list):
            for i, display_json in enumerate(variable_display_values):
                if display_json:
                    try:
                        # Parse the JSON if it's a string
                        if isinstance(display_json, str):
                            display_obj = json.loads(display_json)
                        else:
                            display_obj = display_json

                        # Extract the name field from the display object
                        if isinstance(display_obj, dict) and "name" in display_obj:
                            display_name = display_obj["name"]
                            if display_name and str(display_name).strip():
                                indexed_field_name = (
                                    f"variable_display_name_{i}"
                                    if len(variable_display_values) > 1
                                    else "variable_display_name"
                                )
                                fields_to_check.append((indexed_field_name, str(display_name)))
                    except (json.JSONDecodeError, TypeError):
                        # If parsing fails, skip this display value
                        pass

    # Skip if no substantial content (need at least 1 non-empty field)
    if len(fields_to_check) < 1:
        return [], 0, 0, 0, 0

    # Build prompt text - handle multi-line values (like bullet lists) properly
    formatted_fields = []
    for name, value in fields_to_check:
        field_label = name.replace("_", " ").title()
        # If value contains newlines (e.g., bullet points), add newline after label
        if "\n" in value:
            formatted_fields.append(f"{field_label}:\n{value}")
        else:
            formatted_fields.append(f"{field_label}: {value}")
    fields_text = "\n\n".join(formatted_fields)

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

    if display_prompt:
        from rich import print as rprint

        full_prompt = f"{instructions}\n\n{fields_text}"
        rprint(f"\n[bold yellow]{'=' * 80}[/bold yellow]")
        rprint(f"[bold yellow]Prompt to {CLAUDE_MODEL} (max_tokens=1024) for view {view.get('id')}:[/bold yellow]")
        rprint(f"[bold yellow]{'=' * 80}[/bold yellow]")
        rprint(full_prompt)
        rprint(f"[bold yellow]{'=' * 80}[/bold yellow]\n")

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
        elif view.get("view_type") == "post":
            # For posts (articles, data insights, topic pages), use the slug as title
            post_slug = view.get("slug", "")
            view_title = post_slug.replace("-", " ").title() if post_slug else "Post"
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
            issue["id"] = view["id"]
            # For charts and posts, use 'slug'; for explorers/multidims use 'explorerSlug'
            if view.get("view_type") in ["chart", "post"]:
                issue["slug"] = view.get("slug")
            else:
                issue["slug"] = view.get("explorerSlug")
            issue["type"] = view.get("view_type", "explorer")
            issue["view_title"] = view_title
            issue["source"] = "ai"  # Mark as AI-detected

            # Build URL
            if view.get("view_type") == "post":
                # Post URL (articles, data insights, topic pages)
                post_slug = view.get("slug", "")
                base_url = config.OWID_ENV.site or "https://ourworldindata.org"
                issue["view_url"] = f"{base_url}/{post_slug}" if post_slug else ""
            elif view.get("view_type") == "chart":
                # Chart configs use direct chart URL
                chart_slug = view.get("slug", "")
                base_url = config.OWID_ENV.site or "https://ourworldindata.org"
                issue["view_url"] = f"{base_url}/grapher/{chart_slug}" if chart_slug else ""
            elif view.get("is_config_view"):
                # For config pseudo-views, build base URL without dimension parameters
                view_type = view.get("view_type", "explorer")
                mdim_published = bool(view.get("mdim_published", True))
                mdim_catalog_path = view.get("mdim_catalog_path")

                issue["view_url"] = build_explorer_url(
                    view.get("explorerSlug", ""),
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
                    view.get("explorerSlug", ""),
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
    views: list[dict[str, Any]],
    api_key: str | None,
    display_prompt: bool = False,
    progress_callback: Any = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Check for semantic inconsistencies and absurdities using Claude API.

    Args:
        views: List of view dictionaries
        api_key: Anthropic API key
        display_prompt: If True, print prompts before sending to Claude
        progress_callback: Optional callback for progress updates

    Returns:
        Tuple of (list of semantic issues found, usage stats dict)
    """
    if not api_key:
        log.warning("No Claude API key provided, skipping issue checks")
        return [], {}

    # Run async checks in parallel with concurrency limiting
    async def check_all_views():
        client = anthropic.AsyncAnthropic(api_key=api_key)
        try:
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

            async def check_with_semaphore(view):
                async with semaphore:
                    return await check_view_async(client, view, display_prompt=display_prompt)

            tasks = [check_with_semaphore(view) for view in views]

            all_issues = []
            total_input_tokens = 0
            total_output_tokens = 0
            total_cache_creation_tokens = 0
            total_cache_read_tokens = 0

            if progress_callback:
                progress_callback(description="Checking for semantic issues", total=len(views))

            for coro in asyncio.as_completed(tasks):
                issues, input_tokens, output_tokens, cache_creation, cache_read = await coro
                all_issues.extend(issues)
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens
                total_cache_creation_tokens += cache_creation
                total_cache_read_tokens += cache_read
                if progress_callback:
                    progress_callback(advance=1)

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


def group_issues(
    issues: list[dict[str, Any]], api_key: str | None, display_prompt: bool = False
) -> tuple[list[dict[str, Any]], int]:
    """Group similar issues and prune spurious typos using Claude in a single API call.

    Uses Claude to:
    1. Filter typos to remove false positives based on context
    2. Group remaining typos by similarity
    3. Group semantic issues by similarity

    Args:
        issues: List of issue dictionaries
        api_key: Anthropic API key
        display_prompt: If True, print prompt before sending to Claude

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
            display_prompt=display_prompt,
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
            from rich import print as rprint

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
            from rich import print as rprint

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


def group_identical_typos(typo_issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group ABSOLUTELY identical typos within each collection.

    Groups typos by: slug + field + typo + correction + context (first 200 chars).
    Only typos that match ALL these criteria are grouped together.

    Args:
        typo_issues: List of typo issue dictionaries

    Returns:
        List of grouped typo issues with 'count' field indicating duplicates
    """
    groups = defaultdict(list)
    for issue in typo_issues:
        # Group key for typos - must match ALL fields to be considered identical
        key = (
            issue.get("slug", ""),
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
        representative["count"] = len(group)  # Number of identical occurrences
        grouped_issues.append(representative)

    return grouped_issues


def group_typos(typo_issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group typos using simple string matching (legacy function for display).

    Args:
        typo_issues: List of typo issue dictionaries

    Returns:
        List of grouped typo issues
    """
    groups = defaultdict(list)
    for issue in typo_issues:
        # Group key for typos
        key = (
            issue.get("slug", ""),
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
