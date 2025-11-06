from contextlib import contextmanager
from typing import Any

from rich import print as rprint
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeRemainingColumn

from apps.inspector.config import CLAUDE_MODEL, GROUPING_MODEL, MODEL_PRICING
from apps.inspector.detectors import calculate_cost
from apps.inspector.utils import estimate_tokens

console = Console()


def display_issues(
    issues: list[dict[str, Any]],
    all_explorers: list[str],
    mdim_slugs: set[str],
    chart_slugs: set[str],
    post_slugs: set[str],
) -> None:
    """Display grouped issues.

    Args:
        issues: List of already-grouped issue dictionaries
        all_explorers: List of all explorer slugs that were analyzed
        mdim_slugs: Set of slugs that are multidimensional indicators
        chart_slugs: Set of slugs that are charts
        post_slugs: Set of slugs that are posts (articles, data insights, topic pages)
    """
    from collections import defaultdict

    # Group issues by slug (explorer/multidim/chart)
    issues_by_explorer = defaultdict(list)
    for issue in issues:
        slug = issue.get("slug", "unknown")
        # Convert slug to string to avoid TypeError when sorting mixed types
        slug = str(slug) if slug is not None else "unknown"
        issues_by_explorer[slug].append(issue)

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

    # Display issues section only if there are issues
    if issues:
        # Display summary
        total_unique = len(issues)
        # Calculate total original count from count field (or similar_count for backwards compat)
        total_original = sum(issue.get("count", issue.get("similar_count", 1)) for issue in issues)
        num_collections = len(issues_by_explorer)
        rprint(
            f"\n[bold]Found {total_original} total issues ({total_unique} unique) across {num_collections} collection(s)[/bold]"
        )

    # Display issues grouped by explorer
    for explorer_slug in sorted(issues_by_explorer.keys()):
        explorer_issues = issues_by_explorer[explorer_slug]

        # Display header with appropriate label and color
        # Determine type from the actual issues, not just slug presence
        # (since same slug can exist as both explorer and post)
        issue_types = {issue.get("type") for issue in explorer_issues}

        # Pick the most common type, or the first one if tied
        if "post" in issue_types:
            label = "Post"
            color = "green"
        elif "chart" in issue_types:
            label = "Chart"
            color = "yellow"
        elif "multidim" in issue_types:
            label = "Multidim"
            color = "magenta"
        else:
            label = "Explorer"
            color = "cyan"

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
            # Use 'count' field from grouping (for CSV consistency), fallback to 'similar_count' for backwards compat
            count = issue.get("count", issue.get("similar_count", 1))
            source = issue.get("source", "unknown")

            # Print issue header with embedded clickable link
            view_id = issue.get("id", "unknown")

            if view_url:
                # Embed link in title for clickable terminal support
                rprint(f"\n[bold]{i}. [link={view_url}]{view_title}[/link][/bold] [dim](id: {view_id})[/dim]")
            else:
                rprint(f"\n[bold]{i}. {view_title}[/bold] [dim](id: {view_id})[/dim]")

            # Show count if more than 1 occurrence
            if count > 1:
                rprint(f"   [dim]({count} identical occurrences in this collection)[/dim]")

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

    # Show clean explorers/mdims/charts/posts if we have the full list
    if all_explorers and mdim_slugs is not None and chart_slugs is not None and post_slugs is not None:
        explorers_with_issues = set(issues_by_explorer.keys())
        # Filter out NaN values (from NULL slugs) before sorting
        all_explorers_valid = {e for e in all_explorers if isinstance(e, str)}
        explorers_with_issues_valid = {e for e in explorers_with_issues if isinstance(e, str)}
        all_clean = sorted(all_explorers_valid - explorers_with_issues_valid)

        # Separate into explorers, mdims, charts, and posts
        clean_explorers = [e for e in all_clean if e not in mdim_slugs and e not in chart_slugs and e not in post_slugs]
        clean_mdims = [e for e in all_clean if e in mdim_slugs]
        clean_charts = [e for e in all_clean if e in chart_slugs]
        clean_posts = [e for e in all_clean if e in post_slugs]

        if clean_explorers or clean_mdims or clean_charts or clean_posts:
            rprint(f"\n[bold green]{'=' * 80}[/bold green]")

            if clean_explorers:
                rprint(f"[bold cyan]✓ No issues found in {len(clean_explorers)} explorer(s):[/bold cyan]")
                rprint(f"[cyan]{', '.join(clean_explorers)}[/cyan]")

            if clean_mdims:
                if clean_explorers:
                    rprint()  # Add spacing between the lists
                rprint(f"[bold magenta]✓ No issues found in {len(clean_mdims)} multidim(s):[/bold magenta]")
                rprint(f"[magenta]{', '.join(clean_mdims)}[/magenta]")

            if clean_charts:
                if clean_explorers or clean_mdims:
                    rprint()  # Add spacing between the lists
                rprint(f"[bold yellow]✓ No issues found in {len(clean_charts)} chart(s):[/bold yellow]")
                rprint(f"[yellow]{', '.join(clean_charts)}[/yellow]")

            if clean_posts:
                if clean_explorers or clean_mdims or clean_charts:
                    rprint()  # Add spacing between the lists
                rprint(f"[bold green]✓ No issues found in {len(clean_posts)} post(s):[/bold green]")
                rprint(f"[green]{', '.join(clean_posts)}[/green]")


def display_results(
    grouped_issues: list[dict[str, Any]],
    views: list[dict[str, Any]],
    total_input_tokens: int,
    total_output_tokens: int,
    grouping_tokens: int,
    model: str | None = None,
) -> None:
    """Display grouped issues and cost.

    Args:
        grouped_issues: List of issue dictionaries
        views: List of view dictionaries
        total_input_tokens: Total input tokens used
        total_output_tokens: Total output tokens used
        grouping_tokens: Tokens used for grouping
        model: Model name used for detection (defaults to CLAUDE_MODEL)
    """
    # Extract unique slugs and identify types
    # Charts and posts use 'slug', explorers/multidims use 'explorerSlug'
    all_slugs = []
    for view in views:
        if view.get("view_type") in ["chart", "post"]:
            slug = view.get("slug")
        else:
            slug = view.get("explorerSlug")
        if slug:
            all_slugs.append(slug)
    all_explorers_analyzed = list(set(all_slugs))

    mdim_slugs_list = [view.get("explorerSlug") for view in views if view.get("view_type") == "multidim"]
    mdim_slugs = set(s for s in mdim_slugs_list if s is not None)

    chart_slugs_list = [view.get("slug") for view in views if view.get("view_type") == "chart"]
    chart_slugs = set(s for s in chart_slugs_list if s is not None)

    post_slugs_list = [view.get("slug") for view in views if view.get("view_type") == "post"]
    post_slugs = set(s for s in post_slugs_list if s is not None)

    # Display grouped issues
    display_issues(grouped_issues, all_explorers_analyzed, mdim_slugs, chart_slugs, post_slugs)

    # Display API usage and cost
    if total_input_tokens > 0 or total_output_tokens > 0 or grouping_tokens > 0:
        # Calculate cost for issue detection
        detection_cost = calculate_cost(total_input_tokens, total_output_tokens, model=model)

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


def display_cost_estimate(
    total_input_tokens: int, total_output_tokens: int, skip_issues: bool, num_views: int = 0, model: str | None = None
) -> None:
    """Display cost estimate in dry-run mode with prompt caching.

    Args:
        total_input_tokens: Total input tokens
        total_output_tokens: Total output tokens
        skip_issues: Whether issue checking is skipped
        num_views: Number of views to check
        model: Model name to use for cost estimate (defaults to CLAUDE_MODEL)
    """
    detection_model = model or CLAUDE_MODEL
    # Estimate grouping/pruning tokens if issue checks are enabled
    grouping_tokens_estimate = 0
    if not skip_issues:
        grouping_tokens_estimate = 500
        total_input_tokens += grouping_tokens_estimate // 2
        total_output_tokens += grouping_tokens_estimate // 2

    rprint("\n[bold yellow]DRY RUN - Cost Estimate:[/bold yellow]")
    if total_input_tokens > 0 or total_output_tokens > 0:
        # Calculate cost WITHOUT caching (baseline)
        estimated_cost_no_cache = calculate_cost(total_input_tokens, total_output_tokens, model=detection_model)

        # Calculate cost WITH prompt caching
        # Instructions are ~250 tokens and are cached after first request
        instruction_tokens = 250
        if num_views > 0 and detection_model in MODEL_PRICING:
            # Get pricing for the current model
            pricing = MODEL_PRICING[detection_model]
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


@contextmanager
def create_progress_bar(description: str = "Processing", quiet: bool = False):
    """Create a progress bar context manager.

    Args:
        description: Description to show in progress bar
        quiet: If True, suppress progress bar entirely

    Yields:
        Progress object or None if quiet=True
    """
    if quiet:
        # Return a no-op context manager

        yield None
    else:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            yield progress


def print_step(message: str, quiet: bool = False):
    """Print a step message unless quiet mode is enabled.

    Args:
        message: Message to print
        quiet: If True, suppress output
    """
    if not quiet:
        rprint(message)


def estimate_check_costs(
    views: list[dict[str, Any]],
    skip_typos: bool,
    skip_issues: bool,
) -> tuple[int, int]:
    """Estimate token costs for checks without running them.

    Returns:
        Tuple of (input_tokens, output_tokens)
    """
    from apps.inspector.db import parse_chart_config

    total_input_tokens = 0
    total_output_tokens = 0

    if not skip_issues:
        rprint("[yellow]Estimating issue check costs (dry run)...[/yellow]")
        # Estimate costs by simulating what would be checked
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
                from apps.inspector.config import CHART_FIELDS_TO_CHECK, VARIABLE_FIELDS_TO_CHECK

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

    return total_input_tokens, total_output_tokens
