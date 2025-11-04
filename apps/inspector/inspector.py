"""Script to detect typos and semantic issues in explorer views.

This script uses a three-layer approach to find issues:
1. Codespell - fast dictionary-based typo detection
2. Claude AI - comprehensive detection of typos and semantic issues and absurdities
3. Claude AI - group similar issues and prune spurious ones

"""

from pathlib import Path
from typing import Any

import click
import pandas as pd
from rich import print as rprint
from rich_click.rich_command import RichCommand

from apps.inspector.db import build_explorer_url, load_views
from apps.inspector.detectors import (
    calculate_cost,
    check_issues,
    check_typos,
    group_identical_typos,
    group_issues,
)
from apps.inspector.display import (
    create_progress_bar,
    display_cost_estimate,
    display_results,
    estimate_check_costs,
    print_step,
)
from etl import config


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
        if "slug" in df.columns:
            completed = set(df["slug"].dropna().unique())
            return completed
        return set()
    except Exception:
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
    expected_columns = ["slug", "id", "type", "url", "issue_type", "field", "context", "explanation", "count"]

    # Filter issues to only include expected columns
    filtered_issues = []
    for issue in issues:
        # Map view_url to url for CSV output
        filtered_issue = {}
        for col in expected_columns:
            if col == "url":
                filtered_issue[col] = issue.get("view_url", "")
            elif col == "count":
                # Use count field if present (from grouping), otherwise default to 1
                filtered_issue[col] = issue.get("count", 1)
            else:
                filtered_issue[col] = issue.get(col, "")
        filtered_issues.append(filtered_issue)

    df = pd.DataFrame(filtered_issues, columns=expected_columns)
    output_path = Path(output_file)

    # Append to existing file or create new one
    if output_path.exists():
        df.to_csv(output_path, mode="a", header=False, index=False)
    else:
        df.to_csv(output_path, index=False)


def run_checks(
    views: list[dict[str, Any]],
    skip_typos: bool,
    skip_issues: bool,
    quiet: bool = False,
    display_prompt: bool = False,
) -> tuple[list[dict[str, Any]], int, int, int, int]:
    """Run all enabled checks and return issues and token usage.

    Args:
        views: List of view dictionaries to check
        skip_typos: Skip codespell typo checking
        skip_issues: Skip Claude API semantic checking
        quiet: Suppress progress messages (useful when processing many items with outer progress bar)
        display_prompt: If True, print prompts before sending to Claude

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
        print_step("[cyan]Checking for typos with codespell...[/cyan]", quiet)

        # Create progress callback
        with create_progress_bar("", quiet) as progress:
            if progress is not None:
                task = progress.add_task("", total=0)

                def progress_callback(description=None, total=None, advance=None):
                    if progress is not None:
                        if description is not None and total is not None:
                            progress.update(task, description=description, total=total, completed=0)
                        elif advance is not None:
                            progress.advance(task, advance)

                issues_by_view = check_typos(views, progress_callback=progress_callback)
            else:
                issues_by_view = check_typos(views, progress_callback=None)

        for view_issues in issues_by_view.values():
            all_issues.extend(view_issues)

        # Group identical codespell typos immediately
        codespell_issues = [i for i in all_issues if i.get("issue_type") == "typo" and i.get("source") == "codespell"]
        if codespell_issues:
            grouped_codespell = group_identical_typos(codespell_issues)
            # Replace codespell issues in all_issues with grouped ones
            all_issues = [
                i for i in all_issues if not (i.get("issue_type") == "typo" and i.get("source") == "codespell")
            ]
            all_issues.extend(grouped_codespell)

        codespell_typos = len([i for i in all_issues if i["issue_type"] == "typo" and i.get("source") == "codespell"])
        print_step(f"[green]✓ Found {codespell_typos} unique typos with codespell[/green]\n", quiet)

    # Check for all issues with Claude (typos + semantic)
    if not skip_issues:
        print_step("[bold]Checking for typos and semantic issues with Claude...[/bold]", quiet)

        # Create progress callback
        with create_progress_bar("", quiet) as progress:
            if progress is not None:
                task = progress.add_task("", total=0)

                def progress_callback(description=None, total=None, advance=None):
                    if progress is not None:
                        if description is not None and total is not None:
                            progress.update(task, description=description, total=total, completed=0)
                        elif advance is not None:
                            progress.advance(task, advance)

                issues, usage_stats = check_issues(
                    views, config.ANTHROPIC_API_KEY, display_prompt=display_prompt, progress_callback=progress_callback
                )
            else:
                issues, usage_stats = check_issues(
                    views, config.ANTHROPIC_API_KEY, display_prompt=display_prompt, progress_callback=None
                )

        all_issues.extend(issues)
        total_input_tokens += usage_stats.get("input_tokens", 0)
        total_output_tokens += usage_stats.get("output_tokens", 0)
        cache_creation_tokens += usage_stats.get("cache_creation_tokens", 0)
        cache_read_tokens += usage_stats.get("cache_read_tokens", 0)

        claude_typos = len([i for i in issues if i.get("issue_type") == "typo"])
        semantic_issues = len([i for i in issues if i.get("issue_type") == "semantic"])
        print_step(
            f"[green]✓ Found {claude_typos} typos and {semantic_issues} semantic issues with Claude[/green]\n", quiet
        )

    return all_issues, total_input_tokens, total_output_tokens, cache_creation_tokens, cache_read_tokens


@click.command(cls=RichCommand)
@click.option(
    "--slug",
    multiple=True,
    help="Filter by specific slug (explorer, multidim, chart, or post). Can be specified multiple times (e.g., '--slug global-food --slug covid-boosters')",
)
@click.option(
    "--type",
    "content_type",
    type=click.Choice(["explorer", "multidim", "chart", "post"], case_sensitive=False),
    help="Filter by content type. Useful when a slug exists in multiple types (e.g., both explorer and post with same slug)",
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
    "--enable-grouping",
    is_flag=True,
    help="Enable grouping and pruning of similar issues (EXPERIMENTAL: may not work well with large numbers of collections)",
)
@click.option(
    "--output-file",
    type=click.Path(),
    help="Save issues to CSV file",
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
@click.option(
    "--display-prompt",
    is_flag=True,
    help="Print the exact prompts sent to Claude API",
)
def run(
    slug: tuple[str, ...],
    content_type: str | None,
    skip_typos: bool,
    skip_issues: bool,
    enable_grouping: bool,
    output_file: str | None,
    limit: int | None,
    dry_run: bool,
    display_prompt: bool,
) -> None:
    """Check explorer, multidim views, chart configs, and posts for typos and semantic issues.

    Examples:
        python metadata_inspector.py --skip-issues
        python metadata_inspector.py --slug global-food
        python metadata_inspector.py --slug natural-disasters --type post
        python metadata_inspector.py --slug global-food --slug covid-boosters
        python metadata_inspector.py --slug animal-welfare --limit 5
        python metadata_inspector.py --output-file issues.csv
    """
    if not config.ANTHROPIC_API_KEY and not skip_issues:
        rprint("[red]Error: ANTHROPIC_API_KEY not found. Add to .env file or use --skip-issues[/red]")
        raise click.ClickException("Missing ANTHROPIC_API_KEY")

    # Load views (explorers, multidims, charts, and posts)
    slug_list = list(slug) if slug else None

    # Load all matching views, optionally filtered by type
    views = load_views(slug_list, limit, content_type)
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
        total_input_tokens, total_output_tokens = estimate_check_costs(views, skip_typos, skip_issues)
        display_cost_estimate(total_input_tokens, total_output_tokens, skip_issues, len(views))
        return

    # Group views by collection (explorerSlug for explorers, slug for charts/posts) for incremental processing
    # Separate charts and posts from explorer/multidim collections for better progress display
    chart_collections: dict[str, list[dict[str, Any]]] = {}
    post_collections: dict[str, list[dict[str, Any]]] = {}
    other_collections: dict[str, list[dict[str, Any]]] = {}

    for view in views:
        # Charts and posts use 'slug', explorers/multidims use 'explorerSlug'
        view_type = view.get("view_type")
        if view_type in ["chart", "post"]:
            collection_slug = view.get("slug")
            target_dict = post_collections if view_type == "post" else chart_collections
        else:
            collection_slug = view.get("explorerSlug")
            target_dict = other_collections

        if collection_slug:
            if collection_slug not in target_dict:
                target_dict[collection_slug] = []
            target_dict[collection_slug].append(view)

    # Process collections one by one, saving after each
    all_issues = []
    total_input_tokens = 0
    total_output_tokens = 0
    total_cache_creation_tokens = 0
    total_cache_read_tokens = 0
    total_cost = 0.0

    total_collections = len(other_collections) + len(chart_collections) + len(post_collections)
    rprint(
        f"\n[bold cyan]Processing {total_collections} collection(s): {len(other_collections)} explorer/multidim(s) + {len(chart_collections)} chart(s) + {len(post_collections)} post(s)...[/bold cyan]\n"
    )

    # Process explorers/multidims first
    for collection_num, (collection_slug, collection_views) in enumerate(other_collections.items(), 1):
        rprint(f"[bold]Collection {collection_num}/{len(other_collections)}: {collection_slug}[/bold]")

        # Run checks for this collection
        collection_issues, input_tokens, output_tokens, cache_creation, cache_read = run_checks(
            collection_views, skip_typos, skip_issues, display_prompt=display_prompt
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
                if collection_views:
                    collection_type = collection_views[0].get("view_type", "explorer")
                    collection_id = collection_views[0].get("id")

                    # Build URL based on type
                    base_url = config.OWID_ENV.site
                    if collection_type == "chart":
                        collection_url = f"{base_url}/grapher/{collection_slug}"
                    elif collection_type == "multidim":
                        # For multidims, use the first view to get publish status and catalog path
                        mdim_published = bool(collection_views[0].get("mdim_published", True))
                        mdim_catalog_path = collection_views[0].get("mdim_catalog_path")
                        collection_url = build_explorer_url(
                            collection_slug, {}, "multidim", mdim_published, mdim_catalog_path
                        )
                    else:  # explorer
                        collection_url = f"{base_url}/explorers/{collection_slug}"
                else:
                    collection_type = "explorer"
                    collection_id = None
                    collection_url = ""

                marker = {
                    "slug": collection_slug,
                    "id": collection_id,
                    "type": collection_type,
                    "view_url": collection_url,
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

    # Process charts separately with their own progress display
    if chart_collections:
        rprint(f"\n[bold cyan]Processing {len(chart_collections)} chart(s)...[/bold cyan]")

        # Use a progress bar for charts
        with create_progress_bar("") as progress:
            task = None
            if progress is not None:
                task = progress.add_task("[cyan]Checking charts...", total=len(chart_collections))

            for chart_num, (chart_slug, chart_views) in enumerate(chart_collections.items(), 1):
                # Update progress description with current chart
                if progress is not None and task is not None:
                    progress.update(task, description=f"[cyan]Chart {chart_num}/{len(chart_collections)}: {chart_slug}")

                # Run checks for this chart (quiet mode to avoid cluttering progress bar)
                collection_issues, input_tokens, output_tokens, cache_creation, cache_read = run_checks(
                    chart_views, skip_typos, skip_issues, quiet=True, display_prompt=display_prompt
                )

                # Calculate cost for this chart
                collection_cost = calculate_cost(input_tokens, output_tokens, cache_creation, cache_read)
                total_cost += collection_cost

                total_input_tokens += input_tokens
                total_output_tokens += output_tokens
                total_cache_creation_tokens += cache_creation
                total_cache_read_tokens += cache_read

                # Save this chart's results immediately (even if no issues, for checkpoint tracking)
                if output_file:
                    if collection_issues:
                        save_collection_results(output_file, collection_issues)
                    else:
                        # Save a marker row to track that this chart was processed
                        if chart_views:
                            chart_id = chart_views[0].get("id")
                            base_url = config.OWID_ENV.site
                            chart_url = f"{base_url}/grapher/{chart_slug}"

                            marker = {
                                "slug": chart_slug,
                                "id": chart_id,
                                "type": "chart",
                                "view_url": chart_url,
                                "issue_type": "checkpoint",
                                "field": "completed",
                                "context": "",
                                "explanation": "Collection processed with no issues found",
                            }
                            save_collection_results(output_file, [marker])

                all_issues.extend(collection_issues)

                # Advance progress
                if progress is not None and task is not None:
                    progress.advance(task, 1)

        rprint(f"[green]✓ Completed processing {len(chart_collections)} chart(s)[/green]")
        rprint(f"[dim]Total charts cost: ${total_cost:.4f}[/dim]\n")

    # Process posts separately with their own progress display
    if post_collections:
        rprint(f"\n[bold cyan]Processing {len(post_collections)} post(s)...[/bold cyan]")

        # Use a progress bar for posts
        with create_progress_bar("") as progress:
            task = None
            if progress is not None:
                task = progress.add_task("[green]Checking posts...", total=len(post_collections))

            for post_num, (post_slug, post_views) in enumerate(post_collections.items(), 1):
                # Update progress description with current post
                if progress is not None and task is not None:
                    progress.update(task, description=f"[green]Post {post_num}/{len(post_collections)}: {post_slug}")

                # Run checks for this post (quiet mode to avoid cluttering progress bar)
                collection_issues, input_tokens, output_tokens, cache_creation, cache_read = run_checks(
                    post_views, skip_typos, skip_issues, quiet=True, display_prompt=display_prompt
                )

                # Calculate cost for this post
                collection_cost = calculate_cost(input_tokens, output_tokens, cache_creation, cache_read)
                total_cost += collection_cost

                total_input_tokens += input_tokens
                total_output_tokens += output_tokens
                total_cache_creation_tokens += cache_creation
                total_cache_read_tokens += cache_read

                # Save this post's results immediately (even if no issues, for checkpoint tracking)
                if output_file:
                    if collection_issues:
                        save_collection_results(output_file, collection_issues)
                    else:
                        # Save a marker row to track that this post was processed
                        if post_views:
                            post_id = post_views[0].get("id")
                            post_type = post_views[0].get("type", "")
                            base_url = config.OWID_ENV.site
                            # Data insights need /data-insights/ prefix
                            if post_type == "data-insight":
                                post_url = f"{base_url}/data-insights/{post_slug}"
                            else:
                                post_url = f"{base_url}/{post_slug}"

                            marker = {
                                "slug": post_slug,
                                "id": post_id,
                                "type": "post",
                                "view_url": post_url,
                                "issue_type": "checkpoint",
                                "field": "completed",
                                "context": "",
                                "explanation": "Collection processed with no issues found",
                            }
                            save_collection_results(output_file, [marker])

                all_issues.extend(collection_issues)

                # Advance progress
                if progress is not None and task is not None:
                    progress.advance(task, 1)

        rprint(f"[green]✓ Completed processing {len(post_collections)} post(s)[/green]")
        rprint(f"[dim]Total posts cost: ${total_cost:.4f}[/dim]\n")

    # Group and prune issues (across all collections)
    grouping_tokens = 0
    if all_issues and enable_grouping:
        rprint("  [cyan]Grouping similar issues and pruning false positives...[/cyan]")
        grouped_issues, grouping_tokens = group_issues(
            all_issues, config.ANTHROPIC_API_KEY, display_prompt=display_prompt
        )
        # Note: grouping tokens are tracked separately since they use a different model
    else:
        grouped_issues = all_issues

    # Display results (grouping tokens tracked separately for cost calculation)
    display_results(grouped_issues, views, total_input_tokens, total_output_tokens, grouping_tokens)
