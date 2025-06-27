import re
import subprocess
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from click.testing import CliRunner
from fastmcp import FastMCP
from owid import catalog

from apps.pr.cli import cli as pr_cli
from apps.step_update.cli import cli as step_update_cli
from etl.command import main_cli as etl_cli


@dataclass
class TableMetadata:
    """Metadata for a table in the data catalog."""

    table: Optional[str]
    namespace: Optional[str]
    dataset: Optional[str]
    version: Optional[str]
    channel: Optional[str]
    path: Optional[str]
    download_url: Optional[str]
    dimensions: List[str]
    description: str


@dataclass
class PRResult:
    """Result of creating a pull request."""

    success: bool
    pr_url: Optional[str]
    branch_name: Optional[str]
    message: str


@dataclass
class StepUpdateResult:
    """Result of updating ETL steps."""

    success: bool
    updated_steps: List[str]
    message: str
    dry_run: bool


@dataclass
class ETLRunResult:
    """Result of running ETL steps."""

    success: bool
    executed_steps: List[str]
    skipped_steps: List[str]
    failed_steps: List[str]
    message: str
    dry_run: bool
    execution_time: Optional[float] = None


def invoke_cli_tool(
    cli_command: Callable, args: List[str], cli_options: Dict[str, Any], result_parser: Callable[[Any], Any]
) -> Any:
    """Generic CLI tool invoker with error handling.

    Args:
        cli_command: The Click CLI command to invoke
        args: Positional arguments for the CLI
        cli_options: Dictionary of CLI options (key: option_name, value: option_value)
        result_parser: Function to parse the CLI result into return type

    Returns:
        Parsed result from result_parser function
    """
    try:
        runner = CliRunner()

        # Build CLI arguments
        cli_args = args.copy()

        # Add options
        for option, value in cli_options.items():
            if isinstance(value, bool) and value:
                cli_args.append(f"--{option.replace('_', '-')}")
            elif value is not None and not isinstance(value, bool):
                cli_args.extend([f"--{option.replace('_', '-')}", str(value)])

        # Invoke the CLI
        result = runner.invoke(cli_command, cli_args, catch_exceptions=False)

        return result_parser(result)

    except Exception as e:
        return result_parser(type("MockResult", (), {"exit_code": 1, "output": str(e), "exception": e})())


mcp = FastMCP("Data Catalog 🚀")


@mcp.tool
def find_table(
    table: Optional[str] = None,
    namespace: Optional[str] = None,
    dataset: Optional[str] = None,
    version: Optional[str] = None,
    channel: Optional[str] = "garden",
) -> List[TableMetadata]:
    """Find tables in the data catalog using various filters.

    Args:
        table: Table name (supports regex patterns)
        namespace: Namespace to filter by
        dataset: Dataset name to filter by
        version: Version to filter by
        channel: Channel to search in (default: "garden")

    Returns:
        List of TableMetadata objects with table information
    """
    # Use catalog.find to search for tables
    results = catalog.find(
        table=table,
        namespace=namespace,
        dataset=dataset,
        version=version,
        channels=[channel] if channel else ["garden"],
    )

    # Convert results to a list of TableMetadata objects
    tables = []
    for _, row in results.iterrows():
        # Generate download URL from path
        path = row.get("path")
        download_url = None
        if path:
            download_url = f"https://catalog.ourworldindata.org/{path}.feather"

        tables.append(
            TableMetadata(
                table=row.get("table"),
                namespace=row.get("namespace"),
                dataset=row.get("dataset"),
                version=row.get("version"),
                channel=row.get("channel"),
                path=path,
                download_url=download_url,
                dimensions=row.get("dimensions", []),
                description=row.get("description", ""),
            )
        )

    return tables


def _get_current_branch() -> str:
    """Get the current git branch."""
    result = subprocess.run(["git", "branch", "--show-current"], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def _parse_pr_result(result, work_branch: Optional[str]) -> PRResult:
    """Parse CLI result for PR creation."""
    if hasattr(result, "exception"):
        return PRResult(
            success=False,
            pr_url=None,
            branch_name=work_branch,
            message=f"Error calling PR CLI: {str(result.exception)}",
        )

    if result.exit_code == 0:
        # Extract PR URL from output if available
        output_lines = result.output.split("\n")
        pr_url = None

        for line in output_lines:
            if "html_url" in line or "github.com" in line:
                url_match = re.search(r"https://github\.com/[^\s]+", line)
                if url_match:
                    pr_url = url_match.group()
                    break

        return PRResult(
            success=True, pr_url=pr_url, branch_name=work_branch, message="Pull request created successfully"
        )
    else:
        return PRResult(
            success=False,
            pr_url=None,
            branch_name=work_branch,
            message=f"CLI failed with exit code {result.exit_code}: {result.output}",
        )


@mcp.tool
def create_pr(
    title: str,
    category: Optional[str] = None,
    scope: Optional[str] = None,
    work_branch: Optional[str] = None,
    base_branch: Optional[str] = None,
    direct: bool = False,
    private: bool = False,
    no_llm: bool = False,
) -> PRResult:
    """Create a pull request using the existing PR CLI functionality.

    Args:
        title: The title of the PR
        category: The category of the PR (data, bug, refactor, enhance, feature, docs, chore, style, wip, tests)
        scope: Scope of the PR (optional)
        work_branch: Name of the work branch to create (auto-generated if not provided)
        base_branch: Name of the base branch to merge into (default: current branch)
        direct: Create PR from current branch instead of creating new branch
        private: Make staging server private
        no_llm: Disable LLM for branch name generation

    Returns:
        PRResult with success status, PR URL, branch name, and message
    """
    # Get current branch if base_branch not provided
    if base_branch is None:
        base_branch = _get_current_branch()

    # Build arguments for the CLI
    args = [title]
    if category:
        args.append(category)

    # Build options
    options = {
        "scope": scope,
        "work_branch": work_branch,
        "base_branch": base_branch,
        "direct": direct,
        "private": private,
        "no_llm": no_llm,
    }

    return invoke_cli_tool(pr_cli, args, options, lambda result: _parse_pr_result(result, work_branch))


def _parse_step_update_result(result, steps: List[str], dry_run: bool) -> StepUpdateResult:
    """Parse CLI result for step update."""
    if hasattr(result, "exception"):
        return StepUpdateResult(
            success=False, updated_steps=[], message=f"Error updating steps: {str(result.exception)}", dry_run=dry_run
        )

    if result.exit_code == 0:
        # Parse output to extract updated steps
        output_lines = result.output.split("\n")
        updated_steps = []

        # Look for updated step information in the output
        for line in output_lines:
            if "Updating step:" in line or "Updated:" in line:
                step_match = re.search(r"(data://[^\s]+|snapshot://[^\s]+)", line)
                if step_match:
                    updated_steps.append(step_match.group())

        # If no specific steps found, use the input steps as they were processed
        if not updated_steps:
            updated_steps = steps

        return StepUpdateResult(
            success=True,
            updated_steps=updated_steps,
            message=f"Successfully {'previewed' if dry_run else 'updated'} {len(updated_steps)} step(s)",
            dry_run=dry_run,
        )
    else:
        return StepUpdateResult(
            success=False,
            updated_steps=[],
            message=f"Step update failed with exit code {result.exit_code}: {result.output}",
            dry_run=dry_run,
        )


@mcp.tool
def update_step(
    steps: List[str],
    step_version_new: Optional[str] = None,
    include_dependencies: bool = False,
    include_usages: bool = False,
    dry_run: bool = True,
    non_interactive: bool = True,
) -> StepUpdateResult:
    """Update ETL steps to new versions.

    Args:
        steps: List of step URIs to update (e.g., ["data://garden/biodiversity/2025-04-07/cherry_blossom", "snapshot://biodiversity/2024-01-25/cherry_blossom.xls"])
        step_version_new: New version for steps (default: current date)
        include_dependencies: Update direct dependencies of given steps
        include_usages: Update steps that directly use the given steps
        dry_run: Preview changes without executing (default: True for safety)
        non_interactive: Run without user prompts (default: True)

    Returns:
        StepUpdateResult with success status, list of updated steps, and message
    """
    options = {
        "step_version_new": step_version_new,
        "include_dependencies": include_dependencies,
        "include_usages": include_usages,
        "dry_run": dry_run,
        "non_interactive": non_interactive,
    }

    return invoke_cli_tool(
        step_update_cli, steps, options, lambda result: _parse_step_update_result(result, steps, dry_run)
    )


def _parse_etl_run_result(result, steps: List[str], dry_run: bool) -> ETLRunResult:
    """Parse CLI result for ETL run."""
    if hasattr(result, "exception"):
        return ETLRunResult(
            success=False,
            executed_steps=[],
            skipped_steps=[],
            failed_steps=steps,
            message=f"Error running ETL: {str(result.exception)}",
            dry_run=dry_run
        )
    
    if result.exit_code == 0:
        # Parse output to extract step information
        output_lines = result.output.split("\n")
        executed_steps = []
        skipped_steps = []
        failed_steps = []
        
        # Look for step execution information in the output
        for line in output_lines:
            if "Executing step:" in line or "Running step:" in line:
                step_match = re.search(r'(data://[^\s]+|snapshot://[^\s]+)', line)
                if step_match:
                    executed_steps.append(step_match.group())
            elif "Skipping step:" in line or "Already up-to-date:" in line:
                step_match = re.search(r'(data://[^\s]+|snapshot://[^\s]+)', line)
                if step_match:
                    skipped_steps.append(step_match.group())
            elif "Failed step:" in line or "Error in step:" in line:
                step_match = re.search(r'(data://[^\s]+|snapshot://[^\s]+)', line)
                if step_match:
                    failed_steps.append(step_match.group())
        
        # If no specific steps found, assume input steps were processed
        if not executed_steps and not skipped_steps and not failed_steps:
            executed_steps = steps
        
        return ETLRunResult(
            success=True,
            executed_steps=executed_steps,
            skipped_steps=skipped_steps,
            failed_steps=failed_steps,
            message=f"Successfully {'previewed' if dry_run else 'executed'} {len(executed_steps)} step(s), skipped {len(skipped_steps)}",
            dry_run=dry_run
        )
    else:
        return ETLRunResult(
            success=False,
            executed_steps=[],
            skipped_steps=[],
            failed_steps=steps,
            message=f"ETL run failed with exit code {result.exit_code}: {result.output}",
            dry_run=dry_run
        )


@mcp.tool
def run_etl(
    steps: List[str],
    dry_run: bool = False,
    force: bool = False,
    private: bool = False,
    instant: bool = False,
    grapher: bool = False,
    export: bool = False,
    downstream: bool = False,
    only: bool = False,
    exact_match: bool = False,
    exclude: Optional[str] = None,
    workers: int = 1,
    use_threads: bool = True,
    strict: Optional[bool] = None,
    continue_on_failure: bool = False,
    force_upload: bool = False,
    prefer_download: bool = False,
    subset: Optional[str] = None,
) -> ETLRunResult:
    """Run ETL steps using the existing ETL CLI functionality.

    Args:
        steps: List of step patterns to run (e.g., ["biodiversity/2025-06-28/cherry_blossom"])
        dry_run: Preview the steps without actually running them
        force: Re-run the steps even if they appear done and up-to-date
        private: Run private steps
        instant: Only apply YAML metadata in the garden step
        grapher: Upsert datasets from grapher channel to DB (OWID staff only)
        export: Run export steps like saving explorer (OWID staff only)
        downstream: Include downstream dependencies
        only: Only run the selected step (no upstream or downstream dependencies)
        exact_match: Steps should exactly match the arguments
        exclude: Comma-separated patterns to exclude
        workers: Parallelize execution of steps
        use_threads: Use threads when checking dirty steps and upserting to MySQL
        strict: Force strict or lax validation on DAG steps
        continue_on_failure: Continue running remaining steps if a step fails
        force_upload: Always upload grapher data even if checksums match
        prefer_download: Prefer downloading datasets from catalog instead of building them
        subset: Filter to speed up development (regex for data processing and grapher upload)

    Returns:
        ETLRunResult with success status, executed/skipped/failed steps, and message
    """
    options = {
        "dry_run": dry_run,
        "force": force,
        "private": private,
        "instant": instant,
        "grapher": grapher,
        "export": export,
        "downstream": downstream,
        "only": only,
        "exact_match": exact_match,
        "exclude": exclude,
        "workers": workers,
        "use_threads": use_threads,
        "strict": strict,
        "continue_on_failure": continue_on_failure,
        "force_upload": force_upload,
        "prefer_download": prefer_download,
        "subset": subset,
    }
    
    return invoke_cli_tool(
        etl_cli, steps, options, lambda result: _parse_etl_run_result(result, steps, dry_run)
    )


if __name__ == "__main__":
    mcp.run()
