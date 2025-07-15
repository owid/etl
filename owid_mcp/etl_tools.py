import re
import subprocess
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from click.testing import CliRunner
from fastmcp import FastMCP

from apps.pr.cli import cli as pr_cli
from apps.step_update.cli import cli as step_update_cli


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


# Create ETL Tools MCP server
etl_tools_mcp = FastMCP("ETL Tools")


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


@etl_tools_mcp.tool
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


@etl_tools_mcp.tool
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