from dataclasses import dataclass
from typing import List, Optional
import io
import sys
from contextlib import redirect_stdout, redirect_stderr

from fastmcp import FastMCP
from owid import catalog

from apps.pr.cli import cli as pr_cli


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


@mcp.tool
def create_pr(
    title: str,
    category: Optional[str] = None,
    scope: Optional[str] = None,
    work_branch: Optional[str] = None,
    base_branch: str = "master",
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
        base_branch: Name of the base branch to merge into (default: master)
        direct: Create PR from current branch instead of creating new branch
        private: Make staging server private
        no_llm: Disable LLM for branch name generation

    Returns:
        PRResult with success status, PR URL, branch name, and message
    """
    try:
        # Capture stdout and stderr to get any output or errors
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()
        
        # Build arguments for the CLI
        args = [title]
        if category:
            args.append(category)
        
        # Use Click's testing functionality to invoke the CLI
        from click.testing import CliRunner
        runner = CliRunner()
        
        # Build CLI arguments
        cli_args = args.copy()
        if scope:
            cli_args.extend(["--scope", scope])
        if work_branch:
            cli_args.extend(["--work-branch", work_branch])
        if base_branch != "master":
            cli_args.extend(["--base-branch", base_branch])
        if direct:
            cli_args.append("--direct")
        if private:
            cli_args.append("--private")
        if no_llm:
            cli_args.append("--no-llm")
        
        # Invoke the CLI
        result = runner.invoke(pr_cli, cli_args, catch_exceptions=False)
        
        if result.exit_code == 0:
            # Extract PR URL from output if available
            output_lines = result.output.split('\n')
            pr_url = None
            branch_name = work_branch
            
            for line in output_lines:
                if "html_url" in line or "github.com" in line:
                    # Try to extract URL from the output
                    import re
                    url_match = re.search(r'https://github\.com/[^\s]+', line)
                    if url_match:
                        pr_url = url_match.group()
                        break
            
            return PRResult(
                success=True,
                pr_url=pr_url,
                branch_name=branch_name,
                message="Pull request created successfully"
            )
        else:
            return PRResult(
                success=False,
                pr_url=None,
                branch_name=work_branch,
                message=f"CLI failed with exit code {result.exit_code}: {result.output}"
            )
            
    except Exception as e:
        return PRResult(
            success=False,
            pr_url=None,
            branch_name=work_branch,
            message=f"Error calling PR CLI: {str(e)}"
        )


if __name__ == "__main__":
    mcp.run()
