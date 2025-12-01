#!/usr/bin/env python
"""
Generate Swagger-like markdown documentation from OpenAPI specification.

This script converts an OpenAPI YAML spec into a beautiful, interactive-looking
markdown file compatible with Zensical/Material for MkDocs.

Called from: make docs.pre
Input: Fetched from owid/owid-grapher repository on GitHub (docs/search-api.openapi.yaml)
Output: docs/api/search-api.md
"""

from pathlib import Path
from typing import Any, Dict
import subprocess

import yaml

from etl.git_api_helpers import GithubApiRepo

from .openapi_to_markdown import generate_markdown


def get_current_branch() -> str:
    """Get the current git branch name."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return "master"


def load_openapi_spec_from_file(file_path: str) -> Dict[str, Any]:
    """Load OpenAPI specification from a local YAML file.

    Use this one for testing.

    Args:
        file_path: Path to the OpenAPI spec file
    Returns:
        Parsed OpenAPI specification
    """
    with open(file_path, "r") as f:
        return yaml.safe_load(f)  # type: ignore[return-value]


def load_openapi_spec_from_github(
    org: str = "owid",
    repo: str = "owid-grapher",
    file_path: str = "docs/search-api.openapi.yaml",
    branch: str = "master",
) -> Dict[str, Any]:
    """Load OpenAPI specification from GitHub repository.

    Args:
        org: GitHub organization name
        repo: Repository name
        file_path: Path to the OpenAPI spec file in the repository
        branch: Branch to fetch from

    Returns:
        Parsed OpenAPI specification
    """
    github_repo = GithubApiRepo(org=org, repo_name=repo)
    content = github_repo.fetch_file_content(file_path, branch)
    return yaml.safe_load(content)


def load_with_fallback(org: str, repo: str, file_path: str, current_branch: str) -> Dict[str, Any]:
    """Try to load from current branch, fall back to master if it fails."""
    # Try current branch first (if it's not master)
    if current_branch != "master":
        try:
            print(f"  Trying branch '{current_branch}'...")
            return load_openapi_spec_from_github(org=org, repo=repo, file_path=file_path, branch=current_branch)
        except Exception as e:
            print(f"  Branch '{current_branch}' not found, falling back to master...")
    
    # Fall back to master
    return load_openapi_spec_from_github(org=org, repo=repo, file_path=file_path, branch="master")


def main():
    """Generate Search API documentation from OpenAPI spec."""
    # Paths relative to repository root
    repo_root = Path(__file__).parent.parent.parent.parent
    output_path = repo_root / "docs" / "api" / "search-api.md"

    # Get current branch
    current_branch = get_current_branch()
    print(f"Current branch: {current_branch}")

    print("Fetching OpenAPI spec from GitHub (owid/owid-grapher)...")
    spec = load_with_fallback(
        org="owid",
        repo="owid-grapher",
        file_path="docs/search-api.openapi.yaml",
        current_branch=current_branch,
    )
    # spec = load_openapi_spec_from_file("/home/x/repos/owid-grapher/docs/search-api.openapi.yaml")

    print("Generating markdown documentation...")
    markdown = generate_markdown(spec)

    print(f"Writing documentation to {output_path}...")
    output_path.write_text(markdown)

    print("✓ Search API documentation generated successfully!")


if __name__ == "__main__":
    main()
