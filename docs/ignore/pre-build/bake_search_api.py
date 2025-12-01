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

import yaml

from etl.git_api_helpers import GithubApiRepo

from .openapi_to_markdown import generate_markdown


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


def main():
    """Generate Search API documentation from OpenAPI spec."""
    # Paths relative to repository root
    repo_root = Path(__file__).parent.parent.parent.parent
    output_path = repo_root / "docs" / "api" / "search-api.md"

    print("Fetching OpenAPI spec from GitHub (owid/owid-grapher)...")
    spec = load_openapi_spec_from_github(
        org="owid", repo="owid-grapher", file_path="docs/search-api.openapi.yaml", branch="feat/chart-api-docs"
    )
    # spec = load_openapi_spec_from_file("/home/x/repos/owid-grapher/docs/search-api.openapi.yaml")

    print("Generating markdown documentation...")
    markdown = generate_markdown(spec)

    print(f"Writing documentation to {output_path}...")
    output_path.write_text(markdown)

    print("✓ Search API documentation generated successfully!")


if __name__ == "__main__":
    main()
