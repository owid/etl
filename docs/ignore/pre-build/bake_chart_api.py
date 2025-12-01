#!/usr/bin/env python
"""
Generate Chart API documentation from OpenAPI specification and description file.

Called from: make docs.pre
Input: Local files from ../owid-grapher/docs/ if available, otherwise from GitHub
Output: docs/api/chart-api.md
"""

from pathlib import Path
from typing import Any, Dict

import yaml

from .openapi_to_markdown import generate_markdown
from .openapi_utils import (
    load_openapi_spec_from_github,
    load_text_from_github,
    resolve_parameter_refs,
    strip_frontmatter,
)


def load_openapi_spec(repo_root: Path) -> Dict[str, Any]:
    """Load OpenAPI spec from local file if available, otherwise from GitHub.

    Args:
        repo_root: Root directory of the etl repository

    Returns:
        OpenAPI specification as a dictionary
    """
    # Check if owid-grapher repo exists locally
    grapher_repo = repo_root.parent / "owid-grapher"
    openapi_local = grapher_repo / "docs" / "chart-api.openapi.yaml"

    if openapi_local.exists():
        print(f"Loading OpenAPI spec from local file: {openapi_local}")
        with open(openapi_local) as f:
            return yaml.safe_load(f)
    else:
        print("Local owid-grapher repo not found, fetching OpenAPI spec from GitHub...")
        return load_openapi_spec_from_github(
            org="owid",
            repo="owid-grapher",
            file_path="docs/chart-api.openapi.yaml",
        )


def load_description(repo_root: Path) -> str:
    """Load description from local file if available, otherwise from GitHub.

    Args:
        repo_root: Root directory of the etl repository

    Returns:
        Description text content
    """
    # Check if owid-grapher repo exists locally
    grapher_repo = repo_root.parent / "owid-grapher"
    description_local = grapher_repo / "docs" / "chart-api.md"

    if description_local.exists():
        print(f"Loading description from local file: {description_local}")
        return description_local.read_text()
    else:
        print("Local owid-grapher repo not found, fetching description from GitHub...")
        return load_text_from_github(
            org="owid",
            repo="owid-grapher",
            file_path="docs/chart-api.md",
        )


def main():
    repo_root = Path(__file__).parent.parent.parent.parent
    output_path = repo_root / "docs" / "api" / "chart-api.md"

    # Load OpenAPI spec and description (local or GitHub)
    spec = load_openapi_spec(repo_root)
    description = load_description(repo_root)

    print("Resolving parameter references...")
    spec = resolve_parameter_refs(spec)

    print("Generating markdown documentation...")
    api_docs = generate_markdown(spec)

    print("Stripping frontmatter from description...")
    # Strip frontmatter from description (api_docs already has its own)
    description_body = strip_frontmatter(description)

    print(f"Writing documentation to {output_path}...")
    # Combine: description_body + api_docs
    full_docs = f"{description_body}\n\n{api_docs}"
    output_path.write_text(full_docs)

    print("✓ Chart API documentation generated successfully!")


if __name__ == "__main__":
    main()
