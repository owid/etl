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
from .openapi_utils import load_openapi_spec_from_github, get_current_branch
from .openapi_to_markdown import generate_markdown


def main():
    """Generate Search API documentation from OpenAPI spec."""
    repo_root = Path(__file__).parent.parent.parent.parent
    output_path = repo_root / "docs" / "api" / "search-api.md"

    # Get current branch for logging
    current_branch = get_current_branch()
    print(f"Current branch: {current_branch}")

    print("Fetching OpenAPI spec from GitHub (owid/owid-grapher)...")
    spec = load_openapi_spec_from_github(
        org="owid",
        repo="owid-grapher",
        file_path="docs/search-api.openapi.yaml",
    )

    print("Generating markdown documentation...")
    markdown = generate_markdown(spec)

    print(f"Writing documentation to {output_path}...")
    output_path.write_text(markdown)

    print("✓ Search API documentation generated successfully!")


if __name__ == "__main__":
    main()
