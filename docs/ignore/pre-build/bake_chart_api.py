#!/usr/bin/env python
"""
Generate Chart API documentation from OpenAPI specification and description file.

Called from: make docs.pre
Input: Fetched from owid/owid-grapher repository on GitHub (docs/chart-api.openapi.yaml and docs/chart-api.md)
Output: docs/api/chart-api.md
"""

from pathlib import Path

from .openapi_to_markdown import generate_markdown
from .openapi_utils import (
    get_current_branch,
    load_openapi_spec_from_github,
    load_text_from_github,
    resolve_parameter_refs,
    strip_frontmatter,
)


def main():
    repo_root = Path(__file__).parent.parent.parent.parent
    output_path = repo_root / "docs" / "api" / "chart-api.md"

    # Get current branch for logging
    current_branch = get_current_branch()
    print(f"Current branch: {current_branch}")

    print("Fetching OpenAPI spec from GitHub (owid/owid-grapher)...")
    spec = load_openapi_spec_from_github(
        org="owid",
        repo="owid-grapher",
        file_path="docs/chart-api.openapi.yaml",
    )

    print("Fetching description from GitHub (owid/owid-grapher)...")
    description = load_text_from_github(
        org="owid",
        repo="owid-grapher",
        file_path="docs/chart-api.md",
    )

    print("Resolving parameter references...")
    spec = resolve_parameter_refs(spec)

    print("Generating markdown documentation...")
    api_docs = generate_markdown(spec)

    print("Stripping frontmatter from description...")
    # Strip frontmatter from description (api_docs already has its own)
    description_body = strip_frontmatter(description)

    print(f"Writing documentation to {output_path}...")
    # Combine: api_docs (with frontmatter) + description_body
    full_docs = f"{api_docs}\n\n{description_body}"
    output_path.write_text(full_docs)

    print("✓ Chart API documentation generated successfully!")


if __name__ == "__main__":
    main()
