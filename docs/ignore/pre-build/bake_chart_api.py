#!/usr/bin/env python
"""
Generate Chart API documentation from OpenAPI specification and description file.

Called from: make docs.pre
Input: Local files from ../owid-grapher/docs/ if available, otherwise from GitHub
Output: docs/api/chart-api.md
"""

from pathlib import Path

import yaml

from .openapi_to_markdown import generate_markdown
from .openapi_utils import (
    load_openapi_spec_from_github,
    load_text_from_github,
    resolve_parameter_refs,
    strip_frontmatter,
)


def main():
    repo_root = Path(__file__).parent.parent.parent.parent
    output_path = repo_root / "docs" / "api" / "chart-api.md"

    # Check if owid-grapher repo exists locally
    grapher_repo = repo_root.parent / "owid-grapher"
    openapi_local = grapher_repo / "docs" / "chart-api.openapi.yaml"
    description_local = grapher_repo / "docs" / "chart-api.md"

    if openapi_local.exists() and description_local.exists():
        print(f"Loading OpenAPI spec from local file: {openapi_local}")
        with open(openapi_local) as f:
            spec = yaml.safe_load(f)

        print(f"Loading description from local file: {description_local}")
        description = description_local.read_text()
    else:
        print("Local owid-grapher repo not found, fetching from GitHub...")
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
    # Combine: description_body + api_docs
    full_docs = f"{description_body}\n\n{api_docs}"
    output_path.write_text(full_docs)

    print("✓ Chart API documentation generated successfully!")


if __name__ == "__main__":
    main()
