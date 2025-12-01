#!/usr/bin/env python
"""
Generate Chart API documentation from OpenAPI specification and description file.

Called from: make docs.pre
Input: Fetched from owid/owid-grapher repository on GitHub (docs/chart-api.openapi.yaml and docs/chart-api.md)
Output: docs/api/chart-api.md
"""

from pathlib import Path
import yaml
from .openapi_utils import (
    load_openapi_spec_from_github,
    load_text_from_github,
    get_current_branch,
    resolve_parameter_refs,
    extract_frontmatter,
    strip_frontmatter,
)
from .openapi_to_markdown import generate_markdown


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

    print("Extracting frontmatter...")
    # Extract frontmatter from description
    desc_frontmatter, desc_body = extract_frontmatter(description)
    
    # Strip frontmatter from generated API docs
    api_docs_body = strip_frontmatter(api_docs)

    print(f"Writing documentation to {output_path}...")
    # Reconstruct with proper frontmatter
    frontmatter_yaml = yaml.dump(desc_frontmatter, default_flow_style=False, sort_keys=False)
    full_docs = f"---\n{frontmatter_yaml}---\n\n{desc_body}\n\n{api_docs_body}"
    output_path.write_text(full_docs)

    print("✓ Chart API documentation generated successfully!")


if __name__ == "__main__":
    main()
