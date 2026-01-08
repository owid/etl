#!/usr/bin/env python
"""
Generate Chart API documentation from OpenAPI specification.

Called from: make docs.pre
Input: Local file from ../owid-grapher/docs/chart-api.openapi.yaml if available, otherwise from GitHub
Output: docs/api/chart-api.md
"""

from pathlib import Path
from typing import Any, Dict

import yaml

from .openapi_to_markdown import generate_markdown  # ty: ignore
from .openapi_utils import (  # ty:ignore
    load_openapi_spec_from_github,
    resolve_parameter_refs,
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


def main():
    repo_root = Path(__file__).parent.parent.parent.parent
    output_path = repo_root / "docs" / "api" / "chart-api.md"

    # Load OpenAPI spec (local or GitHub)
    spec = load_openapi_spec(repo_root)

    print("Resolving parameter references...")
    spec = resolve_parameter_refs(spec)

    print("Generating markdown documentation...")
    markdown = generate_markdown(spec)

    print(f"Writing documentation to {output_path}...")
    output_path.write_text(markdown)

    print("✓ Chart API documentation generated successfully!")


if __name__ == "__main__":
    main()
