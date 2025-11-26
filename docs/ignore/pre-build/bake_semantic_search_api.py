#!/usr/bin/env python
"""
Generate Swagger-like markdown documentation from OpenAPI specification for Semantic Search API.

This script fetches the OpenAPI spec from the public semantic search API endpoint
and converts it into a beautiful, interactive-looking markdown file compatible
with Zensical/Material for MkDocs.

Called from: make docs.pre
Input: Fetched from https://search.owid.io/openapi.json
Output: docs/api/semantic-search-api.md
"""

import json
from pathlib import Path
from typing import Any, Dict

import requests

from openapi_to_markdown import generate_markdown


def load_openapi_spec_from_url(url: str = "https://search.owid.io/openapi.json") -> Dict[str, Any]:
    """Load OpenAPI specification from a public URL.

    Args:
        url: URL to the OpenAPI spec (JSON format)

    Returns:
        Parsed OpenAPI specification
    """
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def main():
    """Generate Semantic Search API documentation from OpenAPI spec."""
    # Paths relative to repository root
    repo_root = Path(__file__).parent.parent.parent.parent
    output_path = repo_root / "docs" / "api" / "semantic-search-api.md"

    print("Fetching OpenAPI spec from https://search.owid.io/openapi.json...")
    spec = load_openapi_spec_from_url()

    print("Generating markdown documentation...")
    markdown = generate_markdown(spec)

    print(f"Writing documentation to {output_path}...")
    output_path.write_text(markdown)

    print("✓ Semantic Search API documentation generated successfully!")


if __name__ == "__main__":
    main()
