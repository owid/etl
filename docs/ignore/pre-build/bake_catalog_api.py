#!/usr/bin/env python
"""Fetch documentation files from external sources (GitHub, etc.)

This script fetches documentation from external repositories and places them
in the docs directory. Run this before building docs with Zensical.
"""

from etl.paths import BASE_DIR, LIB_DIR

# Base directory for generated docs
DOCS_DIR = BASE_DIR / "docs"

# GitHub raw content base URL
GITHUB_RAW_BASE = "https://raw.githubusercontent.com"


def generate_catalog_api_docs():
    """Generate catalog API documentation"""
    output_path = DOCS_DIR / "api/catalog-api.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Load template
    with open(DOCS_DIR / "ignore/pre-build/catalog-api.template.md", "r") as f:
        template = f.read()

    # Load Python content from catalog README
    with open(LIB_DIR / "catalog/README.md", "r") as f:
        python_content = f.read()

    # Indent Python content by 4 spaces to fit inside the Python tab
    python_content = "\n".join("    " + line if line.strip() else "" for line in python_content.splitlines())

    # Format template with Python content
    final_content = template.format(python_content=python_content)

    with open(output_path, "w") as f:
        f.write(final_content)

    print(f"✓ Generated {output_path.relative_to(BASE_DIR)}")


############################################################
# MAIN
############################################################


def main():
    """Fetch all external documentation files"""
    print("Baking documentation...")
    print()

    generate_catalog_api_docs()

    print()
    print("✓ All documentation files baked successfully!")


if __name__ == "__main__":
    main()
