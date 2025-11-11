#!/usr/bin/env python
"""Generate documentation files dynamically (standalone version for Zensical)

This script generates dynamic markdown files that were previously generated
by mkdocs-gen-files plugin. Run this before building docs with Zensical.
"""

from etl.docs import (
    render_collection,
    render_collection_view_config,
    render_collection_view_metadata,
    render_dataset,
    render_indicator,
    render_origin,
    render_table,
)
from etl.paths import BASE_DIR, LIB_DIR

# Base directory for generated docs
DOCS_DIR = BASE_DIR / "docs"

header_metadata = """---
tags:
  - Metadata
---

# Metadata reference

<div class="grid cards" markdown>

- __[Indicator](#variable)__ (variable)
- __[Origin](#origin)__
- __[Table](#table)__
- __[Dataset](#dataset)__
</div>

"""

############################################################
# METADATA
############################################################


def generate_metadata_reference():
    """Generate combined metadata reference"""
    output_path = DOCS_DIR / "architecture/metadata/reference/index.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    text_origin = render_origin(level=2)
    text_dataset = render_dataset(level=2)
    text_table = render_table(level=2)
    text_indicator = render_indicator(level=2)
    text = header_metadata + text_indicator + text_origin + text_table + text_dataset

    with open(output_path, "w") as f:
        f.write(text)

    print(f"✓ Generated {output_path.relative_to(BASE_DIR)}")


############################################################
# owid-catalog
############################################################


def generate_catalog_api_docs():
    """Generate catalog API documentation"""
    output_path = DOCS_DIR / "api/example-usage.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Load catalog README
    with open(LIB_DIR / "catalog/README.md", "r") as f:
        docs_catalog = f.read()

    with open(output_path, "w") as f:
        f.write(docs_catalog)

    print(f"✓ Generated {output_path.relative_to(BASE_DIR)}")


############################################################
# COLLECTIONS (MULTIDIM)
############################################################

header_collections = """---
tags:
  - Collections
  - Multidim
  - Explorers
---

# Collections reference

!!! warning "AI-Generated Documentation"
    This documentation was generated with AI assistance and is currently under construction. The content is dynamically generated from `schemas/multidim-schema.json`. If you notice any inconsistencies or missing information, please check the source schema file or report the issue.

Multi-dimensional collections (MDIMs) are interactive data explorers that allow users to explore datasets across multiple dimensions. This reference documents the schema structure for defining collections.

<div class="grid cards" markdown>

- __[Collection](#collection)__ - Main collection configuration
- __[View Config](#viewconfig)__ - Chart and visualization configuration
- __[View Metadata](#viewmetadata)__ - Data presentation metadata

</div>

"""


def generate_collections_reference():
    """Generate collections reference"""
    output_path = DOCS_DIR / "architecture/metadata/reference/collections.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    text_collection = render_collection(level=2)
    text_view_config = render_collection_view_config(level=2)
    text_view_metadata = render_collection_view_metadata(level=2)
    text = header_collections + text_collection + text_view_config + text_view_metadata

    with open(output_path, "w") as f:
        f.write(text)

    print(f"✓ Generated {output_path.relative_to(BASE_DIR)}")


############################################################
# MAIN
############################################################


def main():
    """Generate all dynamic documentation files"""
    print("Generating dynamic documentation files...")
    print()

    generate_metadata_reference()
    generate_catalog_api_docs()
    generate_collections_reference()

    print()
    print("✓ All dynamic documentation files generated successfully!")


if __name__ == "__main__":
    main()
