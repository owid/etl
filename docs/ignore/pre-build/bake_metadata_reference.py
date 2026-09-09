#!/usr/bin/env python
"""Generate documentation files dynamically (standalone version for Zensical)

This script generates dynamic markdown files that were previously generated
by mkdocs-gen-files plugin. Run this before building docs with Zensical.
"""

from etl.docs import (
    render_chart,
    render_chart_view_config,
    render_chart_view_metadata,
    render_dataset,
    render_indicator,
    render_origin,
    render_table,
)
from etl.paths import BASE_DIR

# Base directory for generated docs
DOCS_DIR = BASE_DIR / "docs"

header_metadata = """---
tags:
  - Metadata
icon: material/api
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
# CHARTS (MULTIDIM)
############################################################

header_charts = """---
tags:
  - Charts
  - Multidim
  - Explorers
icon: material/api
---

# Charts reference

!!! warning "AI-Generated Documentation"
    This documentation was generated with AI assistance and is currently under construction. The content is dynamically generated from `schemas/multidim-schema.json`. If you notice any inconsistencies or missing information, please check the source schema file or report the issue.

Charts and multi-dimensional charts (MDIMs) are defined in ETL by a config that names their dimensions, views and metadata. This reference documents that schema.

<div class="grid cards" markdown>

- __[Chart](#chart)__ - Main chart configuration
- __[View Config](#viewconfig)__ - Chart and visualization configuration
- __[View Metadata](#viewmetadata)__ - Data presentation metadata

</div>

"""


def generate_charts_reference():
    """Generate charts reference"""
    output_path = DOCS_DIR / "architecture/metadata/reference/charts.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    text_chart = render_chart(level=2)
    text_view_config = render_chart_view_config(level=2)
    text_view_metadata = render_chart_view_metadata(level=2)
    text = header_charts + text_chart + text_view_config + text_view_metadata

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
    generate_charts_reference()

    print()
    print("✓ All dynamic documentation files generated successfully!")


if __name__ == "__main__":
    main()
