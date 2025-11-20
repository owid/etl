---
title: Demo Page - Zensical Features Showcase
tags:
  - Demo
  - Documentation
  - Examples
icon: lucide/popcorn
status: new
# hide:
#   - toc  # Remove this line to show table of contents
---

# Demo Page - Zensical Features Showcase

This page demonstrates the powerful authoring capabilities available in Zensical, built on Material for MkDocs. You'll find examples of rich formatting, interactive elements, and technical documentation features.

!!! tip "Navigation Note"
    This page has the table of contents (TOC) hidden via front matter. Remove `hide: - toc` from the YAML header to restore it.

## Links and References

### Basic Links

You can create [internal links](../index.md) to other pages or [external links](https://ourworldindata.org) to websites.

### Links with Tooltips

Material for MkDocs supports [hoverable tooltips](https://squidfunk.github.io/mkdocs-material/reference/tooltips/ "Click to learn more about tooltips!") using the title attribute.

You can also use abbreviations for inline tooltips. Hover over ETL below:

The ETL pipeline processes data through multiple stages.

*[ETL]: Extract, Transform, Load - Our data processing pipeline

### Links with preview
Check out [our API](../api/index.md){ data-preview }.

### Reference-style Links

Check out our [data API][api-ref] and [metadata system][metadata-ref].

[api-ref]: ../api/index.md
[metadata-ref]: ../architecture/metadata/index.md

## Page Status Indicators

The status indicator at the top of this page is set via front matter: `status: new`

Available status options:

- `new` - Recently added content
- `deprecated` - Outdated or superseded content

## Admonitions (Callouts)

### Standard Types

!!! note "Information Note"
    This is a note admonition with a custom title. Great for highlighting important information.

!!! abstract "Summary"
    Use abstract for summaries or TL;DR sections.

!!! info "Additional Context"
    Info admonitions provide supplementary details.

!!! tip "Pro Tip"
    Tips offer helpful suggestions and best practices.

!!! success "Success Story"
    Celebrate achievements and successful outcomes.

!!! question "FAQ"
    Use questions for frequently asked questions.

!!! warning "Caution"
    Warnings alert users to potential pitfalls.

!!! failure "Common Mistake"
    Highlight errors or things to avoid.

!!! danger "Critical Warning"
    Danger indicates serious risks or breaking changes.

!!! bug "Known Issue"
    Document known bugs or limitations.

!!! example "Code Example"
    Provide illustrative examples.

!!! quote "Citation"
    Quote sources or include testimonials.

### Collapsible Admonitions

??? note "Click to Expand"
    This admonition starts collapsed. Click the title to reveal the content.

    Perfect for optional details or advanced topics.

???+ tip "Starts Expanded"
    This collapsible admonition begins in the expanded state.

    Users can collapse it if they want to focus on other content.

### Inline Admonitions

!!! info inline end "Sidebar Info"
    This admonition floats to the right side of the page, allowing text to flow around it.

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla et euismod nulla. Curabitur feugiat, tortor non consequat finibus, justo purus auctor massa, nec semper lorem quam in massa. This text wraps around the inline admonition.

Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Vestibulum tortor quam, feugiat vitae, ultricies eget, tempor sit amet, ante.

## Buttons

You can create styled buttons for calls to action:

[Getting Started :material-rocket-launch:](../getting-started/index.md){ .md-button .md-button--primary }
[View on GitHub :fontawesome-brands-github:](https://github.com/owid/etl){ .md-button }
[API Documentation](../api/index.md){ .md-button }

## Code Blocks

### Basic Syntax Highlighting

```python
def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process and clean the input dataframe."""
    # Remove duplicates
    df = df.drop_duplicates()

    # Filter invalid values
    df = df[df["value"] > 0]

    return df
```

### With Title and Line Numbers

```python title="etl_pipeline.py" linenums="1"
from etl.helpers import PathFinder
from etl.data_helpers import geo

paths = PathFinder(__file__)

def run() -> None:
    # Load input dataset
    ds_input = paths.load_dataset("input_dataset")
    tb = ds_input["table_name"].reset_index()

    # Harmonize country names
    tb = geo.harmonize_countries(
        tb,
        countries_file=paths.country_mapping_path
    )

    # Format and save
    tb = tb.format(short_name=paths.short_name)
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
```

### With Highlighted Lines

```python hl_lines="2 3 5-7" linenums="1"
import pandas as pd
from etl.snapshot import Snapshot
from owid.catalog import Table

# Load snapshot data
snap = Snapshot("who/latest/health_data.csv")
df = snap.read_csv()

# Convert to catalog table
tb = Table(df)
```

### With Annotations

```python
def harmonize_countries(tb: Table) -> Table:
    """Standardize country names."""
    tb = geo.harmonize_countries(  # (1)!
        tb,
        countries_file=paths.country_mapping_path  # (2)!
    )
    return tb
```

1. The `geo.harmonize_countries()` function standardizes country names to OWID format.
2. The mapping file contains manual overrides for ambiguous country names.

### Multiple Languages

=== "Python"

    ```python
    def calculate_growth_rate(current, previous):
        """Calculate percentage growth rate."""
        return ((current - previous) / previous) * 100
    ```

=== "SQL"

    ```sql
    SELECT
        country,
        year,
        ((value - LAG(value) OVER (ORDER BY year)) / LAG(value) OVER (ORDER BY year)) * 100 AS growth_rate
    FROM
        indicators
    WHERE
        indicator_name = 'GDP'
    ORDER BY
        country, year;
    ```

=== "Bash"

    ```bash
    # Run ETL pipeline
    .venv/bin/etl run data://garden/who/2024/health_data

    # With force flag
    .venv/bin/etl run --force data://garden/who/2024/health_data
    ```

## Mermaid Diagrams

### Flow Chart

```mermaid
graph LR
    A[Snapshot] -->|Raw Data| B[Meadow]
    B -->|Cleaned| C[Garden]
    C -->|Harmonized| D[Grapher]
    D -->|Published| E[Visualization]

    style A fill:#f9f,stroke:#333
    style E fill:#9f9,stroke:#333
```

### Sequence Diagram

```mermaid
sequenceDiagram
    participant User
    participant ETL
    participant Database
    participant API

    User->>ETL: Request dataset update
    ETL->>Database: Check current version
    Database-->>ETL: Version info
    ETL->>ETL: Process new data
    ETL->>Database: Insert updated dataset
    Database-->>ETL: Success
    ETL->>API: Refresh cache
    API-->>User: Updated data available
```

### Pipeline Architecture

```mermaid
graph TB
    subgraph "Data Sources"
        S1[WHO]
        S2[World Bank]
        S3[UN]
    end

    subgraph "ETL Pipeline"
        Snap[Snapshot Layer]
        Mead[Meadow Layer]
        Gard[Garden Layer]
        Graf[Grapher Layer]
    end

    subgraph "Output"
        DB[(MySQL Database)]
        API[REST API]
        Charts[Interactive Charts]
    end

    S1 --> Snap
    S2 --> Snap
    S3 --> Snap
    Snap --> Mead
    Mead --> Gard
    Gard --> Graf
    Graf --> DB
    DB --> API
    DB --> Charts
```

## Tabs

### Content Tabs

=== "Overview"

    The ETL system is Our World in Data's content-addressable data pipeline with DAG-based execution. It processes global development data through multiple stages with rich metadata.

=== "Core Stages"

    - **Snapshot**: DVC-tracked raw files
    - **Meadow**: Basic cleaning and standardization
    - **Garden**: Business logic and harmonization
    - **Grapher**: MySQL database ingestion
    - **Export**: Final outputs and APIs

=== "Key Features"

    ✓ Content-based checksums for change detection
    ✓ Automatic dependency resolution
    ✓ Rich metadata at every level
    ✓ Reproducible data workflows
    ✓ Version control integration

## Tables

### Simple Table

| Stage | Purpose | Output Format |
|-------|---------|---------------|
| Snapshot | Raw data storage | CSV, Excel, JSON |
| Meadow | Initial cleaning | Feather |
| Garden | Business logic | Feather |
| Grapher | Database ready | MySQL tables |

### Advanced Table with Alignment

| Command | Description | Example |
|:--------|:------------|--------:|
| `etl run` | Execute ETL steps | *Common* |
| `etl harmonize` | Country name mapping | *Essential* |
| `etl diff` | Compare datasets | *Debug* |
| `etl graphviz` | Generate DAG visualization | *Planning* |

### Table with Links

| Documentation | Description |
|---------------|-------------|
| [Getting Started](../getting-started/index.md) | Installation and setup |
| [Guides](../guides/index.md) | How-to guides for common tasks |
| [API Reference](../api/index.md) | API documentation |
| [Metadata](../architecture/metadata/index.md) | Metadata system overview |

## Grids

<div class="grid cards" markdown>

-   :material-clock-fast:{ .lg .middle } __Quick Start__

    ---

    Install dependencies and run your first ETL step in under 10 minutes.

    [:octicons-arrow-right-24: Getting Started](../getting-started/index.md)

-   :material-book-open-variant:{ .lg .middle } __User Guides__

    ---

    Step-by-step guides for adding data, updating datasets, and managing charts.

    [:octicons-arrow-right-24: Browse Guides](../guides/index.md)

-   :material-chart-line:{ .lg .middle } __Data API__

    ---

    Programmatic access to OWID data via REST API and catalog system.

    [:octicons-arrow-right-24: API Docs](../api/index.md)

-   :material-cog:{ .lg .middle } __Architecture__

    ---

    Deep dive into ETL design principles, workflow, and data model.

    [:octicons-arrow-right-24: Design Docs](../architecture/index.md)

</div>

## Footnotes

The ETL pipeline[^1] processes data from multiple international organizations[^2] and makes it available through our API[^3].

[^1]: ETL stands for Extract, Transform, Load - the three core stages of data processing.

[^2]:
    Major data providers include:

    - World Health Organization (WHO)
    - World Bank
    - United Nations
    - OECD
    - National statistical agencies

[^3]: Our REST API provides programmatic access to all OWID datasets. See the [API documentation](../api/index.md) for details.

## Mathematical Notation

### Inline Math

The growth rate can be calculated as $r = \frac{P_t - P_{t-1}}{P_{t-1}} \times 100$, where $P_t$ is the current population.

### Block Math

$$
\text{Life Expectancy} = \frac{\sum_{i=1}^{n} \text{age}_i \times \text{deaths}_i}{\sum_{i=1}^{n} \text{deaths}_i}
$$

The GDP per capita growth rate:

$$
g = \left(\frac{GDP_{t}}{GDP_{t-1}}\right)^{\frac{1}{t-(t-1)}} - 1
$$

## Images

### Basic Image

![Our World in Data Logo](../assets/site-logo.svg)

### Image with Caption

<figure markdown>
  ![ETL Pipeline](../assets/site-logo.svg){ width="300" }
  <figcaption>The ETL processing pipeline at Our World in Data</figcaption>
</figure>

### Image Grid

<div class="grid" markdown>

![Logo](../assets/site-logo.svg){ width="200" }

![Logo](../assets/site-logo.svg){ width="200" }

![Logo](../assets/site-logo.svg){ width="200" }

</div>


## Keyboard Keys

Use ++ctrl+shift+r++ to hard refresh your browser and clear the favicon cache.

Common ETL commands:

- Run a step: ++ctrl+enter++
- Stop execution: ++ctrl+c++
- Search documentation: ++ctrl+k++ or ++cmd+k++

## Progress Bars

Track your ETL learning progress:

<progress value="75" max="100"></progress> 75% Complete

Dataset processing status:

<progress value="100" max="100"></progress> Snapshot ✓
<progress value="100" max="100"></progress> Meadow ✓
<progress value="60" max="100"></progress> Garden
<progress value="0" max="100"></progress> Grapher

## Icons

Material for MkDocs includes thousands of icons:

- :material-database: Database operations
- :material-chart-line: Data visualization
- :material-code-braces: Code development
- :fontawesome-brands-python: Python programming
- :fontawesome-brands-github: Version control
- :octicons-git-branch-16: Git branching
- :simple-mysql: MySQL database

## Horizontal Rules

Use horizontal rules to separate major sections:

---

## Summary

This demo page showcases the rich authoring capabilities available in Zensical:

✓ Interactive elements (buttons, tabs, collapsible sections)
✓ Rich formatting (admonitions, grids, cards)
✓ Technical content (code blocks, diagrams, math)
✓ Documentation features (tooltips, footnotes, cross-references)
✓ Visual elements (images, icons, progress bars)

!!! success "Ready to Start?"
    Explore the [Getting Started guide](../getting-started/index.md) to begin building your own documentation!
