#!/usr/bin/env python
"""Generate llms.txt and llms-full.txt for the owid-catalog library.

Extracts docstrings and signatures from the public API modules so the files
stay in sync as the code evolves. Outputs:
  - docs/llms.txt       concise overview with links
  - docs/llms-full.txt  comprehensive self-contained reference
"""

from __future__ import annotations

import inspect
import textwrap
from typing import Any

from etl.paths import BASE_DIR

DOCS_DIR = BASE_DIR / "docs"

# Base URL for linking to hosted docs
DOCS_BASE = "https://docs.owid.io/projects/etl"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_signature(obj: Any) -> str:
    """Return a clean signature string for *obj*, or '' on failure."""
    try:
        sig = inspect.signature(obj)
        return str(sig)
    except (ValueError, TypeError):
        return ""


def _get_docstring(obj: Any, *, max_lines: int = 0) -> str:
    """Return the dedented docstring of *obj*, optionally truncated."""
    doc = inspect.getdoc(obj) or ""
    if max_lines and doc:
        lines = doc.splitlines()
        if len(lines) > max_lines:
            doc = "\n".join(lines[:max_lines]) + "\n..."
    return doc


def _format_func(name: str, obj: Any, *, max_doc_lines: int = 0) -> str:
    """Format a function/method as a Markdown code block + docstring."""
    sig = _get_signature(obj)
    doc = _get_docstring(obj, max_lines=max_doc_lines)
    parts = [f"### `{name}{sig}`"]
    if doc:
        parts.append("")
        parts.append(doc)
    return "\n".join(parts)


# Pydantic inherited methods that clutter API reference
_PYDANTIC_SKIP = frozenset(
    {
        "copy",
        "dict",
        "json",
        "model_copy",
        "model_dump",
        "model_dump_json",
        "model_post_init",
        "model_rebuild",
        "model_validate",
        "model_validate_json",
        "model_validate_strings",
    }
)

# Pydantic inherited properties to skip
_PYDANTIC_PROP_SKIP = frozenset(
    {
        "model_extra",
        "model_fields_set",
        "model_computed_fields",
        "model_config",
        "model_fields",
    }
)


def _format_class(name: str, cls: type, *, include_methods: bool = True, max_doc_lines: int = 0) -> str:
    """Format a class with its docstring and public methods."""
    sig = _get_signature(cls)
    doc = _get_docstring(cls, max_lines=max_doc_lines)
    parts = [f"## `{name}{sig}`"]
    if doc:
        parts.append("")
        parts.append(doc)

    if include_methods:
        for method_name, method_obj in inspect.getmembers(cls, predicate=inspect.isfunction):
            if method_name.startswith("_") or method_name in _PYDANTIC_SKIP:
                continue
            parts.append("")
            msig = _get_signature(method_obj)
            mdoc = _get_docstring(method_obj, max_lines=max_doc_lines)
            parts.append(f"### `{name}.{method_name}{msig}`")
            if mdoc:
                parts.append("")
                parts.append(mdoc)

        # Also grab properties
        for attr_name in sorted(dir(cls)):
            if attr_name.startswith("_") or attr_name in _PYDANTIC_PROP_SKIP:
                continue
            attr = getattr(cls, attr_name, None)
            if isinstance(attr, property) and attr.fget:
                pdoc = _get_docstring(attr.fget, max_lines=max_doc_lines)
                if pdoc:
                    parts.append("")
                    parts.append(f"### `{name}.{attr_name}` (property)")
                    parts.append("")
                    parts.append(pdoc)

    return "\n".join(parts)


def _collect_public_functions(module: Any) -> list[tuple[str, Any]]:
    """Return (name, obj) pairs for public functions defined in *module*."""
    results = []
    for name, obj in inspect.getmembers(module, predicate=inspect.isfunction):
        if name.startswith("_"):
            continue
        # Only include functions actually defined in this module
        if getattr(obj, "__module__", None) == module.__name__:
            results.append((name, obj))
    return results


# ---------------------------------------------------------------------------
# Concise llms.txt
# ---------------------------------------------------------------------------


def generate_llms_txt() -> str:
    """Generate the concise llms.txt content."""
    return textwrap.dedent(f"""\
        # owid-catalog

        > Python library for accessing Our World in Data's published data.
        > Search, discover, and fetch charts, tables, and indicators.

        ## Quick Start

        - [Installation & Examples]({DOCS_BASE}/api/catalog-api/): `pip install owid-catalog`

        ## Convenience Functions

        - [search()]({DOCS_BASE}/api/catalog/api/#search): Search for charts, tables, or indicators
        - [fetch()]({DOCS_BASE}/api/catalog/api/#fetch): Fetch data by path or chart slug

        ## API Reference

        - [Client]({DOCS_BASE}/api/catalog/api/#owid.catalog.api.client.Client): Unified client with .charts, .tables, .indicators sub-APIs
        - [ChartsAPI]({DOCS_BASE}/api/catalog/api/#owid.catalog.api.charts.ChartsAPI): Search and fetch published chart data
        - [TablesAPI]({DOCS_BASE}/api/catalog/api/#owid.catalog.api.tables.TablesAPI): Query catalog tables and datasets
        - [IndicatorsAPI]({DOCS_BASE}/api/catalog/api/#owid.catalog.api.indicators.IndicatorsAPI): Semantic search for indicators

        ## Response Types

        - [ResponseSet]({DOCS_BASE}/api/catalog/api/#owid.catalog.api.models.ResponseSet): Iterable, indexable container for search results
        - [ChartResult]({DOCS_BASE}/api/catalog/api/#owid.catalog.api.charts.ChartResult): Chart search/fetch result with .fetch() method
        - [TableResult]({DOCS_BASE}/api/catalog/api/#owid.catalog.api.tables.TableResult): Table search result with .fetch() method
        - [IndicatorResult]({DOCS_BASE}/api/catalog/api/#owid.catalog.api.indicators.IndicatorResult): Indicator search result with .fetch() method

        ## Data Structures

        - [Table]({DOCS_BASE}/api/catalog/structures/#owid.catalog.Table): Enhanced pandas DataFrame with metadata
        - [Dataset]({DOCS_BASE}/api/catalog/structures/#owid.catalog.Dataset): Container for multiple tables

        ## Optional

        - [REST APIs]({DOCS_BASE}/api/): Raw HTTP API documentation
        - [Full reference (llms-full.txt)]({DOCS_BASE}/llms-full.txt): Complete API documentation for agents
    """)


# ---------------------------------------------------------------------------
# Comprehensive llms-full.txt
# ---------------------------------------------------------------------------


def generate_llms_full_txt() -> str:
    """Generate the comprehensive llms-full.txt content."""
    # Late imports so the script works even if owid-catalog isn't importable
    # at module level (e.g. during a docs build in a minimal env).
    from owid.catalog.api.charts import ChartResult, ChartsAPI
    from owid.catalog.api.client import Client
    from owid.catalog.api.indicators import IndicatorResult, IndicatorsAPI
    from owid.catalog.api.models import ResponseSet
    from owid.catalog.api.quick import fetch, search
    from owid.catalog.api.tables import TableResult, TablesAPI
    from owid.catalog.core import tables as tables_module
    from owid.catalog.core.datasets import Dataset
    from owid.catalog.core.tables import Table

    sections: list[str] = []

    # -- Header --
    sections.append(
        textwrap.dedent("""\
        # owid-catalog — Complete API Reference

        > Python library for accessing Our World in Data's published data.
        > Search, discover, and fetch charts, tables, and indicators.

        ## Installation

        ```bash
        pip install owid-catalog
        ```
    """)
    )

    # -- Quick-start examples --
    sections.append(
        textwrap.dedent("""\
        ## Quick Start

        ```python
        from owid.catalog import search, fetch

        # Search for charts (default)
        charts = search("population")
        tb = charts[0].fetch()

        # Fetch chart data by slug
        tb = fetch("life-expectancy")

        # Search for tables
        tables = search("population", kind="table", namespace="un")
        tb = tables[0].fetch()

        # Search indicators (semantic search)
        indicators = search("renewable energy", kind="indicator")

        # Use Client for more control
        from owid.catalog import Client
        client = Client()
        results = client.charts.search("CO2 emissions")
        tb = client.charts.fetch("co2-emissions")
        ```
    """)
    )

    # -- Convenience functions --
    sections.append("## Convenience Functions\n")
    sections.append(_format_func("search", search))
    sections.append("")
    sections.append(_format_func("fetch", fetch))
    sections.append("")

    # -- Client --
    sections.append("## Client\n")
    sections.append(_format_class("Client", Client, include_methods=False))
    sections.append("")
    sections.append(
        textwrap.dedent("""\
        The Client exposes three sub-API objects:

        - `client.charts` — ChartsAPI instance
        - `client.tables` — TablesAPI instance
        - `client.indicators` — IndicatorsAPI instance
    """)
    )

    # -- ChartsAPI --
    sections.append("## ChartsAPI\n")
    sections.append(_format_class("ChartsAPI", ChartsAPI))
    sections.append("")

    # -- TablesAPI --
    sections.append("## TablesAPI\n")
    sections.append(_format_class("TablesAPI", TablesAPI))
    sections.append("")

    # -- IndicatorsAPI --
    sections.append("## IndicatorsAPI\n")
    sections.append(_format_class("IndicatorsAPI", IndicatorsAPI))
    sections.append("")

    # -- ResponseSet --
    sections.append("## ResponseSet\n")
    sections.append(_format_class("ResponseSet", ResponseSet))
    sections.append("")

    # -- Result types --
    sections.append("## Result Types\n")
    for name, cls in [("ChartResult", ChartResult), ("TableResult", TableResult), ("IndicatorResult", IndicatorResult)]:
        sections.append(_format_class(name, cls, include_methods=True))
        # Show fields from the Pydantic model
        if hasattr(cls, "model_fields"):
            fields = []
            for field_name, field_info in cls.model_fields.items():
                annotation = field_info.annotation
                ann_str = getattr(annotation, "__name__", str(annotation))
                default = field_info.default
                if default is not None and str(default) != "PydanticUndefined":
                    fields.append(f"  - `{field_name}`: {ann_str} = {default!r}")
                else:
                    fields.append(f"  - `{field_name}`: {ann_str}")
            if fields:
                sections.append("")
                sections.append("**Fields:**\n")
                sections.append("\n".join(fields))
        sections.append("")

    # -- Table --
    sections.append("## Table\n")
    sections.append(
        textwrap.dedent("""\
        `Table` is a pandas DataFrame subclass with metadata support.
        Import: `from owid.catalog import Table`
    """)
    )
    # Key methods only — the full class is huge
    key_table_methods = [
        "format",
        "underscore",
        "to",
        "read",
        "to_csv",
        "to_feather",
        "to_parquet",
        "read_csv",
        "read_feather",
        "read_parquet",
        "copy_metadata",
        "update_metadata",
    ]
    for method_name in key_table_methods:
        method = getattr(Table, method_name, None)
        if method is not None:
            sig = _get_signature(method)
            doc = _get_docstring(method, max_lines=5)
            sections.append(f"### `Table.{method_name}{sig}`\n")
            if doc:
                sections.append(doc)
            sections.append("")

    # Key properties
    for prop_name in ["m", "primary_key", "all_columns", "codebook"]:
        attr = getattr(Table, prop_name, None)
        if isinstance(attr, property) and attr.fget:
            doc = _get_docstring(attr.fget, max_lines=3)
            if doc:
                sections.append(f"### `Table.{prop_name}` (property)\n")
                sections.append(doc)
                sections.append("")

    # -- Dataset --
    sections.append("## Dataset\n")
    sections.append(_format_class("Dataset", Dataset, include_methods=True, max_doc_lines=5))
    sections.append("")

    # -- Processing module (top-level functions) --
    sections.append("## Processing Functions\n")
    sections.append(
        textwrap.dedent("""\
        Metadata-preserving replacements for pandas operations.
        Import: `from owid.catalog import processing as pr`
    """)
    )
    processing_funcs = [
        "concat",
        "merge",
        "melt",
        "pivot",
        "multi_merge",
        "read_csv",
        "read_excel",
        "read_feather",
        "read_parquet",
        "read_json",
        "read_fwf",
        "read_stata",
        "read_rds",
        "read_rda",
        "read_from_df",
        "read_from_dict",
        "read",
    ]
    for func_name in processing_funcs:
        func = getattr(tables_module, func_name, None)
        if func is not None:
            sig = _get_signature(func)
            doc = _get_docstring(func, max_lines=3)
            sections.append(f"### `pr.{func_name}{sig}`\n")
            if doc:
                sections.append(doc)
            sections.append("")

    # -- Common patterns --
    sections.append(
        textwrap.dedent("""\
        ## Common Patterns

        ### Searching and fetching data

        ```python
        from owid.catalog import search, fetch

        # Search returns a ResponseSet you can iterate, slice, or convert
        results = search("GDP per capita")
        print(results)           # shows summary
        print(len(results))      # number of results
        first = results[0]       # index into results
        df = results.to_frame()  # convert to pandas DataFrame

        # Each result has a .fetch() method to get the actual data
        tb = results[0].fetch()
        ```

        ### Working with tables

        ```python
        from owid.catalog import Table
        from owid.catalog import processing as pr

        # Tables preserve metadata through operations
        tb_filtered = tb[tb["year"] > 2000]           # keeps metadata
        tb_merged = pr.merge(tb1, tb2, on="country")  # merges metadata
        tb_concat = pr.concat([tb1, tb2])              # combines metadata

        # Format for OWID pipeline (set index, underscore columns, sort)
        tb = tb.format(["country", "year"])
        ```

        ### Using the Client

        ```python
        from owid.catalog import Client

        client = Client()

        # Charts API — search by topic, fetch by slug
        results = client.charts.search("life expectancy", limit=5)
        tb = client.charts.fetch("life-expectancy")

        # Tables API — search catalog by namespace, version, etc.
        results = client.tables.search(namespace="un", dataset="igme")
        tb = results.latest().fetch()

        # Indicators API — semantic search
        results = client.indicators.search("child mortality rate")
        tb = results[0].fetch()        # single indicator column
        tb = results[0].fetch_table()  # full parent table
        ```

        ### ResponseSet operations

        ```python
        results = search("energy")
        results.filter(lambda r: "solar" in r.title.lower())  # filter
        results.sort_by("popularity", reverse=True)            # sort
        latest = results.latest()                               # latest version
        ```
    """)
    )

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    # Concise version
    llms_path = DOCS_DIR / "llms.txt"
    llms_path.write_text(generate_llms_txt())
    print(f"✓ Generated {llms_path.relative_to(BASE_DIR)}")

    # Full version
    llms_full_path = DOCS_DIR / "llms-full.txt"
    llms_full_path.write_text(generate_llms_full_txt())
    print(f"✓ Generated {llms_full_path.relative_to(BASE_DIR)}")


if __name__ == "__main__":
    main()
