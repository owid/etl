#!/usr/bin/env python
"""Generate llms.txt for the owid-catalog library.

Dynamically extracts content from:
- Module/class/function docstrings (via ``inspect``)
- Documentation markdown files (``docs/libraries/catalog/``)
- Public API surface (``owid.catalog.api`` and ``owid.catalog.core``)

No hardcoded examples or imports — everything is derived from the codebase
and doc files so the output stays in sync automatically.

Run via: make docs.llms
Output: docs/llms.txt
"""

from __future__ import annotations

import importlib
import inspect
import re
import textwrap
from pathlib import Path
from types import ModuleType

from etl.paths import BASE_DIR

DOCS_DIR = BASE_DIR / "docs"
CATALOG_DOCS_DIR = DOCS_DIR / "libraries" / "catalog"

# Modules to inspect — grouped by section.
API_MODULES = [
    "owid.catalog.api.quick",
    "owid.catalog.api.client",
    "owid.catalog.api.charts",
    "owid.catalog.api.tables",
    "owid.catalog.api.indicators",
    "owid.catalog.api.models",
]

# Core modules — key user-facing data structures.
# For modules without __all__, we whitelist the important public names
# so the output stays focused on what users actually need.
CORE_MODULES = [
    "owid.catalog.core.tables",
    "owid.catalog.core.indicators",
    "owid.catalog.core.datasets",
    "owid.catalog.core.meta",
    "owid.catalog.core.processing",
]

# Whitelist of names to include from core modules that lack __all__.
# This prevents dumping dozens of internal helpers into the llms.txt.
CORE_WHITELIST: dict[str, list[str] | None] = {
    # None means "use __all__ or all public names"
    "owid.catalog.core.processing": None,
    # Explicit whitelists for modules without __all__
    "owid.catalog.core.tables": [
        "Table",
        "merge",
        "concat",
        "melt",
        "pivot",
        "read_csv",
        "read_feather",
        "read_excel",
        "read_parquet",
        "read_from_df",
        "read_from_dict",
        "multi_merge",
        "keep_metadata",
        "copy_metadata",
        "ExcelFile",
    ],
    "owid.catalog.core.indicators": [
        "Indicator",
        "Variable",
        "copy_metadata",
    ],
    "owid.catalog.core.datasets": [
        "Dataset",
        "FileFormat",
        "SUPPORTED_FORMATS",
        "PREFERRED_FORMAT",
        "CHANNEL",
    ],
    "owid.catalog.core.meta": [
        "MetaBase",
        "License",
        "Source",
        "Origin",
        "FaqLink",
        "VariablePresentationMeta",
        "VariableMeta",
        "DatasetMeta",
        "TableDimension",
        "TableMeta",
        "to_html",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _strip_mkdocs_directives(text: str) -> str:
    """Remove MkDocs-specific directives (admonitions, ::: blocks, YAML front matter)."""
    lines = text.splitlines()
    out: list[str] = []
    in_frontmatter = False
    in_directive = False
    in_admonition = False

    for i, line in enumerate(lines):
        # YAML front matter
        if i == 0 and line.strip() == "---":
            in_frontmatter = True
            continue
        if in_frontmatter:
            if line.strip() == "---":
                in_frontmatter = False
            continue

        # MkDocs ::: directives (API reference blocks)
        if line.strip().startswith(":::"):
            in_directive = True
            continue
        if in_directive:
            # Directive blocks end at the next non-indented, non-empty line
            if line.strip() and not line.startswith("    "):
                in_directive = False
            else:
                continue

        # Admonitions (!!! type "title") and their indented body
        if line.strip().startswith("!!!"):
            in_admonition = True
            continue
        if in_admonition:
            if line.startswith("    ") or not line.strip():
                continue
            in_admonition = False

        out.append(line)

    # Clean up MkDocs icon shortcodes like :fontawesome-brands-github:
    result = "\n".join(out).strip()
    result = re.sub(r":[\w-]+:", "", result)
    return result


def _strip_markdown_links(text: str) -> str:
    """Convert markdown links to plain text (keep the link text, drop the URL)."""
    return re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)


def _read_doc_file(path: Path) -> str:
    """Read a markdown doc file and strip MkDocs directives and links."""
    if not path.exists():
        return ""
    text = path.read_text()
    text = _strip_mkdocs_directives(text)
    text = _strip_markdown_links(text)
    return text


def _get_signature(obj: object) -> str:
    """Return a clean signature string."""
    name = getattr(obj, "__name__", str(obj))
    try:
        sig = inspect.signature(obj)
        return f"{name}{sig}"
    except (ValueError, TypeError):
        return name


def _get_docstring(obj: object) -> str:
    """Return cleaned docstring."""
    doc = inspect.getdoc(obj) or ""
    doc = textwrap.dedent(doc).strip()
    # Strip markdown links that reference MkDocs-relative paths
    doc = _strip_markdown_links(doc)
    return doc


def _get_module_docstring(mod: ModuleType) -> str:
    """Return the module-level docstring."""
    return _get_docstring(mod)


def _discover_modules(
    module_paths: list[str],
    whitelist: dict[str, list[str] | None] | None = None,
) -> dict[str, list[tuple[str, object]]]:
    """Discover public classes and functions from a list of modules.

    Args:
        module_paths: Dotted module paths to inspect.
        whitelist: Optional dict of module_path -> list of names to include.
            If None or if a module is not in the dict, uses __all__ or all public names.
            If a module maps to None, same fallback applies.
            If a module maps to a list, only those names are included.

    Returns:
        Dict of module_name -> [(name, obj), ...].
    """
    whitelist = whitelist or {}
    result: dict[str, list[tuple[str, object]]] = {}

    for mod_path in module_paths:
        mod = importlib.import_module(mod_path)
        members: list[tuple[str, object]] = []

        # Determine which names to inspect
        allowed = whitelist.get(mod_path)
        if allowed is not None:
            # Explicit whitelist for this module
            names = allowed
        else:
            # Use __all__ if available, otherwise public names
            names = getattr(mod, "__all__", None)
            if names is None:
                names = [n for n in dir(mod) if not n.startswith("_")]

        # If we have an explicit list (from __all__ or whitelist), trust it.
        # Otherwise, filter to objects defined in this module.
        has_explicit_list = allowed is not None or getattr(mod, "__all__", None) is not None

        for name in names:
            obj = getattr(mod, name, None)
            if obj is None:
                continue
            if has_explicit_list:
                # Trust the explicit list — include everything in it
                members.append((name, obj))
            else:
                # Only include classes and functions defined in this module
                obj_module = getattr(obj, "__module__", "")
                if obj_module == mod_path or (inspect.isclass(obj) and issubclass(obj, Exception)):
                    members.append((name, obj))

        if members:
            result[mod_path] = members

    return result


def _get_own_members(cls: type) -> list[str]:
    """Get names of public members defined on cls with custom behavior.

    Includes members that are either:
    - New (not present on any parent class), or
    - Overridden with a different docstring (custom OWID documentation)

    Excludes members that are simple wrappers with unchanged pandas docstrings.
    """
    result = []

    for name in sorted(dir(cls)):
        if name.startswith("_"):
            continue
        if name not in cls.__dict__:
            continue

        attr = getattr(cls, name, None)
        if attr is None:
            continue

        # Check if this is a new member (not on any parent)
        is_new = not any(hasattr(parent, name) for parent in cls.__mro__[1:])
        if is_new:
            result.append(name)
            continue

        # It's overridden — only include if docstring differs from parent
        doc = getattr(attr, "__doc__", "") or ""
        for parent in cls.__mro__[1:]:
            parent_attr = getattr(parent, name, None)
            if parent_attr is not None:
                parent_doc = getattr(parent_attr, "__doc__", "") or ""
                if doc != parent_doc and len(doc) > 10:
                    result.append(name)
                break

    return result


def _get_parent_info(cls: type) -> str:
    """Return a note about the parent class if it's a well-known type."""
    import pandas as pd

    for parent in cls.__mro__[1:]:
        if parent is pd.DataFrame:
            return (
                f"`{cls.__name__}` extends `pandas.DataFrame`. "
                "All standard DataFrame methods are available. "
                "Only methods unique to this class are listed below.\n"
            )
        if parent is pd.Series:
            return (
                f"`{cls.__name__}` extends `pandas.Series`. "
                "All standard Series methods are available. "
                "Only methods unique to this class are listed below.\n"
            )
    return ""


def _format_class(cls: type, *, methods: bool = True) -> str:
    """Format a class with docstring and optionally its public methods.

    Only includes methods with custom OWID docstrings, not thin wrappers
    with unchanged pandas documentation.
    """
    lines: list[str] = []
    lines.append(f"### {cls.__name__}")
    lines.append("")

    # Note about parent class (e.g. "extends pandas.DataFrame")
    parent_info = _get_parent_info(cls)
    if parent_info:
        lines.append(parent_info)
        lines.append("")

    doc = _get_docstring(cls)
    if doc:
        lines.append(doc)
        lines.append("")

    if methods:
        own_members = _get_own_members(cls)
        for name in own_members:
            attr = getattr(cls, name, None)
            if attr is None:
                continue

            if isinstance(attr, property):
                prop_doc = _get_docstring(attr.fget) if attr.fget else ""
                lines.append(f"#### `{cls.__name__}.{name}` (property)")
                lines.append("")
                if prop_doc:
                    lines.append(prop_doc)
                    lines.append("")
            elif callable(attr) and not isinstance(attr, type):
                sig = _get_signature(attr)
                method_doc = _get_docstring(attr)
                lines.append(f"#### `{sig}`")
                lines.append("")
                if method_doc:
                    lines.append(method_doc)
                    lines.append("")

    return "\n".join(lines)


def _format_function(func: object) -> str:
    """Format a standalone function. Returns empty string for undocumented wrappers."""
    name = getattr(func, "__name__", "")
    doc = _get_docstring(func)
    # Skip decorated functions that lost their name (missing @functools.wraps)
    if name == "wrapper" and not doc:
        return ""
    sig = _get_signature(func)
    lines = [f"### `{sig}`", ""]
    if doc:
        lines.append(doc)
        lines.append("")
    return "\n".join(lines)


def _format_exception(cls: type) -> str:
    """Format an exception class (brief)."""
    doc = _get_docstring(cls)
    desc = doc.split("\n")[0] if doc else ""
    return f"- **`{cls.__name__}`**: {desc}" if desc else f"- **`{cls.__name__}`**"


def _format_module_section(
    modules: dict[str, list[tuple[str, object]]],
    section_title: str,
) -> list[str]:
    """Format a group of modules into a section."""
    lines: list[str] = []
    lines.append(f"# {section_title}")
    lines.append("")

    for mod_path, members in modules.items():
        mod = importlib.import_module(mod_path)
        mod_doc = _get_module_docstring(mod)

        short_name = mod_path.rsplit(".", 1)[-1]
        lines.append(f"## {short_name}")
        lines.append("")
        if mod_doc:
            first_para = mod_doc.split("\n\n")[0]
            lines.append(first_para)
            lines.append("")

        for _name, obj in members:
            if inspect.isclass(obj):
                if issubclass(obj, Exception):
                    lines.append(_format_exception(obj))
                else:
                    lines.append(_format_class(obj))
            elif callable(obj):
                lines.append(_format_function(obj))
            lines.append("")

    return lines


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


def generate_llms_txt() -> str:
    """Generate llms.txt.

    Content sourced from:
    - docs/libraries/catalog/ markdown files (intro, api, structures, etc.)
    - owid.catalog.api module docstrings (auto-discovered)
    - owid.catalog.core module docstrings (auto-discovered)
    """
    lines: list[str] = []

    # Header
    lines.append("# owid-catalog")
    lines.append("")
    lines.append("> Python library for accessing Our World in Data's catalog of research data.")
    lines.append("")

    # Documentation content from markdown files
    # Start with intro, then remaining docs in alphabetical order
    doc_order = ["intro.md"]
    for md_file in sorted(CATALOG_DOCS_DIR.glob("*.md")):
        if md_file.name not in ("index.md", "intro.md"):
            doc_order.append(md_file.name)

    for md_name in doc_order:
        content = _read_doc_file(CATALOG_DOCS_DIR / md_name)
        if content:
            lines.append(content)
            lines.append("")
            lines.append("---")
            lines.append("")

    # API reference from docstrings
    api_modules = _discover_modules(API_MODULES)
    lines.extend(_format_module_section(api_modules, "API Reference (owid.catalog.api)"))
    lines.append("---")
    lines.append("")

    # Core reference from docstrings
    core_modules = _discover_modules(CORE_MODULES, whitelist=CORE_WHITELIST)
    lines.extend(_format_module_section(core_modules, "Core Reference (owid.catalog.core)"))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Generate llms.txt in docs/."""
    llms_txt = generate_llms_txt()

    out_path = DOCS_DIR / "llms.txt"
    out_path.write_text(llms_txt)
    print(f"Generated {out_path} ({len(llms_txt)} chars)")


if __name__ == "__main__":
    main()
