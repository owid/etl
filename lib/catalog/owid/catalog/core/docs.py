"""Human-readable documentation rendered from table and dataset metadata.

Everything here is derived from metadata already carried by tables (origins, titles, units,
descriptions) so that any dataset can produce a codebook, a sources table and a README without
hand-written text living in step code.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING, Any

import pandas as pd

from owid.catalog.core.jinja import _uses_jinja
from owid.catalog.core.meta import DatasetMeta, Origin, VariableMeta, description_key_to_string
from owid.catalog.core.utils import remove_details_on_demand

if TYPE_CHECKING:
    from owid.catalog.core.datasets import Dataset
    from owid.catalog.core.tables import Table

# Columns of the sources table, in order.
SOURCES_COLUMNS = [
    "label",
    "producer",
    "title",
    "description",
    "date_published",
    "date_accessed",
    "url_main",
    "url_download",
    "citation_full",
    "license_name",
    "license_url",
]

# Paragraph shared with the README of chart downloads on ourworldindata.org.
PROCESSING_NOTE = (
    "Our World in Data is almost never the original producer of the data. Almost all of the data we use has "
    "been compiled by others. If you want to reuse data, it is your responsibility to ensure that you adhere "
    "to the sources' license and to credit them correctly. Please note that a single time series may have "
    "more than one source, for example when we stitch together data from different time periods by different "
    "producers, or when we calculate per capita metrics using population data from a second source.\n\n"
    "Preparing this data involves several processing steps. Depending on the data, this can include "
    "standardizing country names and world region definitions, converting units, calculating derived "
    "indicators such as per capita measures, as well as adding or adapting metadata such as the name or the "
    "description given to an indicator.\n"
    "[Read about our data pipeline](https://docs.owid.io/projects/etl/)."
)


def origin_label(origin: Origin) -> str:
    """Short label naming an origin, e.g. "Energy Institute – Statistical Review of World Energy (2026)".

    Uses the origin's attribution when the producer set one, otherwise "Producer – Title (year)".
    """
    if origin.attribution:
        return origin.attribution
    label = origin.producer
    title = origin.title or origin.title_snapshot
    if title and title != origin.producer:
        label += f" – {title}"
    year = _year(origin.date_published)
    if year:
        label += f" ({year})"
    return label


def _year(value: Any) -> str | None:
    if not value:
        return None
    text = str(value)
    if text == "latest":
        return None
    return text.split("-")[0]


def _origin_key(origin: Origin) -> tuple[Any, ...]:
    """Fields that identify an origin once rendered; two origins that render identically are one source."""
    return (
        origin_label(origin),
        origin.producer,
        origin.title or origin.title_snapshot,
        origin.description or origin.description_snapshot,
        str(origin.date_published or ""),
        str(origin.date_accessed or ""),
        origin.url_main,
        origin.url_download,
        origin.citation_full,
        origin.license.name if origin.license else None,
        origin.license.url if origin.license else None,
    )


def unique_origins(origins_by_column: dict[str, list[Origin]]) -> list[Origin]:
    """Deduplicate origins across columns, most-referenced first, ties by first appearance."""
    origins: dict[tuple[Any, ...], Origin] = {}
    counts: dict[tuple[Any, ...], int] = {}
    for column_origins in origins_by_column.values():
        for origin in column_origins:
            key = _origin_key(origin)
            origins.setdefault(key, origin)
            counts[key] = counts.get(key, 0) + 1
    order = sorted(origins, key=lambda key: -counts[key])
    return [origins[key] for key in order]


def sources_frame(origins_by_column: dict[str, list[Origin]]) -> pd.DataFrame:
    """One row per unique origin, with the fields that document it."""
    rows = []
    for origin in unique_origins(origins_by_column):
        rows.append(
            {
                "label": origin_label(origin),
                "producer": origin.producer,
                "title": origin.title or origin.title_snapshot,
                "description": origin.description or origin.description_snapshot,
                "date_published": str(origin.date_published) if origin.date_published else None,
                "date_accessed": str(origin.date_accessed) if origin.date_accessed else None,
                "url_main": origin.url_main,
                "url_download": origin.url_download,
                "citation_full": origin.citation_full,
                "license_name": origin.license.name if origin.license else None,
                "license_url": origin.license.url if origin.license else None,
            }
        )
    return pd.DataFrame(rows, columns=SOURCES_COLUMNS)


def column_source_labels(origins: list[Origin]) -> str:
    """Labels of a column's origins, deduplicated and joined with "; "."""
    labels: list[str] = []
    for origin in origins:
        label = origin_label(origin)
        if label not in labels:
            labels.append(label)
    return "; ".join(labels)


def variable_title(name: str, meta: VariableMeta) -> str:
    """Best human title for a column, falling back to its name when the title is a Jinja template."""
    if meta.presentation and meta.presentation.title_public and not _uses_jinja(meta.presentation.title_public):
        return meta.presentation.title_public
    if meta.display and meta.display.get("name") and not _uses_jinja(meta.display["name"]):
        return meta.display["name"]
    if meta.title and not _uses_jinja(meta.title):
        return meta.title
    return name


def _clean_text(text: str | None) -> str | None:
    if not text or _uses_jinja(text):
        return None
    return remove_details_on_demand(text).strip()


def _unit(meta: VariableMeta) -> str:
    unit = meta.unit or ""
    if meta.short_unit and meta.short_unit != meta.unit:
        unit += f" ({meta.short_unit})"
    return unit


def _year_range(table: Table, column: str) -> str | None:
    """First and last year (or date) for which the column has data."""
    for time_column in ("year", "date"):
        if time_column not in table.all_columns:
            continue
        time_values = table.get_column_or_index(time_column)
        if column != time_column and column in table.all_columns:
            time_values = time_values[table.get_column_or_index(column).notna().to_numpy()]
        time_values = time_values.dropna()
        if len(time_values) == 0:
            return None
        low, high = time_values.min(), time_values.max()
        if time_column == "date":
            low, high = str(low)[:10], str(high)[:10]
        return f"{low}–{high}"
    return None


def _latest_date_published(origins: list[Origin]) -> str | None:
    dates = [str(origin.date_published) for origin in origins if origin.date_published]
    dates = [d for d in dates if d != "latest"]
    return max(dates) if dates else None


def _indicator_section(name: str, meta: VariableMeta, table: Table, level: int) -> str:
    heading = "#" * level
    lines = [f"{heading} {variable_title(name, meta)}", ""]
    description = _clean_text(meta.description_short)
    if description:
        lines += [description, ""]
    facts = [f"Column: `{name}`"]
    unit = _unit(meta)
    if unit:
        facts.append(f"Unit: {unit}")
    year_range = _year_range(table, name)
    if year_range:
        facts.append(f"Date range: {year_range}")
    last_updated = _latest_date_published(meta.origins)
    if last_updated:
        facts.append(f"Last updated: {last_updated}")
    if meta.origins:
        facts.append(f"Sources: {column_source_labels(meta.origins)}")
    lines += [f"{fact}  " for fact in facts]
    lines.append("")
    key = meta.description_key
    if isinstance(key, list):
        key = description_key_to_string(key)
    key = _clean_text(key)
    if key:
        lines += [f"{heading}# What you should know about this indicator", "", key, ""]
    processing = _clean_text(meta.description_processing)
    if processing:
        lines += [f"{heading}# Notes on our processing step for this indicator", "", processing, ""]
    return "\n".join(lines)


def _source_section(origin: Origin) -> str:
    lines = [f"### {origin_label(origin)}", ""]
    description = _clean_text(origin.description or origin.description_snapshot)
    if description:
        lines += [description, ""]
    facts = [f"Producer: {origin.producer}"]
    if origin.date_published:
        facts.append(f"Published: {origin.date_published}")
    if origin.date_accessed:
        facts.append(f"Retrieved on: {origin.date_accessed}")
    if origin.url_main:
        facts.append(f"Retrieved from: {origin.url_main}")
    if origin.url_download:
        facts.append(f"Download: {origin.url_download}")
    if origin.license and (origin.license.name or origin.license.url):
        license_text = origin.license.name or ""
        if origin.license.url:
            license_text += f" ({origin.license.url})" if license_text else origin.license.url
        facts.append(f"License: {license_text}")
    lines += [f"{fact}  " for fact in facts]
    lines.append("")
    citation = _clean_text(origin.citation_full)
    if citation:
        # Multi-line citations (several references) go on their own lines.
        lines += ["Citation:", ""] + citation.splitlines() + [""] if "\n" in citation else [f"Citation: {citation}", ""]
    return "\n".join(lines)


def ordered_table_names(dataset: Dataset) -> list[str]:
    """Table names with the dataset's main table (the one named after the dataset) first, the rest alphabetical."""
    names = sorted(dataset.table_names)
    main = dataset.metadata.short_name
    if main in names:
        names.remove(main)
        names.insert(0, main)
    return names


def dataset_title(meta: DatasetMeta, tables: list[Table]) -> str:
    if meta.title:
        return meta.title
    for table in tables:
        if table.metadata.title:
            return table.metadata.title
    return meta.short_name or "Dataset"


def render_readme(dataset: Dataset, tables: list[Table], url: str | None = None) -> str:
    """Markdown README for a dataset, built from its metadata and that of its tables."""
    meta = dataset.metadata
    title = dataset_title(meta, tables)
    parts = [f"# {title}", ""]
    if url:
        parts += [f"This file documents the dataset published at {url}.", ""]
    description = _clean_text(meta.description)
    if description:
        parts += [description, ""]

    parts += ["## How we process data at Our World in Data", "", PROCESSING_NOTE, ""]

    parts += ["## Detailed information about the data", ""]
    multi_table = len(tables) > 1
    origins_by_column: dict[str, list[Origin]] = {}
    for table in tables:
        level = 3
        if multi_table:
            table_title = table.metadata.title or table.metadata.short_name or "table"
            parts += [f"### {table_title}", ""]
            if table.metadata.short_name:
                parts += [f"Table: `{table.metadata.short_name}`", ""]
            level = 4
        for column in table.all_columns:
            column_meta = table.get_column_or_index(column).metadata
            parts.append(_indicator_section(column, column_meta, table, level))
            origins_by_column[f"{table.metadata.short_name}.{column}"] = list(column_meta.origins)

    origins = unique_origins(origins_by_column)
    if origins:
        parts += [
            "## Sources",
            "",
            "These are the sources behind the data in this dataset. Each indicator above names the ones it draws on.",
            "",
        ]
        parts += [_source_section(origin) for origin in origins]

    parts += ["## License", ""]
    if meta.licenses:
        for license in meta.licenses:
            text = license.name or ""
            if license.url:
                text += f" ({license.url})" if text else license.url
            parts.append(f"This dataset is published under {text}.")
        parts.append("")
    parts += [
        "The data is derived from the sources above, whose own licenses apply to the underlying data. Please "
        "credit them alongside Our World in Data.",
        "",
    ]

    parts += ["## How to cite this dataset", ""]
    year = _year(meta.version) or str(date.today().year)
    citation = f'Our World in Data ({year}). "{title}"'
    if origins:
        citation += f". Based on {'; '.join(origin_label(origin) for origin in origins)}"
    citation += "."
    if url:
        citation += f" Retrieved from {url}."
    parts += [citation, ""]
    return "\n".join(parts).rstrip() + "\n"
