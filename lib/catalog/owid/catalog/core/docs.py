"""Human-readable documentation rendered from table and dataset metadata.

Everything here is derived from metadata already carried by tables (origins, titles, units,
descriptions) so that any dataset can produce a codebook, a sources table and a README without
hand-written text living in step code.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from owid.catalog.core.jinja import _uses_jinja
from owid.catalog.core.meta import DatasetMeta, Origin, VariableMeta, description_key_to_string
from owid.catalog.core.utils import remove_details_on_demand

if TYPE_CHECKING:
    from owid.catalog.core.datasets import Dataset
    from owid.catalog.core.tables import Table

# Columns of the sources table, in order.
# Rows an Excel sheet can hold, header included; a longer table gets no workbook rather than a truncated one.
EXCEL_MAX_ROWS = 1_048_576
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
]

# Citations follow the same rules as the chart downloads on ourworldindata.org (owid-grapher,
# packages/@ourworldindata/utils/src/metadataHelpers.ts): attributions are the origin's attribution
# or "Producer (year)"; more than three of them are shortened to the first one "and other sources".
OWID_ATTRIBUTION = "Our World in Data"
MAX_ATTRIBUTIONS_IN_SHORT_CITATION = 3

# Where the code of every data step lives, followed by the step's channel, namespace and version.
ETL_STEPS_URL = "https://github.com/owid/etl/tree/master/etl/steps/data/"

LICENSE_NOTE = (
    "Our World in Data collects and republishes this data; it is not the original producer. The licenses of "
    "the original sources still apply; each source above links to the producer's site, where its terms are "
    "stated. It is your responsibility to check that your use of the data is permitted by them, and to credit "
    "the sources correctly."
)

PROCESSING_NOTE = (
    "Preparing this data involves several processing steps. Depending on the data, this can include "
    "standardizing country names and world region definitions, converting units, calculating derived "
    "indicators such as per capita measures, and adding or adapting metadata such as the name or the "
    "description given to an indicator. An indicator can therefore draw on more than one source, for example "
    "when we stitch together data from different periods by different producers, or when we calculate per "
    "capita measures using population data from a second source.\n\n"
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


def attribution_label(origin: Origin) -> str:
    """Attribution of an origin as grapher renders it: its attribution, or "Producer (year)"."""
    if origin.attribution:
        return origin.attribution
    name = origin.producer or origin.title or origin.url_main or ""
    year = _year(origin.date_published)
    return f"{name} ({year})" if year else name


def attribution_labels(origins: list[Origin]) -> list[str]:
    labels: list[str] = []
    for origin in origins:
        label = attribution_label(origin)
        if label and label not in labels:
            labels.append(label)
    return labels


def _processing_phrase(attributions: list[str], processing_level: str) -> str | None:
    if all(attribution.startswith(OWID_ATTRIBUTION) for attribution in attributions):
        return None
    if processing_level == "major":
        return f"with major processing by {OWID_ATTRIBUTION}"
    if processing_level == "minor":
        return f"with minor processing by {OWID_ATTRIBUTION}"
    return f"processed by {OWID_ATTRIBUTION}"


def citation_short(origins: list[Origin], processing_level: str = "major") -> str:
    """Short citation, e.g. "Energy Institute (2026) and other sources – with major processing by Our World in Data"."""
    attributions = attribution_labels(origins)
    if len(attributions) > MAX_ATTRIBUTIONS_IN_SHORT_CITATION:
        text = f"{attributions[0]} and other sources"
    else:
        text = "; ".join(attributions)
    phrase = _processing_phrase(attributions, processing_level)
    return f"{text} – {phrase}" if phrase else text


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


def description_key_text(meta: VariableMeta) -> str | None:
    """The "what you should know" text of a column as markdown, or None."""
    key = meta.description_key
    if isinstance(key, list):
        key = description_key_to_string(key)
    return _clean_text(key)


def unit_label(meta: VariableMeta) -> str:
    """The unit with its short form in brackets, e.g. "terawatt-hours (TWh)"."""
    unit = meta.unit or ""
    if meta.short_unit and meta.short_unit != meta.unit:
        unit += f" ({meta.short_unit})"
    return unit


def year_range(table: Table, column: str) -> str | None:
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


def _indicator_section(name: str, meta: VariableMeta, table: Table, level: int) -> str:
    heading = "#" * level
    lines = [f"{heading} {variable_title(name, meta)}", ""]
    description = _clean_text(meta.description_short)
    if description:
        lines += [description, ""]
    facts = [f"Column: `{name}`"]
    unit = unit_label(meta)
    if unit:
        facts.append(f"Unit: {unit}")
    date_range = year_range(table, name)
    if date_range:
        facts.append(f"Date range: {date_range}")
    if meta.origins:
        facts.append(f"Sources: {column_source_labels(meta.origins)}")
    lines += [f"{fact}  " for fact in facts]
    lines.append("")
    key = description_key_text(meta)
    if key:
        lines += [f"{heading}# What you should know about this indicator", "", key, ""]
    from_producer = _clean_text(meta.description_from_producer)
    if from_producer:
        lines += [f"{heading}# How is this data described by its producer?", "", from_producer, ""]
    processing = _clean_text(meta.description_processing)
    if processing:
        lines += [f"{heading}# Notes on our processing step for this indicator", "", processing, ""]
    return "\n".join(lines)


def _source_section(origin: Origin, level: int = 3) -> str:
    lines = [f"{'#' * level} {origin_label(origin)}", ""]
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


def table_citation(table: Table) -> str | None:
    """How to cite one table, in the short form used on the charts; None for a table without origins."""
    metas = [table.get_column_or_index(column).metadata for column in table.all_columns]
    origins = unique_origins({column: list(meta.origins) for column, meta in zip(table.all_columns, metas)})
    if not origins:
        return None
    level = "major" if any(meta.processing_level == "major" for meta in metas) else "minor"
    return citation_short(origins, processing_level=level)


def fits_in_excel(table: Table) -> bool:
    return len(table) + 1 <= EXCEL_MAX_ROWS


def render_readme(dataset: Dataset, tables: list[Table], url: str | None = None) -> str:
    """Markdown README for a dataset: the catalog page as a text file, with the same sections in the same order.

    "About this dataset" (the description, with its changelog), then "Data" with one block per table (how to cite
    it, its indicators, its sources), then the processing note, the license and the advanced download options.
    """
    meta = dataset.metadata
    title = dataset_title(meta, tables)
    parts = [f"# {title}", ""]
    description = _clean_text(meta.description)
    if description:
        parts += ["## About this dataset", "", description, ""]

    parts += ["## Data", ""]
    for table in tables:
        parts += [f"### {table.metadata.title or table.metadata.short_name or 'table'}", ""]
        if table.metadata.short_name:
            parts += [f"Table: `{table.metadata.short_name}`", ""]
        table_description = _clean_text(table.metadata.description)
        if table_description:
            parts += [table_description, ""]
        parts += [f"{len(table):,} rows × {len(list(table.all_columns))} columns.", ""]
        citation = table_citation(table)
        if citation:
            parts += ["#### How to cite this data", "", f"{citation}.", ""]
        parts += ["#### Indicators", ""]
        origins_by_column: dict[str, list[Origin]] = {}
        for column in table.all_columns:
            column_meta = table.get_column_or_index(column).metadata
            parts.append(_indicator_section(column, column_meta, table, 5))
            origins_by_column[column] = list(column_meta.origins)
        origins = unique_origins(origins_by_column)
        parts += ["#### Sources", ""]
        if origins:
            parts += [_source_section(origin, level=5) for origin in origins]
        else:
            parts += ["This table has no external sources: its columns document the dataset itself.", ""]

    parts += ["## How we process data", "", PROCESSING_NOTE, ""]
    # No dataset-wide license statement: OWID republishes data produced by others, so the original
    # sources' licenses are what governs reuse, and a single blanket license here would misstate that.
    parts += ["## License", "", LICENSE_NOTE, ""]

    # The ways in beyond the download buttons, one per line: the stable data URLs (readable from any tool that
    # opens a URL), the dated copies that never change, and the source code of the step. The catalog page lists
    # the same items.
    items: list[str] = []
    catalog_path = (
        f"{meta.channel}/{meta.namespace}/{meta.version}/{meta.short_name}"
        if meta.channel and meta.namespace and meta.version and meta.short_name
        else None
    )
    if url:
        items += ["- Catalog page:", f"  - {url}"]
        short_key = f"{meta.namespace}/{meta.short_name}/"
        base = url[: -len(short_key)] if catalog_path and url.endswith(short_key) else None
        for table in tables:
            name = table.metadata.short_name
            formats = ("csv", "xlsx", "parquet") if fits_in_excel(table) else ("csv", "parquet")
            items.append(f"- {table.metadata.title or name}")
            items.append("  - Links to the latest data. They always give you the newest release:")
            items += [f"    - {url}{name}.{suffix}" for suffix in formats + ("codebook.csv", "sources.csv")]
            if base:
                items.append(
                    f"  - Links to this version ({meta.version}). They always give you the same data, even if there "
                    "are newer releases:"
                )
                items += [
                    f"    - {base}{catalog_path}/{name}.{suffix}" for suffix in formats + ("feather", "meta.json")
                ]
    if meta.channel and meta.namespace and meta.version:
        items += ["- Source code:", f"  - {ETL_STEPS_URL}{meta.channel}/{meta.namespace}/{meta.version}/"]
    if items:
        parts += ["## Advanced download options", ""] + items + [""]
    return "\n".join(parts).rstrip() + "\n"
