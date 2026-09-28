"""Public page files for a catalog dataset: data files, codebook, README and manifest.

The files are written into the dataset's stable folder ``<namespace>/<short_name>/`` of the catalog, next to
the ``dataset.jsonld`` side product. The Cloudflare Worker that serves ``catalog.ourworldindata.org`` renders the
dataset page from ``manifest.json`` and ``readme.md``; everything in them is derived from the dataset's own
metadata by ``owid.catalog``. The manifest contract (version 1) is shared with the Worker: keep both sides equal.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pandas as pd
from owid.catalog import Dataset
from owid.catalog.core.docs import dataset_title, ordered_table_names
from structlog import get_logger

log = get_logger()

MANIFEST_VERSION = 1
MANIFEST_FILENAME = "manifest.json"
README_FILENAME = "readme.md"
CODEBOOK_FILENAME = "codebook.csv"
SOURCES_FILENAME = "sources.csv"
DATASET_JSONLD_FILENAME = "dataset.jsonld"
# Formats every table is published in, at the stable URL: CSV for anyone, parquet for anyone with a dataframe
# library (a fraction of the CSV's size, and it keeps the column types).
TABLE_FORMATS = ("csv", "parquet")
# Dated catalog files (immutable) that the manifest links to when they exist on disk.
VERSIONED_FORMATS = ("parquet", "feather")
# What each file is for, so the page can group them: the data itself, one bundle with everything, the
# documentation, and the immutable dated copies.
ROLE_DATA = "data"
ROLE_BUNDLE = "bundle"
ROLE_DOCUMENTATION = "documentation"
ROLE_ARCHIVE = "archive"
ENCODING_FORMATS = {
    "csv": "text/csv",
    "parquet": "application/vnd.apache.parquet",
    "feather": "application/vnd.apache.arrow.file",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}


@dataclass
class PageFiles:
    """Files written for one dataset page, as keys relative to the catalog root."""

    keys: list[str] = field(default_factory=list)
    xlsx_skipped: str | None = None


def page_url(base_url: str, short_key: str) -> str:
    return f"{base_url.rstrip('/')}/{short_key}/"


def page_filenames(ds: Dataset) -> list[str]:
    """Every file a dataset page can own in its stable folder, so that a stale page can be removed in full."""
    names = [f"{name}.{format}" for name in ordered_table_names(ds) for format in TABLE_FORMATS]
    names += [f"{ds.metadata.short_name}.xlsx", CODEBOOK_FILENAME, SOURCES_FILENAME, README_FILENAME, MANIFEST_FILENAME]
    names.append(DATASET_JSONLD_FILENAME)
    return names


def write_page_files(
    ds: Dataset,
    *,
    catalog_dir: Path,
    catalog_path: str,
    short_key: str,
    version: str,
    base_url: str,
    jsonld: dict[str, Any] | None = None,
    topics: list[str] | None = None,
) -> PageFiles:
    """Write the data files, codebook, README, manifest and JSON-LD of a dataset into ``catalog_dir / short_key``.

    ``jsonld`` is the Schema.org record built from the dataset's metadata (``None`` when the dataset failed a
    JSON-LD gate). It is written next to the manifest with its ``distribution`` replaced by the manifest's own
    file list, so search engines and the page always see the same downloads.

    ``topics`` are the dataset's topic tags, most-tagged first; the first one is the dataset's primary topic
    and gives the page its link back to the charts on ourworldindata.org.
    """
    target_dir = catalog_dir / short_key
    target_dir.mkdir(parents=True, exist_ok=True)
    url = page_url(base_url, short_key)
    result = PageFiles()
    files: list[dict[str, Any]] = []

    def register(name: str, format: str, role: str, table: str | None = None, versioned: bool = False) -> None:
        path = (catalog_dir / catalog_path / name) if versioned else (target_dir / name)
        entry: dict[str, Any] = {
            "name": name,
            "format": format,
            "role": role,
            "size_bytes": path.stat().st_size,
            "url": f"{base_url.rstrip('/')}/{catalog_path if versioned else short_key}/{name}",
        }
        if table:
            entry["table"] = table
        if versioned:
            entry["versioned"] = True
        files.append(entry)
        if not versioned:
            result.keys.append(f"{short_key}/{name}")

    # One CSV and one parquet per table; the download filename is the table name, which says what the file is
    # once detached from our folders.
    tables: list[dict[str, Any]] = []
    for name in ordered_table_names(ds):
        table = ds[name]
        flat = pd.DataFrame(table.reset_index(drop=not table.primary_key))
        flat.to_csv(target_dir / f"{name}.csv", index=False)
        register(f"{name}.csv", "csv", ROLE_DATA, table=name)
        flat.to_parquet(target_dir / f"{name}.parquet", index=False)
        register(f"{name}.parquet", "parquet", ROLE_DATA, table=name)
        tables.append(
            {
                "name": name,
                "title": table.metadata.title,
                "description": table.metadata.description,
                "rows": int(len(flat)),
                "columns": int(len(flat.columns)),
            }
        )

    # One workbook with every table, the codebook, the sources and the README. Refused, not truncated, when a
    # table exceeds Excel's row limit.
    xlsx_name = f"{ds.metadata.short_name}.xlsx"
    xlsx_path = target_dir / xlsx_name
    try:
        ds.to_excel(xlsx_path)
        register(xlsx_name, "xlsx", ROLE_BUNDLE)
    except ValueError as error:
        result.xlsx_skipped = str(error)
        log.warning("catalog_pages.xlsx_skipped", dataset=catalog_path, reason=str(error))
        if xlsx_path.exists():
            xlsx_path.unlink()

    ds.codebook.to_csv(target_dir / CODEBOOK_FILENAME, index=False)
    register(CODEBOOK_FILENAME, "csv", ROLE_DOCUMENTATION)
    ds.sources.to_csv(target_dir / SOURCES_FILENAME, index=False)
    register(SOURCES_FILENAME, "csv", ROLE_DOCUMENTATION)

    (target_dir / README_FILENAME).write_text(ds.readme(url=url))
    register(README_FILENAME, "md", ROLE_DOCUMENTATION)

    # Immutable, dated copies of the tables as they live in the catalog.
    for name in ordered_table_names(ds):
        for format in VERSIONED_FORMATS:
            if (catalog_dir / catalog_path / f"{name}.{format}").exists():
                register(f"{name}.{format}", format, ROLE_ARCHIVE, table=name, versioned=True)

    if jsonld is not None:
        jsonld = {**jsonld, "distribution": jsonld_distributions(files)}
        with open(target_dir / DATASET_JSONLD_FILENAME, "w") as ostream:
            json.dump(jsonld, ostream, indent=2, ensure_ascii=False)
            ostream.write("\n")
        result.keys.append(f"{short_key}/{DATASET_JSONLD_FILENAME}")

    manifest: dict[str, Any] = {
        "manifest_version": MANIFEST_VERSION,
        "catalog_path": catalog_path,
        "namespace": ds.metadata.namespace,
        "short_name": ds.metadata.short_name,
        "version": version,
        "title": dataset_title(ds.metadata, [ds.read(name, load_data=False) for name in ordered_table_names(ds)]),
        "description": ds.metadata.description,
        "published_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "readme": README_FILENAME,
        "codebook": CODEBOOK_FILENAME,
        "sources": SOURCES_FILENAME,
        # First entry is the main table (the one named after the dataset).
        "tables": tables,
        "files": files,
    }
    if jsonld is not None:
        manifest["jsonld"] = DATASET_JSONLD_FILENAME
    if topics:
        manifest["topics"] = topics
        manifest["explore_url"] = explore_url(topics[0])
    if result.xlsx_skipped:
        manifest["xlsx_skipped"] = result.xlsx_skipped
    with open(target_dir / MANIFEST_FILENAME, "w") as ostream:
        json.dump(manifest, ostream, indent=2, ensure_ascii=False)
        ostream.write("\n")
    result.keys.append(f"{short_key}/{MANIFEST_FILENAME}")
    return result


def explore_url(topic: str) -> str:
    """The site search filtered to a topic: the closest thing to "all our charts about this" that has a URL."""
    return f"https://ourworldindata.org/search?{urlencode({'q': topic, 'resultType': 'all'})}"


def jsonld_distributions(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Schema.org ``DataDownload`` entries for the data files of a page: the tables in every format, at the
    stable URL, plus the dated copies. The documentation files are not data and stay out."""
    return [
        {
            "@type": "DataDownload",
            "name": entry["name"],
            "encodingFormat": ENCODING_FORMATS.get(entry["format"], entry["format"]),
            "contentUrl": entry["url"],
            "contentSize": str(entry["size_bytes"]),
        }
        for entry in files
        if entry["role"] in (ROLE_DATA, ROLE_BUNDLE, ROLE_ARCHIVE)
    ]


def remove_page_files(ds: Dataset, target_dir: Path) -> None:
    for name in page_filenames(ds):
        path = target_dir / name
        if path.exists():
            path.unlink()
