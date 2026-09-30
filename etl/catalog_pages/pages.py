"""Public page files for a catalog dataset: per table its data, codebook and sources; per dataset a README and a manifest.

The files are written into the dataset's stable folder ``<namespace>/<short_name>/`` of the catalog, next to
the ``dataset.jsonld`` side product. The Cloudflare Worker that serves ``catalog.ourworldindata.org`` renders the
dataset page from ``manifest.json`` and ``readme.md``; everything in them is derived from the dataset's own
metadata by ``owid.catalog``. The manifest contract (version 1) is shared with the Worker: keep both sides equal.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pandas as pd
from owid.catalog import Dataset
from owid.catalog.core import docs
from owid.catalog.core.docs import dataset_title, fits_in_excel, ordered_table_names, table_citation
from structlog import get_logger

log = get_logger()

MANIFEST_VERSION = 1
MANIFEST_FILENAME = "manifest.json"
README_FILENAME = "readme.md"
DATASET_JSONLD_FILENAME = "dataset.jsonld"
# Formats every table is published in, at the stable URL: CSV for anyone, parquet for anyone with a dataframe
# library (a fraction of the CSV's size, and it keeps the column types), and an Excel workbook with the table,
# its codebook and its sources as sheets.
TABLE_FORMATS = ("csv", "parquet", "xlsx")
# Documentation files written next to each table.
TABLE_DOCUMENTATION = ("codebook.csv", "sources.csv")
# Dated catalog files (immutable) that the manifest links to. The CSV and Excel are copies the page writer puts
# next to the pipeline's own parquet, feather and metadata, so that every format has a permanent link.
VERSIONED_SUFFIXES = {"csv": "csv", "xlsx": "xlsx", "parquet": "parquet", "feather": "feather", "meta.json": "json"}
VERSIONED_COPIES = ("csv", "xlsx")
# Formats that count as a download of the data in the JSON-LD (the dated metadata file is not one).
DATA_FORMATS = ("csv", "parquet", "feather", "xlsx")
# What each file is for, so the page can group them: the data itself, the documentation, and the immutable
# dated copies.
ROLE_DATA = "data"
ROLE_DOCUMENTATION = "documentation"
ROLE_ARCHIVE = "archive"
ENCODING_FORMATS = {
    "csv": "text/csv",
    "parquet": "application/vnd.apache.parquet",
    "feather": "application/vnd.apache.arrow.file",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "json": "application/json",
}


@dataclass
class PageFiles:
    """Files written for one dataset page, as keys relative to the catalog root."""

    keys: list[str] = field(default_factory=list)
    # Tables that got no workbook, with the reason.
    xlsx_skipped: dict[str, str] = field(default_factory=dict)


def page_url(base_url: str, short_key: str) -> str:
    return f"{base_url.rstrip('/')}/{short_key}/"


def page_filenames(ds: Dataset) -> list[str]:
    """Every file a dataset page can own in its stable folder, so that a stale page can be removed in full."""
    names = [f"{name}.{suffix}" for name in ordered_table_names(ds) for suffix in TABLE_FORMATS + TABLE_DOCUMENTATION]
    return names + [README_FILENAME, MANIFEST_FILENAME, DATASET_JSONLD_FILENAME]


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
    """Write the data files, documentation, README, manifest and JSON-LD of a dataset into ``catalog_dir / short_key``.

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
        elif name.rsplit(".", 1)[-1] in VERSIONED_COPIES:
            # The pipeline's own dated files are uploaded with the dataset; the page writer's copies are not.
            result.keys.append(f"{catalog_path}/{name}")

    # Per table: the data as CSV, parquet and Excel workbook, its codebook and its sources. The download filename
    # is the table name, which says what the file is once detached from our folders.
    tables: list[dict[str, Any]] = []
    for name in ordered_table_names(ds):
        table = ds[name].reset_index(drop=not ds[name].primary_key)
        flat = pd.DataFrame(table)
        flat.to_csv(target_dir / f"{name}.csv", index=False)
        register(f"{name}.csv", "csv", ROLE_DATA, table=name)
        flat.to_parquet(target_dir / f"{name}.parquet", index=False)
        register(f"{name}.parquet", "parquet", ROLE_DATA, table=name)
        entry: dict[str, Any] = {
            "name": name,
            "title": table.metadata.title,
            "description": table.metadata.description,
            "rows": int(len(flat)),
            "columns": int(len(flat.columns)),
            "codebook": f"{name}.codebook.csv",
            "sources": f"{name}.sources.csv",
        }
        # How to cite the table, in the short form used on the charts; a table without origins (documentation of
        # the dataset itself) gets none.
        citation = table_citation(table)
        if citation:
            entry["citation"] = citation
        if not fits_in_excel(table):
            reason = f"{len(flat):,} rows plus the header exceed Excel's limit of {docs.EXCEL_MAX_ROWS:,}"
            result.xlsx_skipped[name] = entry["xlsx_skipped"] = reason
            log.warning("catalog_pages.xlsx_skipped", dataset=catalog_path, table=name, reason=reason)
            # A workbook left by an earlier build, when the table was smaller, must not be served as current.
            (target_dir / f"{name}.xlsx").unlink(missing_ok=True)
        else:
            table.to_excel(target_dir / f"{name}.xlsx", sheet_name="data", metadata_sheet_name="codebook", index=False)
            register(f"{name}.xlsx", "xlsx", ROLE_DATA, table=name)
        table.codebook.to_csv(target_dir / entry["codebook"], index=False)
        register(entry["codebook"], "csv", ROLE_DOCUMENTATION, table=name)
        table.sources.to_csv(target_dir / entry["sources"], index=False)
        register(entry["sources"], "csv", ROLE_DOCUMENTATION, table=name)
        tables.append(entry)

    (target_dir / README_FILENAME).write_text(ds.readme(url=url))
    register(README_FILENAME, "md", ROLE_DOCUMENTATION)

    # Immutable, dated copies of the tables: every format the page offers, plus the pipeline's own files.
    dated_dir = catalog_dir / catalog_path
    dated_dir.mkdir(parents=True, exist_ok=True)
    for name in ordered_table_names(ds):
        for suffix in VERSIONED_COPIES:
            source = target_dir / f"{name}.{suffix}"
            if source.exists():
                shutil.copyfile(source, dated_dir / f"{name}.{suffix}")
            else:
                (dated_dir / f"{name}.{suffix}").unlink(missing_ok=True)
        for suffix, format in VERSIONED_SUFFIXES.items():
            if (dated_dir / f"{name}.{suffix}").exists():
                register(f"{name}.{suffix}", format, ROLE_ARCHIVE, table=name, versioned=True)

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
        # First entry is the main table (the one named after the dataset). Each carries its own codebook and sources.
        "tables": tables,
        "files": files,
    }
    if jsonld is not None:
        manifest["jsonld"] = DATASET_JSONLD_FILENAME
    if topics:
        manifest["topics"] = topics
        manifest["explore_url"] = explore_url(topics[0])
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
        if entry["role"] in (ROLE_DATA, ROLE_ARCHIVE) and entry["format"] in DATA_FORMATS
    ]


def remove_page_files(ds: Dataset, target_dir: Path) -> None:
    for name in page_filenames(ds):
        path = target_dir / name
        if path.exists():
            path.unlink()
