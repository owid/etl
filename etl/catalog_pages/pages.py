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

import pandas as pd
from owid.catalog import Dataset
from owid.catalog.core.docs import dataset_title, ordered_table_names
from structlog import get_logger

log = get_logger()

MANIFEST_VERSION = 1
MANIFEST_FILENAME = "manifest.json"
README_FILENAME = "readme.md"
CODEBOOK_FILENAME = "codebook.csv"
DATASET_JSONLD_FILENAME = "dataset.jsonld"
# Dated catalog files (immutable) that the manifest links to when they exist on disk.
VERSIONED_FORMATS = ("parquet", "feather")


@dataclass
class PageFiles:
    """Files written for one dataset page, as keys relative to the catalog root."""

    keys: list[str] = field(default_factory=list)
    xlsx_skipped: str | None = None


def page_url(base_url: str, short_key: str) -> str:
    return f"{base_url.rstrip('/')}/{short_key}/"


def page_filenames(ds: Dataset) -> list[str]:
    """Every file a dataset page can own in its stable folder, so that a stale page can be removed in full."""
    names = [f"{name}.csv" for name in ordered_table_names(ds)]
    names += [f"{ds.metadata.short_name}.xlsx", CODEBOOK_FILENAME, README_FILENAME, MANIFEST_FILENAME]
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
    jsonld_written: bool,
    explore_url: str | None = None,
) -> PageFiles:
    """Write the CSV, XLSX, codebook, README and manifest of a dataset into ``catalog_dir / short_key``."""
    target_dir = catalog_dir / short_key
    target_dir.mkdir(parents=True, exist_ok=True)
    url = page_url(base_url, short_key)
    result = PageFiles()
    files: list[dict[str, Any]] = []

    def register(name: str, format: str, table: str | None = None, versioned: bool = False) -> None:
        path = (catalog_dir / catalog_path / name) if versioned else (target_dir / name)
        entry: dict[str, Any] = {
            "name": name,
            "format": format,
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

    # One CSV per table; the download filename is the table name, which says what the file is once detached
    # from our folders.
    for name in ordered_table_names(ds):
        table = ds[name]
        csv_name = f"{name}.csv"
        pd.DataFrame(table.reset_index(drop=not table.primary_key)).to_csv(target_dir / csv_name, index=False)
        register(csv_name, "csv", table=name)

    # One workbook with every table, the codebook, the sources and the README. Refused, not truncated, when a
    # table exceeds Excel's row limit.
    xlsx_name = f"{ds.metadata.short_name}.xlsx"
    xlsx_path = target_dir / xlsx_name
    try:
        ds.to_excel(xlsx_path)
        register(xlsx_name, "xlsx")
    except ValueError as error:
        result.xlsx_skipped = str(error)
        log.warning("catalog_pages.xlsx_skipped", dataset=catalog_path, reason=str(error))
        if xlsx_path.exists():
            xlsx_path.unlink()

    ds.codebook.to_csv(target_dir / CODEBOOK_FILENAME, index=False)
    register(CODEBOOK_FILENAME, "csv")

    (target_dir / README_FILENAME).write_text(ds.readme(url=url))
    register(README_FILENAME, "md")

    # Immutable, dated copies of the tables as they live in the catalog.
    for name in ordered_table_names(ds):
        for format in VERSIONED_FORMATS:
            if (catalog_dir / catalog_path / f"{name}.{format}").exists():
                register(f"{name}.{format}", format, table=name, versioned=True)

    manifest: dict[str, Any] = {
        "manifest_version": MANIFEST_VERSION,
        "catalog_path": catalog_path,
        "namespace": ds.metadata.namespace,
        "short_name": ds.metadata.short_name,
        "version": version,
        "title": dataset_title(ds.metadata, [ds.read(name, load_data=False) for name in ordered_table_names(ds)]),
        "description": ds.metadata.description,
        "published_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "license": _license(ds),
        "readme": README_FILENAME,
        "codebook": CODEBOOK_FILENAME,
        "tables": list(ordered_table_names(ds)),
        "files": files,
    }
    if jsonld_written:
        manifest["jsonld"] = DATASET_JSONLD_FILENAME
    if explore_url:
        manifest["explore_url"] = explore_url
    if result.xlsx_skipped:
        manifest["xlsx_skipped"] = result.xlsx_skipped
    with open(target_dir / MANIFEST_FILENAME, "w") as ostream:
        json.dump(manifest, ostream, indent=2, ensure_ascii=False)
        ostream.write("\n")
    result.keys.append(f"{short_key}/{MANIFEST_FILENAME}")
    return result


def _license(ds: Dataset) -> dict[str, str | None] | None:
    if ds.metadata.licenses:
        license = ds.metadata.licenses[0]
        return {"name": license.name, "url": license.url}
    return None


def remove_page_files(ds: Dataset, target_dir: Path) -> None:
    for name in page_filenames(ds):
        path = target_dir / name
        if path.exists():
            path.unlink()
