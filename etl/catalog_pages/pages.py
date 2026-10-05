"""Public page files for a catalog dataset: per table its data, codebook and sources; per dataset a manifest.

The files are published at the dataset's stable key ``<namespace>/<short_name>/`` of the catalog bucket, next to
the ``dataset.jsonld`` side product. They are built in a folder of their own, never in the local catalog, whose
top level holds only channels. The Cloudflare Worker that serves ``catalog.ourworldindata.org`` renders the
dataset page from ``manifest.json`` and the tables' codebooks and sources; everything in them is derived from the
dataset's own metadata by ``owid.catalog``. The manifest contract (version 1) is shared with the Worker: keep both
sides equal.
"""

from __future__ import annotations

import functools
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import owid.catalog
import pandas as pd
from owid.catalog import Dataset
from owid.catalog.core import docs
from owid.catalog.core.docs import dataset_title, fits_in_excel, ordered_table_names, table_citation
from structlog import get_logger

from etl import files as etl_files

log = get_logger()

MANIFEST_VERSION = 1
MANIFEST_FILENAME = "manifest.json"
DATASET_JSONLD_FILENAME = "dataset.jsonld"
# Files earlier publishes wrote into a page's folder and this one no longer does. They stay among the files a page
# can own, so the next publish of each page deletes them from R2.
RETIRED_FILENAMES = ("readme.md",)
# Formats every table is published in, at the stable URL: CSV for anyone, parquet for anyone with a dataframe
# library (a fraction of the CSV's size, and it keeps the column types), and an Excel workbook with the table,
# its codebook and its sources as sheets.
TABLE_FORMATS = ("csv", "parquet", "xlsx")
# Documentation files written next to each table.
TABLE_DOCUMENTATION = ("codebook.csv", "sources.csv")
# Files of the dataset's dated catalog folder that the manifest links to, as a link to one version. They are the
# pipeline's own files, published with the dataset (a CSV among them only when the step saves one); the page
# writer never writes into that folder, which belongs to the pipeline (a CSV there would become one of the
# dataset's data files, and change its checksum).
VERSIONED_SUFFIXES = {suffix: "json" if suffix == "meta.json" else suffix for suffix in docs.VERSIONED_SUFFIXES}
# Formats that count as a download of the data in the JSON-LD (the dated metadata file is not one).
DATA_FORMATS = ("csv", "parquet", "feather", "xlsx")
# What each file is for, so the page can group them: the data itself, the documentation, and the dated files of
# one version.
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
# Live team-page URLs for dataset owners that have one (checked by hand); owners without an
# entry render unlinked on the page, as on data pages. The map lives in this package rather
# than etl/owners.py because only this package is covered by _code_checksum, so editing it
# here republishes the pages.
OWNER_TEAM_PAGES = {
    "Bastian Herre": "https://ourworldindata.org/team/bastian-herre",
    "Bertha Rohenkohl": "https://ourworldindata.org/team/bertha-rohenkohl",
    "Charlie Giattino": "https://ourworldindata.org/team/charlie-giattino",
    "Edouard Mathieu": "https://ourworldindata.org/team/edouard-mathieu",
    "Esteban Ortiz-Ospina": "https://ourworldindata.org/team/esteban-ortiz-ospina",
    "Fiona Spooner": "https://ourworldindata.org/team/fiona-spooner",
    "Hannah Ritchie": "https://ourworldindata.org/team/hannah-ritchie",
    "Joe Hasell": "https://ourworldindata.org/team/joe-hasell",
    "Lucas Rodés-Guirao": "https://ourworldindata.org/team/lucas-rodes-guirao",
    "Max Roser": "https://ourworldindata.org/team/max-roser",
    "Pablo Arriagada": "https://ourworldindata.org/team/pablo-arriagada",
    "Pablo Rosado": "https://ourworldindata.org/team/pablo-rosado",
    "Tuna Acisu": "https://ourworldindata.org/team/tuna-acisu",
}


@dataclass
class PageFiles:
    """Files written for one dataset page, as keys relative to the catalog root."""

    keys: list[str] = field(default_factory=list)
    # Tables that got no workbook, with the reason.
    xlsx_skipped: dict[str, str] = field(default_factory=dict)


def page_filenames(ds: Dataset) -> list[str]:
    """Every file a dataset page can own in its stable folder, so that a stale page can be removed in full."""
    names = [f"{name}.{suffix}" for name in ordered_table_names(ds) for suffix in TABLE_FORMATS + TABLE_DOCUMENTATION]
    return names + [MANIFEST_FILENAME, DATASET_JSONLD_FILENAME, *RETIRED_FILENAMES]


def page_build_checksum(ds: Dataset, *, short_key: str, base_url: str, has_jsonld: bool) -> str:
    """Fingerprint of everything a dataset's page files are made from.

    It covers the dataset's data and metadata, the code that renders the page files, where they are published and
    whether the page has a JSON-LD. A page whose fingerprint matches the published manifest's ``build_checksum`` is
    left as it is, so a publish only rebuilds the pages of datasets that changed.
    """
    parts = [ds.checksum(), short_key, base_url.rstrip("/"), str(has_jsonld), _code_checksum()]
    return etl_files.checksum_str(",".join(parts))


@functools.cache
def _code_checksum() -> str:
    """Checksum of the code that renders page files: all of owid.catalog, and this package."""
    code = sorted(Path(owid.catalog.__file__).parent.rglob("*.py")) + sorted(Path(__file__).parent.glob("*.py"))
    return etl_files.checksum_str(",".join(etl_files.checksum_file(path) for path in code))


def write_page_files(
    ds: Dataset,
    *,
    output_dir: Path,
    catalog_path: str,
    short_key: str,
    version: str,
    base_url: str,
    jsonld: dict[str, Any] | None = None,
    topics: list[str] | None = None,
    build_checksum: str | None = None,
) -> PageFiles:
    """Write the data files, documentation, manifest and JSON-LD of a dataset into ``output_dir / short_key``.

    The dataset's own folder is never written to: it only supplies the links to this version. ``build_checksum`` (see :func:`page_build_checksum`) is recorded in the
    manifest, for the next publish to compare against.

    ``jsonld`` is the Schema.org record built from the dataset's metadata (``None`` when the dataset failed a
    JSON-LD gate). It is written next to the manifest with its ``distribution`` replaced by the manifest's own
    file list, so search engines and the page always see the same downloads.

    ``topics`` are the dataset's topic tags, most-tagged first; the first one is the dataset's primary topic
    and gives the page its link back to the charts on ourworldindata.org.
    """
    target_dir = output_dir / short_key
    target_dir.mkdir(parents=True, exist_ok=True)
    result = PageFiles()
    files: list[dict[str, Any]] = []

    def register(name: str, format: str, role: str, table: str | None = None, versioned: bool = False) -> None:
        path = (Path(ds.path) / name) if versioned else (target_dir / name)
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
        # Dated files are uploaded with the dataset itself, not by the page.
        if not versioned:
            result.keys.append(f"{short_key}/{name}")

    # Per table: the data as CSV, parquet and Excel workbook, its codebook and its sources. The download filename
    # is the table name, which says what the file is once detached from our folders.
    table_entries: list[dict[str, Any]] = []
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
        else:
            table.to_excel(target_dir / f"{name}.xlsx", sheet_name="data", metadata_sheet_name="codebook", index=False)
            register(f"{name}.xlsx", "xlsx", ROLE_DATA, table=name)
        table.codebook.to_csv(target_dir / entry["codebook"], index=False)
        register(entry["codebook"], "csv", ROLE_DOCUMENTATION, table=name)
        table.sources.to_csv(target_dir / entry["sources"], index=False)
        register(entry["sources"], "csv", ROLE_DOCUMENTATION, table=name)
        table_entries.append(entry)

    # Links to this version: the pipeline's own dated files, where they exist.
    for name in ordered_table_names(ds):
        for suffix, format in VERSIONED_SUFFIXES.items():
            if (Path(ds.path) / f"{name}.{suffix}").exists():
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
        # Markdown, as the page renders it: without details-on-demand links, and absent when it is a template.
        "description": docs._clean_text(ds.metadata.description),
        "published_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        # First entry is the main table (the one named after the dataset). Each carries its own codebook and sources.
        "tables": table_entries,
        "files": files,
    }
    # Newest release first, as the page lists them; each change is markdown.
    if ds.metadata.changelog:
        manifest["changelog"] = [
            {"date": entry.date, "changes": list(entry.changes)}
            for entry in sorted(ds.metadata.changelog, key=lambda entry: entry.date, reverse=True)
        ]
    if jsonld is not None:
        manifest["jsonld"] = DATASET_JSONLD_FILENAME
    # First owner is the accountable one; names without a team page carry no url.
    if ds.metadata.owners:
        manifest["owners"] = [
            {"name": name, "url": OWNER_TEAM_PAGES[name]} if name in OWNER_TEAM_PAGES else {"name": name}
            for name in ds.metadata.owners
        ]
    if topics:
        manifest["topics"] = topics
        manifest["explore_url"] = explore_url(topics[0])
    if build_checksum:
        manifest["build_checksum"] = build_checksum
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
    stable URL, plus the dated files of this version. The documentation files are not data and stay out."""
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
