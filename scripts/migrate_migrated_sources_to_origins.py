#!/usr/bin/env python3
"""Migrate legacy Source metadata to Origin metadata for dag/migrated.yml snapshots.

This is intentionally conservative:
- It only considers snapshot dependencies listed in dag/migrated.yml.
- It converts snapshot DVC `meta.source` / legacy source fields to `meta.origin`.
- It can simplify the common backport snapshot script pattern by removing
  `fill_from_backport_snapshot` and the config snapshot dependency.
- It writes a compact TSV report to stdout so ambiguous inferred fields can be reviewed.

Example dry run:
    .venv/bin/python scripts/migrate_migrated_sources_to_origins.py --limit 10

Apply first 10 migrations:
    .venv/bin/python scripts/migrate_migrated_sources_to_origins.py --apply --limit 10
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import LiteralScalarString

from etl.files import ruamel_dump, ruamel_load

DAG_PATH = Path("dag/migrated.yml")
SNAPSHOTS_DIR = Path("snapshots")

YEAR_RE = re.compile(r"(?<!\d)(1[5-9]\d{2}|20\d{2})(?!\d)")
SNAPSHOT_URI_RE = re.compile(r"(?m)^\s*(?:-\s*)?(snapshot://[^\s:#]+)")


@dataclass
class MigrationResult:
    path: Path
    status: str
    title: str = ""
    producer: str = ""
    date_published: str = ""
    date_published_source: str = ""
    citation_full: str = ""
    script_status: str = ""
    notes: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write changes. Default is dry run.")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N legacy snapshots.")
    parser.add_argument(
        "--offset", type=int, default=0, help="Skip the first N legacy snapshots before applying --limit."
    )
    parser.add_argument(
        "--no-script-update",
        action="store_true",
        help="Only migrate DVC metadata; do not simplify matching snapshot scripts.",
    )
    parser.add_argument(
        "--only-easy",
        action="store_true",
        help=(
            "Only migrate rows where the snapshot script matches the standard backport pattern "
            "and date_published can be inferred from source.name or source.published_by."
        ),
    )
    parser.add_argument(
        "--only-fallback-easy",
        action="store_true",
        help=(
            "Only migrate rows where the snapshot script matches the standard backport pattern "
            "and date_published falls back to the snapshot version."
        ),
    )
    return parser.parse_args()


def snapshot_paths_from_migrated_dag() -> list[Path]:
    uris = sorted({match.group(1).removeprefix("snapshot://") for match in SNAPSHOT_URI_RE.finditer(DAG_PATH.read_text())})
    return [SNAPSHOTS_DIR / f"{uri}.dvc" for uri in uris if (SNAPSHOTS_DIR / f"{uri}.dvc").exists()]


def has_legacy_source(meta: dict[str, Any]) -> bool:
    return "source" in meta or "source_name" in meta


def as_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def extract_publication_years(text: str) -> list[str]:
    years = []
    for match in YEAR_RE.finditer(text):
        prefix = text[max(0, match.start() - 12) : match.start()].lower()
        suffix = text[match.end() : min(len(text), match.end() + 12)].lower()
        char_before = text[match.start() - 1] if match.start() > 0 else ""
        char_after = text[match.end()] if match.end() < len(text) else ""

        # Skip years that are likely part of data coverage or projection ranges,
        # e.g. "1970–2050", "2030-50", "from 1961", "to 2050".
        if re.search(r"(since|from|to|until|through|between)\s+$", prefix):
            continue
        if re.search(r"^\s*(to|until|through)\b", suffix):
            continue
        if char_before in {"-", "–", "—"} or char_after in {"-", "–", "—"}:
            continue

        years.append(match.group(1))
    return years


def infer_date_published(source: dict[str, Any], snapshot_path: Path) -> tuple[str, str]:
    publication_date = as_str(source.get("publication_date"))
    if publication_date:
        return publication_date, "source.publication_date"

    publication_year = as_str(source.get("publication_year"))
    if publication_year:
        return publication_year, "source.publication_year"

    # Use concise citation-like fields only. Descriptions often contain years that are
    # part of the data coverage or projections (e.g. "to 2050"), not publication years.
    for field in ["published_by", "name"]:
        text = as_str(source.get(field))
        if not text:
            continue
        years = extract_publication_years(text)
        if years:
            return max(years), f"inferred from source.{field}"

    # Last resort for schema validity. These rows should be manually reviewed.
    version = snapshot_path.parent.name
    years = YEAR_RE.findall(version)
    if years:
        return max(years), "fallback to snapshot version"

    return "latest", "fallback to latest"


def legacy_source_from_meta(meta: dict[str, Any]) -> dict[str, Any]:
    if "source" in meta:
        return dict(meta.get("source") or {})

    # Older pre-Source snapshots. Not present in dag/migrated.yml at the time of writing,
    # but supported to keep the script reusable.
    return {
        "name": meta.get("source_name"),
        "description": meta.get("description"),
        "published_by": meta.get("source_published_by"),
        "source_data_url": meta.get("source_data_url"),
        "url": meta.get("url"),
        "date_accessed": meta.get("date_accessed"),
        "publication_date": meta.get("publication_date"),
        "publication_year": meta.get("publication_year"),
    }


def clean_producer_from_title(title: str) -> str:
    """Create a concise producer label from legacy Source.name.

    Legacy `Source.published_by` often stores a full citation. For origins,
    producer is user-facing and should be a short author/institution label with
    no publication year.
    """
    producer = title.strip()
    producer = producer.replace("&", "and").replace("OWID", "Our World in Data")

    # Remove common OWID prefixes; the origin producer should be the underlying source.
    producer = re.sub(r"^(Calculated by )?Our World (?:in|In) Data based on\s+", "", producer)
    producer = re.sub(r"^Our World (?:in|In) Data\s+based on\s+", "", producer)

    # Special case: old CEPII trade snapshots use a full paper citation in
    # Source.published_by, but Source.name gives a concise author label.
    if "Fouquin" in producer and "Hugot" in producer:
        return "Fouquin and Hugot"

    # Drop parentheticals that contain publication years or year-bearing acronyms.
    producer = re.sub(r"\s*\([^)]*\b(?:1[5-9]\d{2}|20\d{2})\b[^)]*\)", "", producer)
    producer = re.sub(r"\b(?:1[5-9]\d{2}|20\d{2})\b", "", producer)
    producer = re.sub(r"\s+", " ", producer).strip(" ,:-")
    if producer.endswith(".") and not producer.endswith("et al."):
        producer = producer[:-1]
    return producer or title


def build_origin(source: dict[str, Any], meta: dict[str, Any], snapshot_path: Path) -> tuple[CommentedMap, str, str]:
    title = as_str(source.get("name")) or as_str(meta.get("name")) or snapshot_path.stem.split(".", 1)[0]
    producer_raw = as_str(source.get("published_by")) or title
    producer = clean_producer_from_title(title)
    date_published, date_published_source = infer_date_published(source, snapshot_path)

    origin = CommentedMap()
    origin["title"] = title
    origin["producer"] = producer

    description = as_str(source.get("description")) or as_str(meta.get("description"))
    if description:
        origin["description"] = LiteralScalarString(description) if "\n" in description else description

    url_main = as_str(source.get("url"))
    if url_main:
        origin["url_main"] = url_main

    url_download = as_str(source.get("source_data_url"))
    if url_download:
        origin["url_download"] = url_download

    date_accessed = as_str(source.get("date_accessed"))
    if date_accessed:
        origin["date_accessed"] = date_accessed

    origin["date_published"] = date_published
    if len(producer_raw) > 255:
        origin["citation_full"] = producer_raw if producer_raw.endswith(".") else f"{producer_raw}."
    else:
        origin["citation_full"] = f"{producer} ({date_published}). {title}."

    return origin, date_published, date_published_source


def migrate_dvc(snapshot_path: Path, apply: bool) -> MigrationResult:
    with open(snapshot_path) as f:
        yml = ruamel_load(f)

    meta = yml.get("meta") or CommentedMap()
    if not has_legacy_source(meta):
        return MigrationResult(path=snapshot_path, status="skip-no-legacy-source")

    source = legacy_source_from_meta(meta)
    origin, date_published, date_published_source = build_origin(source, meta, snapshot_path)

    new_meta = CommentedMap()
    new_meta["origin"] = origin

    legacy_keys = {
        "source",
        "source_name",
        "source_published_by",
        "source_data_url",
        "url",
        "date_accessed",
        "publication_date",
        "publication_year",
        # These were only legacy Source compatibility fields for migrated snapshots.
        "name",
        "description",
    }
    for key, value in meta.items():
        if key not in legacy_keys:
            new_meta[key] = value

    yml["meta"] = new_meta

    if apply:
        snapshot_path.write_text(ruamel_dump(yml))

    return MigrationResult(
        path=snapshot_path,
        status="migrated" if apply else "would-migrate",
        title=origin["title"],
        producer=origin["producer"],
        date_published=date_published,
        date_published_source=date_published_source,
        citation_full=origin["citation_full"],
    )


def simplify_snapshot_script(snapshot_path: Path, apply: bool) -> str:
    script_path = snapshot_path.with_suffix("")  # remove .dvc
    script_path = script_path.with_suffix(".py")
    if not script_path.exists():
        return "no-script"

    text = script_path.read_text()
    if "fill_from_backport_snapshot" not in text:
        return "skip-no-fill-from-backport"

    new_text = text.replace("from etl.snapshot import Snapshot, SnapshotMeta", "from etl.snapshot import Snapshot")
    new_text = new_text.replace("from etl.snapshot import SnapshotMeta, Snapshot", "from etl.snapshot import Snapshot")

    snapshot_uri = str(snapshot_path.relative_to(SNAPSHOTS_DIR)).removesuffix(".dvc")
    replacement = (
        "    # Create a new snapshot. Metadata is hardcoded in the accompanying DVC file.\n"
        f'    snap = Snapshot("{snapshot_uri}")\n'
    )

    pattern = re.compile(
        r"    snap_config = Snapshot\([\s\S]*?"
        r"    snap_config\.pull\(\)\n\n"
        r"    # Create snapshot metadata for the new file\n"
        r"    meta = SnapshotMeta\(\*\*snap_values\.metadata\.to_dict\(\)\)\n"
        r"    meta\.namespace = SNAPSHOT_NAMESPACE\n"
        r"    meta\.version = SNAPSHOT_VERSION\n"
        r"    meta\.short_name = [^\n]+\n"
        r"    meta\.fill_from_backport_snapshot\(snap_config\.path\)\n"
        r"    meta\.save\(\)\n\n"
        r"    # Create a new snapshot\.\n"
        r"    snap = Snapshot\(meta\.uri\)\n"
    )
    new_text, count = pattern.subn(replacement, new_text)
    if count != 1:
        return "needs-review-script-pattern"

    if apply:
        script_path.write_text(new_text)
    return "simplified" if apply else "would-simplify"


def print_report(results: list[MigrationResult]) -> None:
    fields = [
        "status",
        "script_status",
        "path",
        "date_published",
        "date_published_source",
        "title",
        "producer",
        "citation_full",
        "notes",
    ]
    print("\t".join(fields))
    for result in results:
        values = []
        for field in fields:
            value = getattr(result, field)
            values.append(str(value).replace("\t", " ").replace("\n", " "))
        print("\t".join(values))


def main() -> None:
    args = parse_args()
    legacy_paths = []
    for snapshot_path in snapshot_paths_from_migrated_dag():
        with open(snapshot_path) as f:
            yml = ruamel_load(f)
        if has_legacy_source(yml.get("meta") or {}):
            legacy_paths.append(snapshot_path)

    selected_paths = legacy_paths[args.offset :]
    if args.limit is not None:
        selected_paths = selected_paths[: args.limit]

    results = []
    for snapshot_path in selected_paths:
        # Preview first, so --only-easy can skip rows without partially writing DVC files.
        preview = migrate_dvc(snapshot_path=snapshot_path, apply=False)
        if preview.status in {"migrated", "would-migrate"} and not args.no_script_update:
            preview.script_status = simplify_snapshot_script(snapshot_path=snapshot_path, apply=False)

        if args.only_easy or args.only_fallback_easy:
            is_easy = preview.script_status == "would-simplify" and preview.date_published_source.startswith(
                "inferred from source."
            )
            is_fallback_easy = (
                preview.script_status == "would-simplify"
                and preview.date_published_source == "fallback to snapshot version"
            )
            if (args.only_easy and not is_easy) or (args.only_fallback_easy and not is_fallback_easy):
                preview.status = "skip-not-selected"
                results.append(preview)
                continue

        if args.apply:
            result = migrate_dvc(snapshot_path=snapshot_path, apply=True)
            if result.status in {"migrated", "would-migrate"} and not args.no_script_update:
                result.script_status = simplify_snapshot_script(snapshot_path=snapshot_path, apply=True)
            results.append(result)
        else:
            results.append(preview)

    print_report(results)


if __name__ == "__main__":
    main()
