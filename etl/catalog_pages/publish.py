"""Publish catalog dataset pages (data files, codebook, README, manifest, JSON-LD) to R2."""

from __future__ import annotations

import concurrent.futures
from pathlib import Path
from typing import Any

from owid.catalog import Dataset
from owid.catalog.api.legacy import CHANNEL
from owid.catalog.s3_utils import connect_r2

from etl import config, files
from etl.catalog_pages.artifacts import (
    QUALITY_REPORT_FILENAME,
    SITEMAP_FILENAME,
    build_catalog_page_artifacts,
)
from etl.catalog_pages.pages import DATASET_JSONLD_FILENAME, page_filenames
from etl.paths import DATA_DIR
from etl.publish import get_remote_checksum


def build_and_publish_catalog_pages(
    *,
    bucket: str = config.R2_BUCKET,
    catalog_dir: Path = DATA_DIR,
    channel: CHANNEL = "garden",
    dry_run: bool = False,
    base_url: str = "https://catalog.ourworldindata.org",
    only: set[str] | None = None,
    active_steps: set[str] | None = None,
) -> None:
    """Build the page files of every opted-in dataset locally and sync them to R2.

    When ``only`` is given, restrict generation to datasets whose
    ``"<namespace>/<dataset>"`` is in the set (version-agnostic allowlist).
    Otherwise only datasets that opt in via ``dataset: jsonld: true`` in their
    metadata are considered.

    ``active_steps`` overrides the set of active DAG step URIs used to exclude stale,
    archived on-disk builds (see :func:`etl.catalog_pages.artifacts.latest_dataset_paths`).
    Defaults to the real DAG; tests should pass an explicit set instead.
    """
    result = build_catalog_page_artifacts(
        catalog_dir=catalog_dir,
        channel=channel,
        dry_run=dry_run,
        base_url=base_url,
        only=only,
        active_steps=active_steps,
    )
    if dry_run:
        print(
            f"Catalog pages dry run: would write pages for {len(result.pages)} datasets, emit JSON-LD for "
            f"{len(result.emitted)}, skip {len(result.skipped)}, warn {len(result.warnings)}"
        )
        return

    # Datasets are served at their stable "<namespace>/<dataset>" short key rather than their dated catalog
    # path. Every page file written locally is synced; the sitemap and quality report live at the root.
    keys = list(result.page_keys) + [SITEMAP_FILENAME, QUALITY_REPORT_FILENAME]

    def short_key_files(entry: Any) -> list[str]:
        ds = Dataset(catalog_dir / entry.catalog_path)
        return [f"{entry.short_key}/{name}" for name in page_filenames(ds)]

    # Delete the old dated-path dataset.jsonld for every currently-served dataset: it's no
    # longer written locally (superseded by the short key above), but a prior publish may
    # still have left it live on R2, which would otherwise sit around as duplicate content.
    delete_keys = [f"{entry.catalog_path}/{DATASET_JSONLD_FILENAME}" for entry in result.page_entries]
    # A page-eligible dataset that failed a JSON-LD gate keeps its page but loses its JSON-LD.
    delete_keys.extend(
        f"{entry.short_key}/{DATASET_JSONLD_FILENAME}"
        for entry in result.page_entries
        if entry not in result.emitted_entries
    )
    # Datasets that newly failed a page gate (e.g. non_redistributable) must stop being served: delete both
    # the stale dated-path JSON-LD and every page file a prior publish may have written at the short key.
    delete_keys.extend(f"{entry.catalog_path}/{DATASET_JSONLD_FILENAME}" for entry in result.skipped_entries)
    for entry in result.skipped_entries:
        delete_keys.extend(short_key_files(entry))
    # Datasets archived outright (no active replacement at all) never appear above, since no on-disk version
    # of them is active, but a prior publish may still have left their files live on R2. Several
    # archived_entries can share the same short key, so dedupe.
    delete_keys.extend(f"{entry.catalog_path}/{DATASET_JSONLD_FILENAME}" for entry in result.archived_entries)
    archived_short_key_files: set[str] = set()
    for entry in result.archived_entries:
        archived_short_key_files.update(short_key_files(entry))
    delete_keys.extend(sorted(archived_short_key_files))
    # Versions superseded by an active replacement under the same short key (e.g. a stale ".../latest/..."
    # build left behind after re-versioning to a dated one): only the dated path, never the short key, which
    # the active version legitimately owns instead.
    delete_keys.extend(f"{entry.catalog_path}/{DATASET_JSONLD_FILENAME}" for entry in result.superseded_entries)

    sync_jsonld_artifacts(connect_r2(), bucket, catalog_dir, keys, delete_keys=delete_keys)


def sync_jsonld_artifacts(
    s3: Any, bucket: str, catalog_dir: Path, keys: list[str], delete_keys: list[str] | None = None
) -> None:
    futures = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for key in keys:
            local_path = catalog_dir / key
            if not local_path.exists():
                continue
            checksum = files.checksum_file(local_path)
            remote_checksum = get_remote_checksum(s3, bucket, key)
            if checksum == remote_checksum:
                continue
            print(f"  PUT {key}")
            futures.append(
                executor.submit(
                    s3.upload_file,
                    local_path.as_posix(),
                    bucket,
                    key,
                    ExtraArgs={"ACL": "public-read", "Metadata": {"md5": checksum}},
                )
            )

        for key in delete_keys or []:
            if key in keys:
                continue
            if get_remote_checksum(s3, bucket, key) is not None:
                print(f"  DEL {key}")
                futures.append(executor.submit(s3.delete_object, Bucket=bucket, Key=key))

        concurrent.futures.wait(futures)
