"""Publish catalog dataset pages (data files, codebook, README, manifest, JSON-LD) to R2."""

from __future__ import annotations

import concurrent.futures
import functools
import json
import re
import tempfile
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError
from owid.catalog import Dataset
from owid.catalog.api.legacy import CHANNEL, LocalCatalog
from owid.catalog.s3_utils import connect_r2

from etl import config, files
from etl.catalog_pages.artifacts import (
    QUALITY_REPORT_FILENAME,
    SITEMAP_FILENAME,
    JsonLdBuildResult,
    LatestDatasetPath,
    build_catalog_page_artifacts,
)
from etl.catalog_pages.pages import DATASET_JSONLD_FILENAME, MANIFEST_FILENAME, page_filenames
from etl.paths import DATA_DIR
from etl.publish import get_remote_checksum

# A staging server publishes its pages into its own folder of the staging bucket, next to its indicators.
STAGING_BUCKET = "owid-api-staging"
STAGING_BASE_URL = "https://api-staging.owid.io"
STAGING_PAGES_FOLDER = "v1/dataset-pages"
# Channels whose steps can have a page.
PAGE_CHANNELS: tuple[CHANNEL, ...] = ("garden", "grapher")


def build_and_publish_catalog_pages(
    *,
    bucket: str = config.R2_BUCKET,
    catalog_dir: Path = DATA_DIR,
    channel: CHANNEL | tuple[CHANNEL, ...] = "garden",
    dry_run: bool = False,
    base_url: str = "https://catalog.ourworldindata.org",
    only: set[str] | None = None,
    restrict_to: set[str] | None = None,
    active_steps: set[str] | None = None,
    key_prefix: str = "",
    steps_dir: Path | None = None,
) -> None:
    """Build the page files of every opted-in dataset in a temporary folder and sync them to R2.

    Page files are never written into ``catalog_dir``, whose top level holds only channels. A page whose build checksum
    matches the one in its published manifest is not rebuilt, so rerunning a publish only rebuilds the pages of
    datasets that changed.

    When ``only`` is given, restrict generation to datasets whose
    ``"<namespace>/<dataset>"`` is in the set (version-agnostic allowlist).
    Otherwise only datasets that opt in via ``dataset: jsonld: true`` in their
    metadata are considered.

    ``restrict_to`` keeps only the opted-in datasets whose ``"<namespace>/<dataset>"`` is in the set.

    ``active_steps`` overrides the set of active DAG step URIs used to exclude stale,
    archived on-disk builds (see :func:`etl.catalog_pages.artifacts.latest_dataset_paths`).
    Defaults to the real DAG; tests should pass an explicit set instead.

    ``key_prefix`` is a folder inside the bucket to publish into (the bucket's top level by default). ``base_url``
    must be the public address of that folder.
    """
    with tempfile.TemporaryDirectory(prefix="catalog-pages-") as tmp:
        output_dir = Path(tmp)
        s3 = None if dry_run else connect_r2()
        result = build_catalog_page_artifacts(
            output_dir=output_dir,
            catalog_dir=catalog_dir,
            channel=channel,
            dry_run=dry_run,
            base_url=base_url,
            only=only,
            restrict_to=restrict_to,
            active_steps=active_steps,
            published_build_checksum=functools.partial(published_build_checksum, s3, bucket, key_prefix=key_prefix)
            if s3
            else None,
            steps_dir=steps_dir,
        )
        if dry_run:
            print(
                f"Catalog pages dry run: would write pages for {len(result.pages)} datasets, emit JSON-LD for "
                f"{len(result.emitted)}, skip {len(result.skipped)}, warn {len(result.warnings)}"
            )
            return
        sync_page_files(s3, bucket, output_dir, *_page_keys(result, catalog_dir), key_prefix=key_prefix)


def publish_staging_dataset_pages(dry_run: bool = False) -> None:
    """Publish the pages of the datasets that this branch changed into the staging server's own folder.

    A staging server only builds what its branch changes, so it only gets the pages of the opted-in datasets among
    the changed steps and the steps that depend on them. For every other dataset, Grapher reads the production page.
    """
    from etl.git_helpers import get_changed_files
    from etl.io import get_all_changed_catalog_paths

    if config.DATA_API_ENV == "production":
        raise ValueError("Production pages are published by `etl d publish --jsonld`, not by this command.")
    changed_paths = set()
    for catalog_path in get_all_changed_catalog_paths(get_changed_files()):
        parts = catalog_path.split("/")
        if len(parts) == 4 and parts[0] in PAGE_CHANNELS and (DATA_DIR / catalog_path / "index.json").exists():
            changed_paths.add(catalog_path)
    if not changed_paths:
        print("Dataset pages: this branch changed no garden or grapher dataset, nothing to publish.")
        return
    changed = {f"{path.split('/')[1]}/{path.split('/')[3]}" for path in changed_paths}
    # The catalog index on disk can be out of date, so refresh the entries of the changed datasets.
    LocalCatalog(DATA_DIR, channels=PAGE_CHANNELS).reindex(
        include="|".join(re.escape(path) for path in sorted(changed_paths))
    )
    key_prefix = f"{config.DATA_API_ENV}/{STAGING_PAGES_FOLDER}"
    print(f"Dataset pages: {len(changed)} changed datasets to consider, publishing into {STAGING_BUCKET}/{key_prefix}")
    build_and_publish_catalog_pages(
        bucket=STAGING_BUCKET,
        channel=PAGE_CHANNELS,
        dry_run=dry_run,
        base_url=f"{STAGING_BASE_URL}/{key_prefix}",
        restrict_to=changed,
        key_prefix=key_prefix,
    )


def _page_keys(result: JsonLdBuildResult, catalog_dir: Path) -> tuple[list[str], list[str]]:
    """The keys to upload and the keys to delete (when they exist on R2) after a build."""
    # Datasets are served at their stable "<namespace>/<dataset>" short key rather than their dated catalog
    # path. Every page file written this time is synced (unchanged pages wrote none); the sitemap and quality
    # report live at the root.
    keys = list(result.page_keys) + [SITEMAP_FILENAME, QUALITY_REPORT_FILENAME]

    def short_key_files(entry: LatestDatasetPath) -> list[str]:
        ds = Dataset(catalog_dir / entry.catalog_path)
        return [f"{entry.short_key}/{name}" for name in page_filenames(ds)]

    # Delete the old dated-path dataset.jsonld for every currently-served dataset: it's no
    # longer written locally (superseded by the short key above), but a prior publish may
    # still have left it live on R2, which would otherwise sit around as duplicate content.
    delete_keys = [f"{entry.catalog_path}/{DATASET_JSONLD_FILENAME}" for entry in result.page_entries]
    # A rebuilt page owns only the files it wrote this time; anything else a prior build left at its short key goes
    # (a JSON-LD that failed its gates now, a workbook for a table that outgrew Excel). The sync never deletes a key
    # it uploads.
    for entry in result.page_entries:
        if entry.catalog_path not in result.pages_unchanged:
            delete_keys.extend(short_key_files(entry))
    # Datasets that newly failed a page gate (e.g. non_redistributable) must stop being served: delete both
    # the stale dated-path JSON-LD and every page file a prior publish may have written at the short key.
    delete_keys.extend(f"{entry.catalog_path}/{DATASET_JSONLD_FILENAME}" for entry in result.skipped_entries)
    for entry in result.skipped_entries:
        delete_keys.extend(short_key_files(entry))
    # Datasets archived outright (no active replacement at all) never appear above, since no on-disk version
    # of them is active, but a prior publish may still have left their files live on R2. Several
    # archived_entries can share the same short key, so dedupe. A short key that a page of this build owns
    # (a grapher step's page, when the garden step of the same name was archived) is left alone.
    delete_keys.extend(f"{entry.catalog_path}/{DATASET_JSONLD_FILENAME}" for entry in result.archived_entries)
    owned_short_keys = {entry.short_key for entry in result.page_entries}
    archived_short_key_files: set[str] = set()
    for entry in result.archived_entries:
        if entry.short_key in owned_short_keys:
            continue
        archived_short_key_files.update(short_key_files(entry))
    delete_keys.extend(sorted(archived_short_key_files))
    # Versions superseded by an active replacement under the same short key (e.g. a stale ".../latest/..."
    # build left behind after re-versioning to a dated one): only the dated path, never the short key, which
    # the active version legitimately owns instead.
    delete_keys.extend(f"{entry.catalog_path}/{DATASET_JSONLD_FILENAME}" for entry in result.superseded_entries)

    return keys, [key for key in dict.fromkeys(delete_keys) if key not in keys]


def published_build_checksum(s3: Any, bucket: str, short_key: str, key_prefix: str = "") -> str | None:
    """The build checksum in a page's published manifest, or None when there is no page yet (or no checksum)."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=_remote_key(key_prefix, f"{short_key}/{MANIFEST_FILENAME}"))
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(obj["Body"].read()).get("build_checksum")


def _remote_key(key_prefix: str, key: str) -> str:
    """The key of a file in the bucket: inside the ``key_prefix`` folder when there is one."""
    return f"{key_prefix.strip('/')}/{key}" if key_prefix else key


def sync_page_files(
    s3: Any,
    bucket: str,
    local_dir: Path,
    keys: list[str],
    delete_keys: list[str] | None = None,
    key_prefix: str = "",
) -> None:
    """Upload the changed files among ``keys`` from ``local_dir`` and delete the existing ``delete_keys``.

    Keys are relative to ``local_dir``; in the bucket they go inside the ``key_prefix`` folder, when given.

    Manifests go up last, once every other upload and delete has succeeded: a published manifest carries the
    build checksum that lets the next publish skip its page, so it must never get ahead of the page's files.
    """
    manifests = [key for key in keys if key.endswith(f"/{MANIFEST_FILENAME}")]
    others = [key for key in keys if key not in manifests]
    deletes = [key for key in dict.fromkeys(delete_keys or []) if key not in keys]
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(_upload_if_changed, s3, bucket, local_dir, key, key_prefix) for key in others]
        futures += [executor.submit(_delete_if_exists, s3, bucket, _remote_key(key_prefix, key)) for key in deletes]
        for future in futures:
            future.result()
        uploads = [executor.submit(_upload_if_changed, s3, bucket, local_dir, key, key_prefix) for key in manifests]
        for future in uploads:
            future.result()


def _upload_if_changed(s3: Any, bucket: str, local_dir: Path, key: str, key_prefix: str = "") -> None:
    local_path = local_dir / key
    if not local_path.exists():
        return
    remote_key = _remote_key(key_prefix, key)
    checksum = files.checksum_file(local_path)
    if checksum == get_remote_checksum(s3, bucket, remote_key):
        return
    print(f"  PUT {remote_key}")
    s3.upload_file(
        local_path.as_posix(), bucket, remote_key, ExtraArgs={"ACL": "public-read", "Metadata": {"md5": checksum}}
    )


def _delete_if_exists(s3: Any, bucket: str, key: str) -> None:
    if get_remote_checksum(s3, bucket, key) is not None:
        print(f"  DEL {key}")
        s3.delete_object(Bucket=bucket, Key=key)
