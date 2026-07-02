"""Publish catalog JSON-LD artifacts to R2."""

from __future__ import annotations

import concurrent.futures
from pathlib import Path
from typing import Any

from owid.catalog.api.legacy import CHANNEL
from owid.catalog.s3_utils import connect_r2

from etl import config, files
from etl.catalog_jsonld.artifacts import (
    DATASET_JSONLD_FILENAME,
    QUALITY_REPORT_FILENAME,
    SITEMAP_FILENAME,
    build_catalog_jsonld_artifacts,
)
from etl.paths import DATA_DIR
from etl.publish import get_remote_checksum


def build_and_publish_catalog_jsonld(
    *,
    bucket: str = config.R2_BUCKET,
    catalog_dir: Path = DATA_DIR,
    channel: CHANNEL = "garden",
    dry_run: bool = False,
    base_url: str = "https://catalog.ourworldindata.org",
    only: set[str] | None = None,
    active_steps: set[str] | None = None,
) -> None:
    """Build catalog JSON-LD artifacts locally and sync them to R2.

    When ``only`` is given, restrict generation to datasets whose
    ``"<namespace>/<dataset>"`` is in the set (version-agnostic allowlist).

    ``active_steps`` overrides the set of active DAG step URIs used to exclude stale,
    archived on-disk builds (see :func:`etl.catalog_jsonld.artifacts.latest_dataset_paths`).
    Defaults to the real DAG; tests should pass an explicit set instead.
    """
    result = build_catalog_jsonld_artifacts(
        catalog_dir=catalog_dir,
        channel=channel,
        dry_run=dry_run,
        base_url=base_url,
        only=only,
        active_steps=active_steps,
    )
    if dry_run:
        print(
            f"JSON-LD dry run: would emit {len(result.emitted)}, "
            f"skip {len(result.skipped)}, warn {len(result.warnings)} datasets"
        )
        return

    # Datasets are now served at their stable "<namespace>/<dataset>" short key rather than
    # their dated catalog path.
    keys = [f"{entry.short_key}/{DATASET_JSONLD_FILENAME}" for entry in result.emitted_entries]
    keys.extend([SITEMAP_FILENAME, QUALITY_REPORT_FILENAME])

    # Delete the old dated-path dataset.jsonld for every currently-emitted dataset: it's no
    # longer written locally (superseded by the short key above), but a prior publish may
    # still have left it live on R2, which would otherwise sit around as duplicate content.
    delete_keys = [f"{entry.catalog_path}/{DATASET_JSONLD_FILENAME}" for entry in result.emitted_entries]
    # Also delete both locations for datasets that newly failed the quality gate: the stale
    # dated-path copy, and the live short-key copy a prior publish may have emitted — a
    # dataset that becomes ineligible (e.g. non_redistributable) must stop being served.
    delete_keys.extend(f"{item.catalog_path}/{DATASET_JSONLD_FILENAME}" for item in result.skipped)
    delete_keys.extend(f"{entry.short_key}/{DATASET_JSONLD_FILENAME}" for entry in result.skipped_entries)
    # Also delete both locations for datasets archived outright (no active replacement at
    # all) — they never appear in emitted/skipped above, since no on-disk version of them is
    # active, but a prior publish may still have left their JSON-LD live on R2. Several
    # archived_entries can share the same short key (every inactive version of a fully-dead
    # dataset is included), so dedupe with a set to avoid redundant delete calls.
    delete_keys.extend(f"{entry.catalog_path}/{DATASET_JSONLD_FILENAME}" for entry in result.archived_entries)
    delete_keys.extend({f"{entry.short_key}/{DATASET_JSONLD_FILENAME}" for entry in result.archived_entries})
    # Also delete the old dated path for versions superseded by an active replacement under
    # the same short key (e.g. a stale ".../latest/..." build left behind after re-versioning
    # to a dated one) — only the dated path, never the short key, which the active version
    # legitimately owns instead.
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
