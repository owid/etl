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
) -> None:
    """Build catalog JSON-LD artifacts locally and sync them to R2."""
    result = build_catalog_jsonld_artifacts(
        catalog_dir=catalog_dir,
        channel=channel,
        dry_run=dry_run,
        base_url=base_url,
    )
    if dry_run:
        print(
            f"JSON-LD dry run: would emit {len(result.emitted)}, "
            f"skip {len(result.skipped)}, warn {len(result.warnings)} datasets"
        )
        return

    keys = [f"{path}/{DATASET_JSONLD_FILENAME}" for path in result.emitted]
    keys.extend([SITEMAP_FILENAME, QUALITY_REPORT_FILENAME])
    stale_keys = [f"{item.catalog_path}/{DATASET_JSONLD_FILENAME}" for item in result.skipped]
    sync_jsonld_artifacts(connect_r2(), bucket, catalog_dir, keys, delete_keys=stale_keys)


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
