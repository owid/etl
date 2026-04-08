#!/usr/bin/env python
"""
Delete private dataset DATA files from the public owid-catalog bucket.

This script identifies private datasets and removes their data files (.feather, .parquet, .csv)
from the public bucket, while keeping metadata files (.meta.json, index.json) intact.

Usage:
    .venv/bin/python scripts/delete_private_from_public_bucket.py --dry-run
    .venv/bin/python scripts/delete_private_from_public_bucket.py
"""

from pathlib import Path
from typing import Any, Dict, Iterator, List

import click
from owid.catalog.api.legacy import CHANNEL, LocalCatalog
from owid.catalog.s3_utils import connect_r2

from etl import config
from etl.paths import DATA_DIR


def is_metadata_file(filename: str) -> bool:
    """Check if a file is a metadata file (should stay in public bucket)."""
    return filename.endswith(".meta.json") or filename.endswith("index.json")


def walk_s3(s3: Any, bucket: str, path: str) -> Iterator[Dict[str, Any]]:
    """Walk all objects in an S3 bucket with a given prefix."""
    objs = s3.list_objects(Bucket=bucket, Prefix=path, MaxKeys=100)
    yield from objs.get("Contents", [])

    while objs["IsTruncated"] and objs.get("Contents"):
        marker = objs.get("NextMarker", objs["Contents"][-1]["Key"])
        objs = s3.list_objects(Bucket=bucket, Prefix=path, Marker=marker)
        yield from objs.get("Contents", [])


def delete_files(s3: Any, bucket: str, keys: List[str], dry_run: bool) -> None:
    """Delete files from S3 in batches of 1000."""
    if dry_run or not keys:
        return

    while keys:
        chunk = keys[:1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
        )
        keys = keys[1000:]


@click.command()
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Preview files to delete without actually deleting them.",
)
@click.option(
    "--channel",
    "-c",
    multiple=True,
    type=click.Choice(CHANNEL.__args__),
    default=CHANNEL.__args__,
    help="Process only selected channel(s).",
)
def main(dry_run: bool, channel: tuple) -> None:
    """Delete private dataset data files from the public R2 bucket."""
    catalog = Path(DATA_DIR)
    local = LocalCatalog(catalog)
    s3 = connect_r2()
    bucket = config.R2_BUCKET

    total_files = 0
    total_bytes = 0

    for ch in channel:
        print(f"\n=== Channel: {ch} ===")

        for ds in local.iter_datasets(ch):
            if ds.metadata.is_public:
                continue

            # This is a private dataset
            path = Path(ds.path).relative_to(catalog).as_posix()
            if not path.endswith("/"):
                path += "/"

            # Find all data files for this dataset in the public bucket
            files_to_delete = []
            for obj in walk_s3(s3, bucket, path):
                key = obj["Key"]
                # Only delete data files, keep metadata
                if not is_metadata_file(key):
                    files_to_delete.append(key)
                    size_mb = obj.get("Size", 0) / (1024 * 1024)
                    print(f"  DEL {key} ({size_mb:.2f} MB)")
                    total_files += 1
                    total_bytes += obj.get("Size", 0)

            if files_to_delete:
                print(f"  -> {path} ({len(files_to_delete)} files)")
                delete_files(s3, bucket, files_to_delete, dry_run)

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Summary:")
    print(f"  Files {'to delete' if dry_run else 'deleted'}: {total_files}")
    print(f"  Total size: {total_bytes / (1024 * 1024):.2f} MB")

    if dry_run and total_files > 0:
        print("\nRun without --dry-run to actually delete these files.")


if __name__ == "__main__":
    main()
