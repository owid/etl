#
#  files.py
#
#  Helpers for downloading and dealing with files.
#

import hashlib
import json
import os
import shutil
from os import path, walk
from typing import IO, Iterator, Optional, Tuple

import requests
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TimeElapsedColumn,
    TransferSpeedColumn,
)

from .ui import log


def _create_progress_bar() -> Progress:
    """Create a fancy progress bar to use for display of download progress.
    Based on https://github.com/Textualize/rich/blob/ae1ee4efa1742e7a91ffd4870ba677aad70ff036/examples/downloader.py"""
    return Progress(
        "[progress.description]{task.description}",
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeElapsedColumn(),
    )


def _stream_to_file(
    r: requests.Response,
    file: IO[bytes],
    chunk_size: int = 2**14,
    progress_bar_min_bytes: int = 2**25,
) -> str:
    """Stream the response to the file, returning the checksum.
    :param progress_bar_min_bytes: Minimum number of bytes to display a progress bar for. Default is 32MB
    """
    # check header to get content length, in bytes
    total_length = int(r.headers.get("content-length", 0))

    md5 = hashlib.md5()

    streamer = r.iter_content(chunk_size=chunk_size)
    display_progress = total_length > progress_bar_min_bytes
    if display_progress:
        progress = _create_progress_bar()
        progress.start()
        task_id = progress.add_task("Downloading", total=total_length)

    for chunk in streamer:  # 16k
        file.write(chunk)
        md5.update(chunk)
        if display_progress:
            progress.update(task_id, advance=len(chunk))  # type: ignore

    if display_progress:
        progress.stop()  # type: ignore

    return md5.hexdigest()


def download(url: str, filename: str, expected_md5: Optional[str] = None, quiet: bool = False) -> None:
    "Download the file at the URL to the given local filename."
    # NOTE: we are not streaming to a NamedTemporaryFile because it was causing weird
    # issues one some systems, it's safer to stream directly to the file and remove it
    # if md5 don't match
    tmp_filename = filename + ".tmp"
    with open(tmp_filename, "wb") as f, requests.get(url, stream=True) as r:
        r.raise_for_status()

        md5 = _stream_to_file(r, f)

        if expected_md5 and md5 != expected_md5:
            if os.path.exists(filename):
                os.remove(filename)
            raise ChecksumDoesNotMatch(
                f"for file downloaded from {url}. Is your walden repository up to date?\n\twalden index checksum = {expected_md5}\n\tdownloaded checksum = {md5}"
                ""
            )

    shutil.move(tmp_filename, filename)

    if not quiet:
        log("DOWNLOADED", f"{url} -> {filename}")


def checksum(local_path: str) -> str:
    md5 = hashlib.md5()
    chunk_size = 2**20  # 1MB
    with open(local_path, "rb") as f:
        chunk = f.read(chunk_size)
        while chunk:
            md5.update(chunk)
            chunk = f.read(chunk_size)

    return md5.hexdigest()


def iter_docs(folder) -> Iterator[Tuple[str, dict]]:
    "Iterate over the JSON documents in the catalog."
    for filename in sorted(iter_json(folder)):
        try:
            with open(filename) as istream:
                yield filename, json.load(istream)

        except json.decoder.JSONDecodeError:
            raise RecordWithInvalidJSON(filename)


def iter_json(base_dir: str) -> Iterator[str]:
    for dirname, _, filenames in walk(base_dir):
        for filename in filenames:
            if filename.endswith(".json"):
                yield path.join(dirname, filename)


def verify_md5(filename: str, expected_md5: str) -> None:
    "Throw an exception if the filename does not match the checksum."
    md5 = checksum(filename)
    if md5 != expected_md5:
        raise ChecksumDoesNotMatch(filename)


class RecordWithInvalidJSON(Exception):
    pass


class ChecksumDoesNotMatch(Exception):
    pass
