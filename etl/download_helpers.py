#
#  files.py
#
#  Helpers for downloading and dealing with files.
#

import hashlib
import os
import shutil
from typing import IO, Optional

import click
import requests
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TimeElapsedColumn,
    TransferSpeedColumn,
)


def blue(s: str) -> str:
    return click.style(s, fg="blue")


def log(action: str, message: str) -> None:
    action = f"{action:20s}"
    click.echo(f"{blue(action)}{message}")


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


def download(url: str, filename: str, expected_md5: Optional[str] = None, quiet: bool = False, **kwargs) -> None:
    "Download the file at the URL to the given local filename."
    # NOTE: we are not streaming to a NamedTemporaryFile because it was causing weird
    # issues one some systems, it's safer to stream directly to the file and remove it
    # if md5 don't match
    tmp_filename = filename + ".tmp"
    # Add a header to the request, to avoid a "requests.exceptions.HTTPError: 403 Client Error: Forbidden for url: ..."
    # error when accessing data files in certain URLs.
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7"
    }
    with open(tmp_filename, "wb") as f, requests.get(url, stream=True, headers=headers) as r:
        r.raise_for_status()

        md5 = _stream_to_file(r, f, **kwargs)

        if expected_md5 and md5 != expected_md5:
            if os.path.exists(filename):
                os.remove(filename)
            raise ChecksumDoesNotMatch(
                f"for file downloaded from {url}. Is your repository up to date?\n\tindex checksum = {expected_md5}\n\tdownloaded checksum = {md5}"
                ""
            )

    shutil.move(tmp_filename, filename)

    if not quiet:
        log("DOWNLOADED", f"{url} -> {filename}")


class ChecksumDoesNotMatch(Exception):
    pass
