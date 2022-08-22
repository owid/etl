#
#  files.py
#

import hashlib
from pathlib import Path
from typing import List, Set, Union


def checksum_file(filename: Union[str, Path]) -> str:
    "Return the md5 hex digest of the file contents."
    if isinstance(filename, Path):
        filename = filename.as_posix()

    chunk_size = 2**20
    _hash = hashlib.md5()
    with open(filename, "rb") as istream:
        chunk = istream.read(chunk_size)
        while chunk:
            _hash.update(chunk)
            chunk = istream.read(chunk_size)

    return _hash.hexdigest()


def checksum_str(s: str) -> str:
    "Return the md5 hex digest of the string."
    return hashlib.md5(s.encode()).hexdigest()


def walk(
    folder: Path, ignore_set: Set[str] = {"__pycache__", ".ipynb_checkpoints"}
) -> List[Path]:
    paths = []
    for p in folder.iterdir():
        if p.is_dir():
            paths.extend(walk(p))
            continue

        if p.name not in ignore_set:
            paths.append(p)

    return paths
