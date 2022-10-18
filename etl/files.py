#
#  files.py
#

import hashlib
from pathlib import Path
from threading import Lock
from typing import Dict, List, Set, Union

# runtime cache, we need locks because we usually run it in threads
cache_md5: Dict[str, str] = {}
cache_md5_locks: Dict[str, Lock] = {}


def checksum_file_nocache(filename: Union[str, Path]) -> str:
    "Return the md5 hex digest of the file without using cache."
    chunk_size = 2**20
    _hash = hashlib.md5()
    with open(filename, "rb") as istream:
        chunk = istream.read(chunk_size)
        while chunk:
            _hash.update(chunk)
            chunk = istream.read(chunk_size)

    return _hash.hexdigest()


def checksum_file(filename: Union[str, Path]) -> str:
    "Return the md5 hex digest of the file contents."
    if isinstance(filename, Path):
        filename = filename.as_posix()

    if filename not in cache_md5:
        cache_md5_locks[filename] = Lock()

        with cache_md5_locks[filename]:
            cache_md5[filename] = checksum_file_nocache(filename)

    return cache_md5[filename]


def checksum_str(s: str) -> str:
    "Return the md5 hex digest of the string."
    return hashlib.md5(s.encode()).hexdigest()


def walk(folder: Path, ignore_set: Set[str] = {"__pycache__", ".ipynb_checkpoints"}) -> List[Path]:
    paths = []
    for p in folder.iterdir():
        if p.is_dir():
            if p.name not in ignore_set:
                paths.extend(walk(p, ignore_set=ignore_set))
            continue
        else:
            paths.append(p)

    return paths
