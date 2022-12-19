#
#  files.py
#

import hashlib
from collections import OrderedDict
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Set, TextIO, Union

import yaml
from yaml.dumper import Dumper

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


class _MyDumper(Dumper):
    pass


def _str_presenter(dumper: Any, data: Any) -> Any:
    if len(data.splitlines()) > 1:  # check for multiline string
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


# dump multi-line strings correctly in YAML and add support for OrderedDict
_MyDumper.add_representer(str, _str_presenter)
_MyDumper.add_representer(
    OrderedDict,
    lambda dumper, data: dumper.represent_mapping("tag:yaml.org,2002:map", data.items()),
)


def yaml_dump(d: Dict[str, Any], stream: Optional[TextIO] = None, strip_lines: bool = True) -> Optional[str]:
    """Alternative to yaml.dump which produces good looking multi-line strings and perserves ordering
    of keys."""
    # strip lines, otherwise YAML won't output strings in literal format
    if strip_lines:
        d = _strip_lines_in_dict(d)
    return yaml.dump(d, stream=stream, sort_keys=False, allow_unicode=True, Dumper=_MyDumper)


def _strip_lines(s: str) -> str:
    """Strip all lines in a string."""
    s = "\n".join([line.strip() for line in s.split("\n")])
    return s.strip()


def _strip_lines_in_dict(d: Any) -> Any:
    """Recursively go through dictionary and strip lines of all encountered strings."""
    if isinstance(d, str):
        return _strip_lines(d)
    elif isinstance(d, list):
        return [_strip_lines_in_dict(e) for e in d]
    elif isinstance(d, dict):
        return {k: _strip_lines_in_dict(v) for k, v in d.items()}
    else:
        return d
