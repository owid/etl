#
#  files.py
#

import hashlib
from collections import OrderedDict
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Set, TextIO, Union

import black
import yaml
from yaml.dumper import Dumper

from etl.paths import BASE_DIR


class RuntimeCache:
    """Runtime cache, we need locks because we usually run it in threads."""

    _cache: Dict[str, str]
    _locks: Dict[str, Lock]

    def __init__(self):
        self._cache = {}
        self._locks = {}

    def __contains__(self, key):
        return key in self._cache

    def __getitem__(self, key: str) -> str:
        return self._cache[key]

    def add(self, key: str, value: Any) -> None:
        if key not in self._locks:
            self._locks[key] = Lock()

        with self._locks[key]:
            self._cache[key] = value

    def clear(self) -> None:
        self._cache = {}
        self._locks = {}


CACHE_CHECKSUM_FILE = RuntimeCache()


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

    if filename not in CACHE_CHECKSUM_FILE:
        CACHE_CHECKSUM_FILE.add(filename, checksum_file_nocache(filename))

    return CACHE_CHECKSUM_FILE[filename]


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
    lines = data.splitlines()
    if len(lines) > 1:  # check for multiline string
        max_line_length = max([len(line) for line in lines])
        if max_line_length > 120:
            return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=">")
        else:
            return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    else:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data)


# dump multi-line strings correctly in YAML and add support for OrderedDict
_MyDumper.add_representer(str, _str_presenter)
_MyDumper.add_representer(
    OrderedDict,
    lambda dumper, data: dumper.represent_mapping("tag:yaml.org,2002:map", data.items()),
)


def yaml_dump(
    d: Dict[str, Any], stream: Optional[TextIO] = None, strip_lines: bool = True, replace_confusing_ascii: bool = False
) -> Optional[str]:
    """Alternative to yaml.dump which produces good looking multi-line strings and perserves ordering
    of keys. If strip_lines is True, all lines in the string will be stripped and all tabs will be
    replaced by two spaces."""
    # strip lines, otherwise YAML won't output strings in literal format
    if strip_lines:
        d = _strip_lines_in_dict(d)
    s = yaml.dump(d, stream=stream, sort_keys=False, allow_unicode=True, Dumper=_MyDumper, width=120)
    if replace_confusing_ascii:
        assert s, "replace_confusing_ascii does not work for streams"
        s = (
            s.replace("’", "'")
            .replace("‘", "'")
            .replace("“", '"')
            .replace("”", '"')
            .replace("–", "-")
            .replace("‑", "")
            .replace("…", "...")
            .replace("—", "-")
            .replace("•", "-")
            .replace(" ", " ")
        )
    return s


def _strip_lines(s: str) -> str:
    """Strip all lines in a string."""
    s = "\n".join([line.strip() for line in s.split("\n")])
    s = s.strip()

    # replace tabs by spaces, otherwise YAML won't output strings in literal format
    return s.replace("\t", "  ")


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


def apply_black_formatter_to_files(file_paths: List[Union[str, Path]]) -> None:
    """Load project configuration for black formatter, and apply formatter to a list of files.

    Parameters
    ----------
    file_paths : List[Union[str, Path]]
        Files to be reformatted using black.

    """
    # Parse black formatter configuration from pyproject.toml file.
    black_config = black.parse_pyproject_toml(BASE_DIR / "pyproject.toml")  # type: ignore
    black_mode = black.Mode(**{key: value for key, value in black_config.items() if key not in ["exclude"]})  # type: ignore
    # Apply black formatter to generated step files.
    for file_path in file_paths:
        black.format_file_in_place(src=file_path, fast=True, mode=black_mode, write_back=black.WriteBack.YES)  # type: ignore
