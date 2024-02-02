#
#  files.py
#

import hashlib
import io
import os
import re
import subprocess
import time
from collections import OrderedDict
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Generator, List, Optional, Set, TextIO, Union

import ruamel.yaml
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

TEXT_CHARS = bytes(range(32, 127)) + b"\n\r\t\f\b"
DEFAULT_CHUNK_SIZE = 512


def dos2unix(data: bytes) -> bytes:
    return data.replace(b"\r\n", b"\n")


def istextblock(block: bytes) -> bool:
    if not block:
        # An empty file is considered a valid text file
        return True

    if b"\x00" in block:
        # Files with null bytes are binary
        return False

    # Use translate's 'deletechars' argument to efficiently remove all
    # occurrences of TEXT_CHARS from the block
    nontext = block.translate(None, TEXT_CHARS)
    return float(len(nontext)) / len(block) <= 0.30


def checksum_str(s: str) -> str:
    "Return the md5 hex digest of the string."
    return hashlib.md5(dos2unix(s.encode())).hexdigest()


def checksum_file_nocache(filename: Union[str, Path]) -> str:
    "Return the md5 hex digest of the file without using cache."
    chunk_size = 2**20
    _hash = hashlib.md5()
    with open(filename, "rb") as istream:
        chunk = istream.read(chunk_size)
        while chunk:
            if istextblock(chunk[:DEFAULT_CHUNK_SIZE]):
                chunk = dos2unix(chunk)

            _hash.update(chunk)
            chunk = istream.read(chunk_size)

    return _hash.hexdigest()


def checksum_file(filename: Union[str, Path]) -> str:
    "Return the md5 hex digest of the file contents."
    if isinstance(filename, Path):
        filename = filename.as_posix()

    mtime = os.path.getmtime(filename)
    key = f"{filename}-{mtime}"

    if filename not in CACHE_CHECKSUM_FILE:
        # Special case for regions.yml, we want to ignore the 'aliases' key
        if os.path.basename(filename) == "regions.yml":
            with open(filename, "r") as f:
                s = f.read()

            # Regular expression to match the 'aliases' and its list
            regex_pattern = r"  aliases:\n(\s+-[^\n]*\n?)*"
            s = re.sub(regex_pattern, "", s)

            checksum = checksum_str(s.strip())
        else:
            checksum = checksum_file_nocache(filename)
        CACHE_CHECKSUM_FILE.add(key, checksum)

    return CACHE_CHECKSUM_FILE[key]


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
    def increase_indent(self, flow=False, indentless=False):
        return super(_MyDumper, self).increase_indent(flow, False)


def _str_presenter(dumper: Any, data: Any) -> Any:
    lines = data.splitlines()
    # If there are multiple lines, or there is a line that is longer than 120 characters, use the literal style.
    # NOTE: Here the 120 is a bit arbitrary. This is the default length of our lines in the code, but once written
    # to YAML, the lines will be longer because of the indentation. So, we could use a smaller number here.
    if (len(lines) > 1) or (max([len(line) for line in lines]) > 120):
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
    d: Dict[str, Any],
    stream: Optional[TextIO] = None,
    strip_lines: bool = True,
    replace_confusing_ascii: bool = False,
    width: int = 120,
) -> Optional[str]:
    """Alternative to yaml.dump which produces good looking multi-line strings and perserves ordering
    of keys. If strip_lines is True, all lines in the string will be stripped and all tabs will be
    replaced by two spaces."""
    # strip lines, otherwise YAML won't output strings in literal format
    if strip_lines:
        d = _strip_lines_in_dict(d)
    s = yaml.dump(d, stream=stream, sort_keys=False, allow_unicode=True, Dumper=_MyDumper, width=width)
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


def ruamel_dump(d: Dict[str, Any]) -> str:
    """Dump dictionary with a consistent style using ruamel.yaml."""
    yml = ruamel.yaml.YAML()
    yml.indent(mapping=2, sequence=4, offset=2)
    # prevent line-wrap
    yml.width = 4096

    stream = io.StringIO()
    yml.dump(d, stream)
    return stream.getvalue()


def ruamel_load(f: io.TextIOWrapper) -> Dict[str, Any]:
    return ruamel.yaml.load(f, Loader=ruamel.yaml.RoundTripLoader, preserve_quotes=True)


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


def apply_ruff_formatter_to_files(file_paths: List[Union[str, Path]]) -> None:
    """Apply ruff formatter to a list of files.

    Parameters
    ----------
    file_paths : List[Union[str, Path]]
        Files to be reformatted using ruff.

    """
    pyproject_path = BASE_DIR / "pyproject.toml"
    subprocess.run(["ruff", "format", "--config", str(pyproject_path)] + [str(fp) for fp in file_paths], check=True)


def _mtime_mapping(path: Path) -> Dict[Path, float]:
    return {f: f.stat().st_mtime for f in path.rglob("*") if f.is_file() and "__pycache__" not in f.parts}


def watch_folder(path: Path) -> Generator[Path, None, None]:
    """Watch folder and yield on any changes."""
    last_seen = _mtime_mapping(path)

    while True:
        time.sleep(1)

        current_files = _mtime_mapping(path)

        # Check for modifications
        for f, mtime in current_files.items():
            # new file
            if f not in last_seen:
                yield f
                break
            # updated file
            else:
                if last_seen[f] != mtime:
                    yield f
                    break

        last_seen = current_files
