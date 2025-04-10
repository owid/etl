#
#  NOTE: the only allowed dependencies are etl.config, etl.paths
#

import hashlib
import io
import json
import os
import re
import subprocess
import time
from collections import OrderedDict
from functools import cache
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Generator, List, Optional, Set, TextIO, Union, cast, overload
from urllib.parse import urljoin

import jsonref
import pandas as pd
import requests
import ruamel.yaml
import structlog
import yaml
from ruamel.yaml import YAML
from yaml.dumper import Dumper

from etl.config import TLS_VERIFY
from etl.paths import BASE_DIR

log = structlog.get_logger()


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


def checksum_str(s: str) -> str:
    "Return the md5 hex digest of the string."
    return hashlib.md5(s.encode()).hexdigest()


def checksum_dict(d: Dict[str, Any]) -> str:
    "Return the md5 hex digest of the dictionary."
    return checksum_str(json.dumps(d, default=str))


def checksum_file_nocache(filename: Union[str, Path]) -> str:
    """Return the md5 hex digest of the file without using cache.

    Python 3.11 has a built-in function for this. It could be rewritten as
    ```
    with open(filename, "rb") as f:
        return hashlib.file_digest(f, "md5").hexdigest()
    ```
    """
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


def checksum_df(df: pd.DataFrame, index=True) -> str:
    """Return the md5 hex digest of dataframe. It is only useful for large dataframes. For smaller
    ones (<1M rows), it's better to use checksum_dict or checksum_str.
    """
    # NOTE: I tried joblib.hash, but it was much slower than pandas hash
    return hashlib.md5(pd.util.hash_pandas_object(df, index=index).values).hexdigest()


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
    if (len(lines) > 1) or (len(lines) > 0 and max([len(line) for line in lines]) > 120):
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    else:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data)


# dump multi-line strings correctly in YAML and add support for OrderedDict
_MyDumper.add_representer(str, _str_presenter)
_MyDumper.add_representer(
    OrderedDict,
    lambda dumper, data: dumper.represent_mapping("tag:yaml.org,2002:map", data.items()),
)


@overload
def yaml_dump(
    d: Dict[str, Any],
    stream: None = None,
    strip_lines: bool = True,
    replace_confusing_ascii: bool = False,
    width: int = 120,
) -> str: ...


@overload
def yaml_dump(
    d: Dict[str, Any], stream: TextIO, strip_lines: bool = True, replace_confusing_ascii: bool = False, width: int = 120
) -> None: ...


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


def yaml_load(f: io.TextIOWrapper) -> Dict[str, Any]:
    return yaml.safe_load(f)


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
    yaml = YAML(typ="rt")  # Create a YAML object with round-trip type
    yaml.preserve_quotes = True
    return yaml.load(f)  # Load the content using the new API


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

    # Add uv to the path, it causes problems in Buildkite
    env = os.environ.copy()
    env["PATH"] = os.path.expanduser("~/.cargo/bin") + ":" + env["PATH"]

    subprocess.run(
        ["uv", "run", "ruff", "format", "--config", str(pyproject_path)] + [str(fp) for fp in file_paths],
        check=True,
        env=env,
    )


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


def upload_file_to_server(local_file_path: Path, target: str) -> None:
    """
    Upload a local file to a remote server using scp.

    :param local_file_path: Path to the local file to upload.
    :param target: The target destination on the remote server in the format 'user@host:/remote/path'.
                   Example: 'owid@staging-site-explorer-step:~/owid-content/explorers/'
    """
    # Check if the local file exists
    if not local_file_path.is_file():
        raise FileNotFoundError(f"The file {local_file_path} does not exist.")

    # Validate the target format (basic check)
    if "@" not in target or ":" not in target:
        raise ValueError(f"The target '{target}' is not properly formatted. Expected format: 'user@host:/remote/path'.")

    try:
        # Construct the scp command
        scp_command = ["scp", str(local_file_path), target]

        # Execute the command
        subprocess.run(scp_command, check=True, text=True, capture_output=True)
        log.info("file.uploaded", target=target, path=local_file_path)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to upload file {local_file_path} to {target}") from e


def create_folder(folder_path: str | Path) -> None:
    """Create a folder if it does not exist."""
    if isinstance(folder_path, str):
        folder_path = Path(folder_path)
    if not folder_path.exists():
        folder_path.mkdir(parents=True, exist_ok=True)


def download_file_from_server(
    local_file_path: Path,
    target: str,
) -> None:
    """
    Download a remote file from a server to a local path using scp.

    :param target: The source file on the remote server in the format 'user@host:/remote/path'.
                   Example: 'user@example.com:/remote/path/to/file.txt'
    :param local_file_path: Path where the downloaded file will be saved locally.
    """
    # Validate the target format (basic check)
    if "@" not in target or ":" not in target:
        raise ValueError(f"The target '{target}' is not properly formatted. Expected format: 'user@host:/remote/path'.")

    # Ensure the parent directory of the local file path exists
    if not local_file_path.parent.exists():
        local_file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Construct the scp command
        scp_command = ["scp", target, str(local_file_path)]

        # Execute the command
        subprocess.run(scp_command, check=True, text=True, capture_output=True)
        log.info("file.downloaded", target=target, path=local_file_path)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to download file {target} to {local_file_path}") from e


def run_command_on_server(
    ssh_target: str,
    command: str,
) -> str:
    """
    Run a command on a remote server via SSH using subprocess.

    :param ssh_target: The SSH target in the format 'user@hostname'.
    :param command: The command to execute on the remote server.
    :return: The stdout output from the command.
    """
    try:
        # Construct the SSH command
        ssh_command = ["ssh", ssh_target, command]

        # Execute the command
        result = subprocess.run(
            ssh_command,
            check=True,
            text=True,
            capture_output=True,
        )

        log.info("command.executed", target=ssh_target, command=command)
        return result.stdout
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to execute command on {ssh_target}:\n{e.stderr}") from e


def read_json_schema(path: Union[Path, str]) -> Dict[str, Any]:
    """Read JSON schema with resolved references."""
    path = Path(path)

    # pathlib does not append trailing slashes, but jsonref needs that.
    base_dir_url = path.parent.absolute().as_uri() + "/"
    base_file_url = urljoin(base_dir_url, path.name)
    with path.open("r") as f:
        dix = jsonref.loads(f.read(), base_uri=base_file_url, lazy_load=False)
        return cast(Dict[str, Any], dix)


@cache
def get_schema_from_url(schema_url: str) -> dict:
    """Get the schema of a chart configuration. Schema URL is saved in config["$schema"] and looks like:

    https://files.ourworldindata.org/schemas/grapher-schema.007.json

    More details on available versions can be found
    at https://github.com/owid/owid-grapher/tree/master/packages/%40ourworldindata/grapher/src/schema

    Returns
    -------
    Dict[str, Any]
        Schema of a chart configuration.
    """
    return requests.get(schema_url, timeout=20, verify=TLS_VERIFY).json()
