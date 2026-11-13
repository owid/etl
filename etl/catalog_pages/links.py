"""Extra links from the indicators of a dataset page to grapher indicators that hold the same data.

Grapher finds the charts of a page from the page's own indicators, so a page built from a grapher step needs nothing
here. A step whose columns are copies of indicators in other grapher datasets (a garden step that compiles several
datasets, for example) lists those indicators in a file next to the step, ``<short_name>.indicator_links.yml``::

    owid_energy:                      # table name
      coal_energy_twh:                # column short name
        - grapher/energy/*/energy_mix/energy_mix#coal_twh

A path is ``grapher/<namespace>/<version>/<dataset>/<table>#<column>``; the version is ``*`` (any version) or a fixed
one. The paths are published in the manifest as written: it is Grapher that matches them against its indicators.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml
from owid.catalog.schema_org import ENTITY_TIME_DIMENSIONS

from etl.paths import DATA_DIR, STEP_DIR

LINKS_SUFFIX = ".indicator_links.yml"
ANY_VERSION = "*"
_PATH_RE = re.compile(
    r"^grapher/(?P<namespace>[^/#*]+)/(?P<version>[^/#]+)/(?P<dataset>[^/#*]+)/(?P<table>[^/#*]+)#(?P<column>[^/#*]+)$"
)
_STEP_RE = re.compile(r"^data(?:-private)?://grapher/(?P<namespace>[^/]+)/(?P<version>[^/]+)/(?P<dataset>[^/]+)$")

# Links of a dataset: table name -> column name -> catalog paths.
IndicatorLinks = dict[str, dict[str, list[str]]]


def links_path(catalog_path: str, steps_dir: Path | None = None) -> Path:
    """The links file of the step that builds the dataset at ``catalog_path`` (it may not exist)."""
    return (steps_dir or STEP_DIR / "data") / f"{catalog_path}{LINKS_SUFFIX}"


def load_indicator_links(
    catalog_path: str,
    columns_by_table: dict[str, list[str]],
    active_steps: set[str],
    steps_dir: Path | None = None,
    catalog_dir: Path | None = None,
) -> tuple[IndicatorLinks, list[str]]:
    """Read the links file of a dataset, and return its usable links and the problems found in it.

    An entry for a table or column that the dataset does not have is left out, and so is a path that is not a
    grapher catalog path. A path that matches no active grapher step is kept as written, since it harms nothing,
    and so is a path whose table or column is not in the grapher dataset built in ``catalog_dir`` (a grapher
    dataset that is not built there is not checked).
    All of them are returned as problems, for the quality report. A dataset without a links file has no links.
    """
    path = links_path(catalog_path, steps_dir)
    if not path.exists():
        return {}, []
    content = yaml.safe_load(path.read_text()) or {}
    if not isinstance(content, dict):
        raise ValueError(f"{path.name} must map table names to columns.")

    grapher_steps = _grapher_steps(active_steps)
    built_tables: dict[Path, dict | None] = {}
    links: IndicatorLinks = {}
    problems: list[str] = []
    for table, columns in content.items():
        if table not in columns_by_table:
            problems.append(f"unknown table '{table}'")
            continue
        if not isinstance(columns, dict):
            raise ValueError(f"{path.name}: table '{table}' must map column names to lists of paths.")
        for column, paths in columns.items():
            if column not in columns_by_table[table]:
                problems.append(f"unknown column '{table}#{column}'")
                continue
            if not isinstance(paths, list) or not all(isinstance(item, str) for item in paths):
                raise ValueError(f"{path.name}: '{table}#{column}' must be a list of paths.")
            usable = []
            for item in paths:
                match = _PATH_RE.match(item)
                if match is None:
                    problems.append(f"not a grapher catalog path '{item}'")
                    continue
                versions = _active_versions(match, grapher_steps)
                if not versions:
                    problems.append(f"no active grapher step for '{item}'")
                else:
                    missing = _missing_in_built_datasets(match, versions, catalog_dir or DATA_DIR, built_tables)
                    if missing:
                        problems.append(f"no {missing} for '{item}'")
                usable.append(item)
            if usable:
                links.setdefault(table, {})[column] = usable
    return links, problems


def _grapher_steps(active_steps: set[str]) -> set[tuple[str, str, str]]:
    """The (namespace, version, dataset) of every active grapher step."""
    matches = (_STEP_RE.match(step) for step in active_steps)
    return {(match["namespace"], match["version"], match["dataset"]) for match in matches if match}


def _active_versions(match: re.Match[str], grapher_steps: set[tuple[str, str, str]]) -> list[str]:
    """The versions of the active grapher steps that a path matches."""
    return sorted(
        version
        for namespace, version, dataset in grapher_steps
        if (namespace, dataset) == (match["namespace"], match["dataset"]) and match["version"] in (ANY_VERSION, version)
    )


def _missing_in_built_datasets(
    match: re.Match[str], versions: list[str], catalog_dir: Path, built_tables: dict[Path, dict | None]
) -> str | None:
    """What a path names that the grapher datasets built in ``catalog_dir`` lack: its table, or its column.

    None if one of these datasets has both, or if none of them is built there. ``built_tables`` keeps the tables
    already read.
    """
    tables = []
    is_built = False
    for version in versions:
        dataset_dir = catalog_dir / "grapher" / match["namespace"] / version / match["dataset"]
        if not (dataset_dir / "index.json").exists():
            continue
        is_built = True
        meta_path = dataset_dir / f"{match['table']}.meta.json"
        if meta_path not in built_tables:
            built_tables[meta_path] = json.loads(meta_path.read_text()) if meta_path.exists() else None
        table = built_tables[meta_path]
        if table is not None:
            tables.append(table)
    if not is_built:
        return None
    if not tables:
        return f"table '{match['table']}' in the grapher dataset"
    column = match["column"]
    for table in tables:
        columns = list(table.get("fields", {}))
        if column in columns:
            return None
        # An indicator of a table with dimensions is named after its column, followed by its dimension values.
        has_dimensions = bool(set(table.get("primary_key") or []) - ENTITY_TIME_DIMENSIONS)
        if has_dimensions and any(column.startswith(f"{name}__") for name in columns):
            return None
    return f"column '{column}' in the grapher table"
