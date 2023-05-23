#
#  __init__.py
#  steps
#
import concurrent.futures
import graphlib
import hashlib
import os
import re
import subprocess
import sys
import tempfile
import warnings
from collections import defaultdict
from dataclasses import dataclass, field
from glob import glob
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Protocol, Set, Union, cast
from urllib.parse import urlparse

import pandas as pd
import requests
import structlog
import yaml
from dvc.dvcfile import load_file
from dvc.repo import Repo

from etl.db import get_engine

# smother deprecation warnings by papermill
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import papermill as pm

from owid import catalog
from owid.walden import CATALOG as WALDEN_CATALOG
from owid.walden import Dataset as WaldenDataset

from etl import backport_helpers, config, files, git
from etl import grapher_helpers as gh
from etl import paths
from etl.grapher_import import (
    DatasetUpsertResult,
    VariableUpsertResult,
    cleanup_ghost_sources,
    cleanup_ghost_variables,
    fetch_db_checksum,
    set_dataset_checksum_and_editedAt,
    upsert_dataset,
    upsert_table,
)

log = structlog.get_logger()

Graph = Dict[str, Set[str]]
DAG = Dict[str, Any]

dvc_lock = Lock()

DVC_REPO = Repo(paths.BASE_DIR)


def compile_steps(
    dag: DAG,
    includes: Optional[List[str]] = None,
    excludes: Optional[List[str]] = None,
    downstream: bool = False,
    only: bool = False,
) -> List["Step"]:
    """
    Return the list of steps which, if executed in order, mean that every
    step has its dependencies ready for it.
    """
    includes = includes or []
    excludes = excludes or []

    # make sure each step runs after its dependencies
    steps = to_dependency_order(dag, includes, excludes, downstream=downstream, only=only)

    # parse the steps into Python objects
    return [parse_step(name, dag) for name in steps]


def to_dependency_order(
    dag: DAG,
    includes: List[str],
    excludes: List[str],
    downstream: bool = False,
    only: bool = False,
) -> List[str]:
    """
    Organize the steps in dependency order with a topological sort. In other words,
    the resulting list of steps is a valid ordering of steps such that no step is run
    before the steps it depends on. Note: this ordering is not necessarily unique.
    """
    subgraph = filter_to_subgraph(dag, includes, downstream=downstream, only=only) if includes else dag
    in_order = list(graphlib.TopologicalSorter(subgraph).static_order())

    # filter out explicit excludes
    filtered = [s for s in in_order if not any(re.findall(pattern, s) for pattern in excludes)]

    return filtered


def filter_to_subgraph(graph: Graph, includes: Iterable[str], downstream: bool = False, only: bool = False) -> Graph:
    """
    Filter the full graph to only the included nodes, and all their dependencies.

    If the downstream flag is true, also include downstream dependencies (ie steps
    that depend on the included steps), as well as their OWN dependencies.

    Assumes that the graph is organized dependent -> dependency (A -> B means A is
    dependent on B).
    """
    all_steps = graph_nodes(graph)
    included = {s for s in all_steps if any(re.findall(pattern, s) for pattern in includes)}

    if only:
        # Do not search for dependencies, only include explicitly selected nodes
        return {step: set() for step in included}

    if downstream:
        # Reverse the graph to find all nodes dependent on included nodes (forward deps)
        forward_deps = set(traverse(reverse_graph(graph), included))
        included = included.union(forward_deps)

    # Now traverse the other way to find all dependencies of included nodes (backward deps)
    return traverse(graph, included)


def traverse(graph: Graph, nodes: Set[str]) -> Graph:
    """
    Use BFS to find all nodes in a graph that are reachable from a given
    subset of nodes.
    """
    reachable: Graph = defaultdict(set)
    to_visit = nodes.copy()

    while to_visit:
        node = to_visit.pop()
        if node in reachable:
            continue  # already visited
        reachable[node] = set(graph.get(node, set()))
        to_visit = to_visit.union(reachable[node])

    return dict(reachable)


def load_dag(filename: Union[str, Path] = paths.DEFAULT_DAG_FILE) -> Dict[str, Any]:
    return _load_dag(filename, {})


def _load_dag(filename: Union[str, Path], prev_dag: Dict[str, Any]):
    """
    Recursive helper to 1) load a dag itself, and 2) load any sub-dags
    included in the dag via 'include' statements
    """
    dag_yml = _load_dag_yaml(str(filename))
    curr_dag = _parse_dag_yaml(dag_yml)
    curr_dag.update(prev_dag)

    for sub_dag_filename in dag_yml.get("include", []):
        sub_dag = _load_dag(paths.BASE_DIR / sub_dag_filename, curr_dag)
        curr_dag.update(sub_dag)

    return curr_dag


def _load_dag_yaml(filename: str) -> Dict[str, Any]:
    with open(filename) as istream:
        return yaml.safe_load(istream)


def _parse_dag_yaml(dag: Dict[str, Any]) -> Dict[str, Any]:
    steps = dag["steps"] or {}

    return {node: set(deps) if deps else set() for node, deps in steps.items()}


def reverse_graph(graph: Graph) -> Graph:
    """
    Invert the edge direction of a graph.
    """
    g = defaultdict(set)
    for dest, sources in graph.items():
        for source in sources:
            g[source].add(dest)

        # trigger creation of dest if it's not there
        g[dest]

    return dict(g)


def graph_nodes(graph: Graph) -> Set[str]:
    all_steps = set(graph)
    for children in graph.values():
        all_steps.update(children)
    return all_steps


def parse_step(step_name: str, dag: Dict[str, Any]) -> "Step":
    "Convert each step's name into a step object that we can run."
    parts = urlparse(step_name)
    step_type = parts.scheme
    path = parts.netloc + parts.path
    dependencies = [parse_step(s, dag) for s in dag.get(step_name, [])]

    step: Step
    if step_type == "data":
        step = DataStep(path, dependencies)

    elif step_type == "walden":
        step = WaldenStep(path)

    elif step_type == "snapshot":
        step = SnapshotStep(path)

    elif step_type == "github":
        step = GithubStep(path)

    elif step_type == "etag":
        step = ETagStep(path)

    elif step_type == "grapher":
        step = GrapherStep(path, dependencies)

    elif step_type == "backport":
        step = BackportStep(path, dependencies)

    elif step_type == "data-private":
        step = DataStepPrivate(path, dependencies)

    elif step_type == "walden-private":
        step = WaldenStepPrivate(path)

    elif step_type == "backport-private":
        step = BackportStepPrivate(path, dependencies)

    elif step_type == "snapshot-private":
        step = SnapshotStepPrivate(path)

    else:
        raise Exception(f"no recipe for executing step: {step_name}")

    return step


def extract_step_attributes(step: str) -> Dict[str, str]:
    """Extract attributes of a step from its name in the dag.

    Parameters
    ----------
    step : str
        Step (as it appears in the dag).

    Returns
    -------
    step : str
        Step (as it appears in the dag).
    kind : str
        Kind of step (namely, 'public' or 'private').
    channel: str
        Channel (e.g. 'meadow').
    namespace: str
        Namespace (e.g. 'energy').
    version: str
        Version (e.g. '2023-01-26').
    name: str
        Short name of the dataset (e.g. 'primary_energy').
    identifier : str
        Identifier of the step that is independent of the kind and of the version of the step.

    """
    # Extract the prefix (whatever is on the left of the '://') and the root of the step name.
    prefix, root = step.split("://")

    # Field 'kind' informs whether the dataset is public or private.
    if "private" in prefix:
        kind = "private"
    else:
        kind = "public"

    # From now on we remove the 'public' or 'private' from the prefix.
    prefix = prefix.split("-")[0]

    if prefix in ["etag", "github"]:
        # Special kinds of steps.
        channel = "etag"
        namespace = "etag"
        version = "latest"
        name = root
        identifier = root
    elif prefix in ["snapshot", "walden"]:
        # Ingestion steps.
        channel = prefix

        # Extract attributes from root of the step.
        namespace, version, name = root.split("/")

        # Define an identifier for this step, that is identical for all versions.
        identifier = f"{channel}/{namespace}/{name}"
    else:
        # Regular data steps.

        # Extract attributes from root of the step.
        channel, namespace, version, name = root.split("/")

        # Define an identifier for this step, that is identical for all versions.
        identifier = f"{channel}/{namespace}/{name}"

    attributes = {
        "step": step,
        "kind": kind,
        "channel": channel,
        "namespace": namespace,
        "version": version,
        "name": name,
        "identifier": identifier,
    }

    return attributes


class Step(Protocol):
    path: str
    is_public: bool = True

    def run(self) -> None:
        ...

    def is_dirty(self) -> bool:
        ...

    def checksum_output(self) -> str:
        ...


@dataclass
class DataStep(Step):
    """
    A step which creates a local Dataset on disk in the data/ folder. You specify it
    by making a Python module or a Jupyter notebook with a matching path in the
    etl/steps/data folder.
    """

    path: str
    dependencies: List[Step]

    def __init__(self, path: str, dependencies: List[Step]) -> None:
        self.path = path
        self.dependencies = dependencies

    def __str__(self) -> str:
        return f"data://{self.path}"

    def _dataset_index_mtime(self) -> Optional[float]:
        try:
            return os.stat(self._output_dataset._index_file).st_mtime
        except Exception as e:
            if str(e) == "dataset has not been created yet":
                return None
            else:
                raise e

    def run(self) -> None:
        # make sure the enclosing folder is there
        self._dest_dir.parent.mkdir(parents=True, exist_ok=True)

        ds_idex_mtime = self._dataset_index_mtime()

        sp = self._search_path
        if sp.with_suffix(".py").exists() or (sp / "__init__.py").exists():
            self._run_py()

        elif sp.with_suffix(".ipynb").exists():
            self._run_notebook()

        else:
            raise Exception(f"have no idea how to run step: {self.path}")

        # was the index file modified? if not then `save` was not called
        # NOTE: we se warnings.warn instead of log.warning because we want this in stderr
        new_ds_index_mtime = self._dataset_index_mtime()
        if new_ds_index_mtime is None or ds_idex_mtime == new_ds_index_mtime:
            warnings.warn(f"Step {self.path} did not call .save() on its output dataset")

        # modify the dataset to remember what inputs were used to build it
        dataset = self._output_dataset
        dataset.metadata.source_checksum = self.checksum_input()
        dataset.save()

        self.after_run()

    def after_run(self) -> None:
        """Optional post-hook, needs to resave the dataset again."""
        ...

    def is_dirty(self) -> bool:
        if not self.has_existing_data() or any(d.is_dirty() for d in self.dependencies):
            return True

        found_source_checksum = catalog.Dataset(self._dest_dir.as_posix()).metadata.source_checksum
        exp_source_checksum = self.checksum_input()

        if found_source_checksum != exp_source_checksum:
            return True

        return False

    def has_existing_data(self) -> bool:
        return self._dest_dir.is_dir()

    def can_execute(self) -> bool:
        sp = self._search_path
        return (
            # python script
            sp.with_suffix(".py").exists()
            # folder of scripts with __init__.py
            or (sp / "__init__.py").exists()
            # jupyter notebook
            or sp.with_suffix(".ipynb").exists()
        )

    def checksum_input(self) -> str:
        "Return the MD5 of all ingredients for making this step."
        checksums = {
            # the pandas library is so important to the output that we include it in the checksum
            "__pandas__": pd.__version__,
        }
        for d in self.dependencies:
            checksums[d.path] = d.checksum_output()

        for f in self._step_files():
            checksums[f] = files.checksum_file(f)

        in_order = [v for _, v in sorted(checksums.items())]
        return hashlib.md5(",".join(in_order).encode("utf8")).hexdigest()

    @property
    def _output_dataset(self) -> catalog.Dataset:
        "If this step is completed, return the MD5 of the output."
        if not self._dest_dir.is_dir():
            raise Exception("dataset has not been created yet")

        return catalog.Dataset(self._dest_dir.as_posix())

    def checksum_output(self) -> str:
        return self._output_dataset.checksum()

    def _step_files(self) -> List[str]:
        "Return a list of code files defining this step."
        # if dataset is a folder, use all its files
        if self._search_path.is_dir():
            return [p.as_posix() for p in files.walk(self._search_path)]

        # if a dataset is a single file, use [dataset].py and shared* files
        return glob(self._search_path.as_posix() + ".*") + glob((self._search_path.parent / "shared*").as_posix())

    @property
    def _search_path(self) -> Path:
        return paths.STEP_DIR / "data" / self.path

    @property
    def _dest_dir(self) -> Path:
        return paths.DATA_DIR / self.path.lstrip("/")

    def _run_py(self) -> None:
        """
        Import the Python module for this step and call run() on it.
        """
        # use a subprocess to isolate each step from the others, and avoid state bleeding
        # between them
        args = []

        if sys.platform == "linux":
            args.extend(["prlimit", f"--as={config.MAX_VIRTUAL_MEMORY_LINUX}"])

        args.extend(["poetry", "run", "run_python_step"])

        if config.IPDB_ENABLED:
            args.append("--ipdb")

        args.extend(
            [
                str(self),
                self._dest_dir.as_posix(),
            ]
        )

        try:
            subprocess.check_call(args)
        except subprocess.CalledProcessError:
            # swallow this exception and just exit -- the important stack trace
            # will already have been printed to stderr
            print(f'\nCOMMAND: {" ".join(args)}', file=sys.stderr)
            sys.exit(1)

    def _run_notebook(self) -> None:
        "Run a parameterised Jupyter notebook."
        notebook_path = self._search_path.with_suffix(".ipynb")
        with tempfile.TemporaryDirectory() as tmp_dir:
            notebook_out = Path(tmp_dir) / "notebook.ipynb"
            log_file = Path(tmp_dir) / "output.log"
            with open(log_file.as_posix(), "w") as ostream:
                pm.execute_notebook(
                    notebook_path.as_posix(),
                    notebook_out.as_posix(),
                    parameters={"dest_dir": self._dest_dir.as_posix()},
                    progress_bar=False,
                    stdout_file=ostream,
                    stderr_file=ostream,
                    cwd=notebook_path.parent.as_posix(),
                )


@dataclass
class WaldenStep(Step):
    path: str

    def __init__(self, path: str) -> None:
        self.path = path

    def __str__(self) -> str:
        return f"walden://{self.path}"

    def run(self) -> None:
        "Ensure the dataset we're looking for is there."
        self._walden_dataset.ensure_downloaded(quiet=True)

    def is_dirty(self) -> bool:
        if not Path(self._walden_dataset.local_path).exists():
            return True

        if files.checksum_file(self._walden_dataset.local_path) != self._walden_dataset.md5:
            return True

        return False

    def has_existing_data(self) -> bool:
        return True

    def checksum_output(self) -> str:
        if not self._walden_dataset.md5:
            raise Exception(f"walden dataset is missing checksum: {self}")

        inputs = [
            # the contents of the dataset
            self._walden_dataset.md5,
            # the metadata describing the dataset
            files.checksum_file(self._walden_dataset.index_path),
        ]

        checksum = hashlib.md5(",".join(inputs).encode("utf8")).hexdigest()

        return checksum

    @property
    def _walden_dataset(self) -> WaldenDataset:
        if self.path.count("/") != 2:
            raise ValueError(f"malformed walden path: {self.path}")

        namespace, version, short_name = self.path.split("/")

        # normally version is a year or date, but we also accept "latest"
        if version == "latest":
            dataset = WALDEN_CATALOG.find_latest(namespace=namespace, short_name=short_name)
        else:
            dataset = WALDEN_CATALOG.find_one(namespace=namespace, version=version, short_name=short_name)

        return dataset


@dataclass
class SnapshotStep(Step):
    path: str

    def __init__(self, path: str) -> None:
        self.path = path

    def __str__(self) -> str:
        return f"snapshot://{self.path}"

    def run(self) -> None:
        DVC_REPO.pull(self._path, remote="public-read", force=True)

    def is_dirty(self) -> bool:
        # check if the snapshot has been added to DVC
        with open(self._dvc_path) as istream:
            if "outs:\n" not in istream.read():
                raise Exception(f"File {self._dvc_path} has not been added to DVC. Run snapshot script to add it.")

        with dvc_lock:
            dvc_file = load_file(DVC_REPO, self._dvc_path)
            with DVC_REPO.lock:
                # DVC returns empty dictionary if file is up to date
                return dvc_file.stage.status() != {}

    def has_existing_data(self) -> bool:
        return True

    def checksum_output(self) -> str:
        return files.checksum_file(self._dvc_path)

    @property
    def _dvc_path(self) -> str:
        return f"snapshots/{self.path}.dvc"

    @property
    def _path(self) -> str:
        return f"{paths.DATA_DIR}/snapshots/{self.path}"


class SnapshotStepPrivate(SnapshotStep):
    def __str__(self) -> str:
        return f"snapshot-private://{self.path}"

    def run(self) -> None:
        DVC_REPO.pull(self._path, remote="private", force=True)


class GrapherStep(Step):
    """
    A step which ingests data from grapher channel into a local mysql database.

    If the dataset with the same short name already exists, it will be updated.
    All variables and sources related to the dataset
    """

    path: str
    data_step: DataStep

    def __init__(self, path: str, dependencies: List[Step]) -> None:
        # GrapherStep should have exactly one DataStep dependency
        assert len(dependencies) == 1
        assert path == dependencies[0].path
        assert isinstance(dependencies[0], DataStep)
        self.path = path
        self.data_step = dependencies[0]

    def __str__(self) -> str:
        return f"grapher://{self.path}"

    @property
    def dataset(self) -> catalog.Dataset:
        """Grapher dataset we are upserting."""
        return self.data_step._output_dataset

    def is_dirty(self) -> bool:
        if self.data_step.is_dirty():
            return True

        # dataset exists, but it is possible that we haven't inserted everything into DB
        dataset = self.dataset
        return fetch_db_checksum(dataset) != self.data_step.checksum_input()

    def run(self) -> None:
        # save dataset to grapher DB
        dataset = self.dataset

        dataset.metadata = gh._adapt_dataset_metadata_for_grapher(dataset.metadata)

        engine = get_engine()

        assert dataset.metadata.namespace
        dataset_upsert_results = upsert_dataset(
            engine,
            dataset,
            dataset.metadata.namespace,
            dataset.metadata.sources,
        )

        variable_upsert_results = []

        # NOTE: multiple tables will be saved under a single dataset, this could cause problems if someone
        # is fetching the whole dataset from data-api as they would receive all tables merged in a single
        # table. This won't be a problem after we introduce the concept of "tables"
        for table in dataset:
            catalog_path = f"{self.path}/{table.metadata.short_name}"

            table = gh._adapt_table_for_grapher(table)

            # generate table with entity_id, year and value for every column
            tables = gh._yield_wide_table(table, na_action="drop")
            upsert = lambda t: upsert_table(  # noqa: E731
                engine,
                t,
                dataset_upsert_results,
                catalog_path=catalog_path,
                dimensions=(t.iloc[:, 0].metadata.additional_info or {}).get("dimensions"),
            )

            # insert data in parallel, this speeds it up considerably and is even faster than loading
            # data with LOAD DATA INFILE
            if config.GRAPHER_INSERT_WORKERS > 1:
                with concurrent.futures.ThreadPoolExecutor(max_workers=config.GRAPHER_INSERT_WORKERS) as executor:
                    results = executor.map(upsert, tables)
            else:
                results = map(upsert, tables)

            variable_upsert_results += list(results)

        self._cleanup_ghost_resources(dataset_upsert_results, variable_upsert_results)

        # set checksum and updatedAt timestamps after all data got inserted
        set_dataset_checksum_and_editedAt(dataset_upsert_results.dataset_id, self.data_step.checksum_input())

    def checksum_output(self) -> str:
        raise NotImplementedError("GrapherStep should not be used as an input")

    @classmethod
    def _cleanup_ghost_resources(
        cls, dataset_upsert_results: DatasetUpsertResult, variable_upsert_results: List[VariableUpsertResult]
    ) -> None:
        """
        Cleanup all ghost variables and sources that weren't upserted
        NOTE: we can't just remove all dataset variables before starting this step because
        there could be charts that use them and we can't remove and recreate with a new ID
        """
        upserted_variable_ids = [r.variable_id for r in variable_upsert_results]
        upserted_source_ids = list(dataset_upsert_results.source_ids.values()) + [
            r.source_id for r in variable_upsert_results
        ]
        # Try to cleanup ghost variables, but make sure to raise an error if they are used
        # in any chart
        cleanup_ghost_variables(
            dataset_upsert_results.dataset_id,
            upserted_variable_ids,
            workers=config.GRAPHER_INSERT_WORKERS,
        )
        cleanup_ghost_sources(dataset_upsert_results.dataset_id, upserted_source_ids)


@dataclass
class GithubStep(Step):
    """
    Shallow-clone a git repo and ensure that the most recent version of the repo is available.
    This has the effect of triggering a rebuild each time the repo gets new commits.
    """

    path: str

    gh_repo: git.GithubRepo = field(repr=False)

    def __init__(self, path: str) -> None:
        self.path = path
        try:
            org, repo = path.split("/")
        except ValueError:
            raise Exception("github step is not in the form github://<org>/<repo>")

        self.gh_repo = git.GithubRepo(org, repo)

    def __str__(self) -> str:
        return f"github://{self.path}"

    def is_dirty(self) -> bool:
        # always poll the git repo
        return not self.gh_repo.is_up_to_date()

    def run(self) -> None:
        # either clone the repo, or update it
        self.gh_repo.ensure_cloned()

    def checksum_output(self) -> str:
        return self.gh_repo.latest_sha


def get_etag(url: str) -> str:
    resp = requests.head(url)
    resp.raise_for_status()
    return cast(str, resp.headers["ETag"])


class ETagStep(Step):
    """
    An empty step that represents a dependency on the ETag of a URL.
    """

    path: str

    def __init__(self, path: str) -> None:
        self.path = path

    def is_dirty(self) -> bool:
        return False

    def run(self) -> None:
        # nothing is done for this step
        pass

    def checksum_output(self) -> str:
        return get_etag(f"https://{self.path}")


class BackportStep(DataStep):
    def __str__(self) -> str:
        return f"backport://{self.path}"

    def run(self) -> None:
        # make sure the enclosing folder is there
        self._dest_dir.parent.mkdir(parents=True, exist_ok=True)

        dataset = backport_helpers.create_dataset(self._dest_dir.as_posix(), self._dest_dir.name)

        # modify the dataset to remember what inputs were used to build it
        dataset.metadata.source_checksum = self.checksum_input()
        dataset.save()

        self.after_run()

    def can_execute(self) -> bool:
        return True

    @property
    def _dest_dir(self) -> Path:
        return paths.DATA_DIR / self.path.lstrip("/")


class PrivateMixin:
    def after_run(self) -> None:
        """Make dataset private"""
        ds = catalog.Dataset(self._dest_dir.as_posix())  # type: ignore
        ds.metadata.is_public = False
        ds.save()


class DataStepPrivate(PrivateMixin, DataStep):
    is_public = False

    def __str__(self) -> str:
        return f"data-private://{self.path}"


class WaldenStepPrivate(WaldenStep):
    is_public = False

    def __str__(self) -> str:
        return f"walden-private://{self.path}"


class BackportStepPrivate(PrivateMixin, BackportStep):
    is_public = False

    def __str__(self) -> str:
        return f"backport-private://{self.path}"


def select_dirty_steps(steps: List[Step], max_workers: int) -> List[Step]:
    """Select dirty steps using threadpool."""
    # dynamically add cached version of `is_dirty` to all steps to avoid re-computing
    # this is a bit hacky, but it's the easiest way to only cache it here without
    # affecting the rest
    cache_is_dirty = files.RuntimeCache()
    for s in steps:
        _add_is_dirty_cached(s, cache_is_dirty)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        steps_dirty = executor.map(lambda s: s.is_dirty(), steps)  # type: ignore
        steps = [s for s, is_dirty in zip(steps, steps_dirty) if is_dirty]

    cache_is_dirty.clear()

    return steps


def _cached_is_dirty(self: Step, cache: files.RuntimeCache) -> bool:
    key = str(self)
    if key not in cache:
        cache.add(key, self._is_dirty())  # type: ignore
    return cache[key]  # type: ignore


def _add_is_dirty_cached(s: Step, cache: files.RuntimeCache) -> None:
    """Save copy of a method to _is_dirty and replace it with a cached version."""
    s._is_dirty = s.is_dirty  # type: ignore
    s.is_dirty = lambda s=s: _cached_is_dirty(s, cache)  # type: ignore
    for dep in getattr(s, "dependencies", []):
        _add_is_dirty_cached(dep, cache)
