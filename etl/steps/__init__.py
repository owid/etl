#
#  __init__.py
#  steps
#
import types
from typing import (
    Any,
    Dict,
    Optional,
    Protocol,
    List,
    Set,
    Tuple,
    Union,
    cast,
    Iterable,
)
from pathlib import Path
import re
import hashlib
import tempfile
from collections import defaultdict
from urllib.parse import urlparse
from dataclasses import dataclass, field
from glob import glob
from importlib import import_module
import warnings
import graphlib
import concurrent.futures

import yaml

# smother deprecation warnings by papermill
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import papermill as pm

from owid import catalog
from owid import walden

from etl import files, paths, git
from etl.helpers import get_etag
from etl.grapher_import import (
    upsert_table,
    upsert_dataset,
    cleanup_ghost_sources,
    cleanup_ghost_variables,
    fetch_db_checksum,
)
from etl import backport_helpers

Graph = Dict[str, Set[str]]
DAG = Dict[str, Any]


def compile_steps(
    dag: DAG, includes: Optional[List[str]] = None, excludes: Optional[List[str]] = None
) -> List["Step"]:
    """
    Return the list of steps which, if executed in order, mean that every
    step has its dependencies ready for it.
    """
    includes = includes or []
    excludes = excludes or []

    # make sure each step runs after its dependencies
    steps = to_dependency_order(dag, includes, excludes)

    # parse the steps into Python objects
    return [parse_step(name, dag) for name in steps]


def to_dependency_order(
    dag: DAG, includes: List[str], excludes: List[str]
) -> List[str]:
    # reverse the graph so that dependencies point to their dependents
    graph = reverse_graph(dag)

    if includes:
        # cut the graph to just the listed steps and the things that
        # then depend on them (transitive closure)
        subgraph = filter_to_subgraph(graph, includes)

        # make sure we also include all dependencies of the subgraph
        nodes = set(subgraph.keys()) | {e for v in subgraph.values() for e in v}
        subgraph = reverse_graph(maximal_subgraph(dag, nodes))
    else:
        subgraph = graph

    # make sure dependencies are built before running the things that depend on them
    in_order = topological_sort(subgraph)

    # filter out explicit excludes
    filtered = [
        s for s in in_order if not any(re.findall(pattern, s) for pattern in excludes)
    ]

    return filtered


def load_dag(filename: Union[str, Path] = paths.DAG_FILE) -> Dict[str, Any]:
    return _load_dag(filename, {})


def _load_dag(filename: Union[str, Path], prev_dag: Dict[str, Any]):
    """
    Recursive helper to 1) load a dag itself, and 2) load any sub-dags
    included in the dag via 'include' statements
    """
    dag_yml = _load_dag_yaml(filename)
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
    return {node: set(deps) if deps else set() for node, deps in dag["steps"].items()}


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


def maximal_subgraph(graph: Graph, nodes: Set[str]) -> Graph:
    """
    Take a graph and a set of nodes and return a subgraph that only includes
    those nodes and their dependencies. (Return subgraph that contains all nodes
    reachable from given nodes... is there a name for it?)
    """
    subgraph = defaultdict(set)

    while nodes:
        node = nodes.pop()
        # already traversed
        if node in subgraph:
            continue
        subgraph[node] = set(graph.get(node, set()))
        nodes |= subgraph[node]

    return dict(subgraph)


def graph_nodes(graph: Graph) -> Set[str]:
    all_steps = set(graph)
    for children in graph.values():
        all_steps.update(children)
    return all_steps


def filter_to_subgraph(graph: Graph, includes: Iterable[str]) -> Graph:
    """
    For each step to be included, find all its ancestors and all its descendents
    recursively and include them too.
    """
    all_steps = graph_nodes(graph)
    subgraph: Graph = defaultdict(set)

    included = [
        s for s in all_steps if any(re.findall(pattern, s) for pattern in includes)
    ]

    child_frontier: List[str] = []
    parent_frontier: List[Tuple[str, str]] = []
    for node in included:
        children = graph.get(node, set())
        child_frontier.extend(children)

        parents = set((n, node) for n in graph if node in graph.get(n, set()))
        parent_frontier.extend(parents)

        subgraph[node] = children

    # follow all children and their children, recursively
    while child_frontier:
        node = child_frontier.pop()
        children = graph.get(node, set())
        child_frontier.extend(children)

        subgraph[node] = children

    # follow all parents and their parents, recursively
    while parent_frontier:
        parent, child = parent_frontier.pop()
        parents = set((n, parent) for n in graph if parent in graph.get(n, set()))
        parent_frontier.extend(parents)

        # ignore other children of this parent
        subgraph[parent] = {child}

    return dict(subgraph)


def topological_sort(graph: Graph) -> List[str]:
    """
    Take a directed graph mapping dependencies to parents, and return a list of
    steps that, if run in this order, make sure that every dependency is run before
    anything that needs it.

    E.g. you have steps, "a" and "b", where you need "a" to build "b". Then you
    will have a graph like {"a": "b"}. This method will return ["a", "b"] as the
    correct build order.

    In general, there can be many build orders which can work for a graph, we don't
    care which one we get, just that it build dependencies first.
    """
    return list(reversed(list(graphlib.TopologicalSorter(graph).static_order())))


def parse_step(step_name: str, dag: Dict[str, Any]) -> "Step":
    "Convert each step's name into a step object that we can run."
    parts = urlparse(step_name)
    step_type = parts.scheme
    path = parts.netloc + parts.path
    dependencies = [parse_step(s, dag) for s in dag.get(step_name, [])]

    step: Step
    if step_type == "data":
        if path == "garden/reference":
            step = ReferenceStep(path)
        else:
            step = DataStep(path, dependencies)

    elif step_type == "walden":
        step = WaldenStep(path)

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

    elif step_type == "grapher-private":
        step = GrapherStepPrivate(path, dependencies)

    elif step_type == "backport-private":
        step = BackportStepPrivate(path, dependencies)

    else:
        raise Exception(f"no recipe for executing step: {step_name}")

    return step


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

    def run(self) -> None:
        # make sure the enclosing folder is there
        self._dest_dir.parent.mkdir(parents=True, exist_ok=True)

        sp = self._search_path
        if sp.with_suffix(".py").exists() or (sp / "__init__.py").exists():
            self._run_py()

        elif sp.with_suffix(".ipynb").exists():
            self._run_notebook()

        else:
            raise Exception(f"have no idea how to run step: {self.path}")

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

        found_source_checksum = catalog.Dataset(
            self._dest_dir.as_posix()
        ).metadata.source_checksum
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
        checksums = {}
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
        # This cast from str to str is IMHO unnecessary but MyPy complains about this without it...
        return cast(str, self._output_dataset.checksum())

    def _step_files(self) -> List[str]:
        "Return a list of code files defining this step."
        if self._search_path.is_dir():
            return [p.as_posix() for p in files.walk(self._search_path)]

        return glob(self._search_path.as_posix() + ".*")

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
        module_path = self.path.lstrip("/").replace("/", ".")
        step_module = import_module(f"{paths.BASE_PACKAGE}.steps.data.{module_path}")
        if not hasattr(step_module, "run"):
            raise Exception(f'no run() method defined for module "{step_module}"')

        # data steps
        step_module.run(self._dest_dir.as_posix())  # type: ignore

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
                )


@dataclass
class ReferenceStep(DataStep):
    """
    A step that marks a dependency on a local dataset. It never runs, but it will checksum
    the local dataset and trigger rebuilds if the local dataset changes.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self.dependencies = []

    def is_dirty(self) -> bool:
        return False

    def can_execute(self) -> bool:
        return True

    def run(self) -> None:
        return


@dataclass
class WaldenStep(Step):
    path: str
    # NOTE: reusing catalog between steps improves performance a lot
    #   call `refresh` to refresh cache and get the latest version
    _walden_catalog = walden.Catalog()

    def __init__(self, path: str) -> None:
        self.path = path

    def __str__(self) -> str:
        return f"walden://{self.path}"

    def run(self) -> None:
        "Ensure the dataset we're looking for is there."
        self._walden_dataset.ensure_downloaded(quiet=True)

    def is_dirty(self) -> bool:
        return not Path(self._walden_dataset.local_path).exists()

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
    def _walden_dataset(self) -> walden.Dataset:
        if self.path.count("/") != 2:
            raise ValueError(f"malformed walden path: {self.path}")

        namespace, version, short_name = self.path.split("/")
        catalog = self._walden_catalog

        # normally version is a year or date, but we also accept "latest"
        if version == "latest":
            dataset = catalog.find_latest(namespace=namespace, short_name=short_name)
        else:
            dataset = catalog.find_one(
                namespace=namespace, version=version, short_name=short_name
            )

        return dataset


class GrapherStep(Step):
    """
    A step which ingests data into a local mysql database. You specify it by
    by making a Python module with a to_grapher_table function that takes a
    table and returns a sequence of tables with the fixed grapher structure of
    (year, entitiyId, value)

    If the dataset with the same short name already exists, it will be updated.
    All variables and sources related to the dataset
    """

    path: str
    dependencies: List[Step]

    def __init__(self, path: str, dependencies: List[Step]) -> None:
        self.path = path
        self.dependencies = dependencies

    def __str__(self) -> str:
        return f"grapher://{self.path}"

    def run(self) -> None:
        # make sure the enclosing folder is there
        self._dest_dir.parent.mkdir(parents=True, exist_ok=True)

        sp = self._search_path
        if sp.with_suffix(".py").exists() or (sp / "__init__.py").exists():
            self._run_py()

        else:
            raise Exception(f"have no idea how to run step: {self.path}")

    def is_dirty(self) -> bool:
        dataset = self._get_step_module().get_grapher_dataset()
        return fetch_db_checksum(dataset) != self.checksum_input()

    def can_execute(self) -> bool:
        sp = self._search_path
        return (
            # python script
            sp.with_suffix(".py").exists()
            # folder of scripts with __init__.py
            or (sp / "__init__.py").exists()
        )

    def checksum_input(self) -> str:
        "Return the MD5 of all ingredients for making this step."
        checksums = {}
        for d in self.dependencies:
            checksums[d.path] = d.checksum_output()

        for f in self._step_files():
            checksums[f] = files.checksum_file(f)

        in_order = [v for _, v in sorted(checksums.items())]
        return hashlib.md5(",".join(in_order).encode("utf8")).hexdigest()

    def checksum_output(self) -> str:
        # This cast from str to str is IMHO unnecessary but MyPy complains about this without it...
        raise Exception("grapher steps do not output a dataset")

    def _step_files(self) -> List[str]:
        "Return a list of code files defining this step."
        if self._search_path.is_dir():
            return [p.as_posix() for p in files.walk(self._search_path)]

        return glob(self._search_path.as_posix() + ".*")

    @property
    def _search_path(self) -> Path:
        return paths.STEP_DIR / "grapher" / self.path

    @property
    def _dest_dir(self) -> Path:
        return paths.DATA_DIR / self.path.lstrip("/")

    def _run_py(self) -> None:
        """
        Import the Python module for this step and call get_grapher_tables() on it.
        """
        step_module = self._get_step_module()
        dataset = step_module.get_grapher_dataset()  # type: ignore
        dataset_upsert_results = upsert_dataset(
            dataset,
            dataset.metadata.namespace,
            dataset.metadata.sources,
            self.checksum_input(),
        )
        variable_upsert_results = [
            upsert_table(table, dataset_upsert_results)
            for table in step_module.get_grapher_tables(dataset)  # type: ignore
        ]

        # Cleanup all ghost variables and sources that weren't upserted
        # NOTE: we can't just remove all dataset variables before starting this step because
        # there could be charts that use them and we can't remove and recreate with a new ID
        upserted_variable_ids = [r.variable_id for r in variable_upsert_results]
        upserted_source_ids = list(dataset_upsert_results.source_ids.values()) + [
            r.source_id for r in variable_upsert_results
        ]
        # Try to cleanup ghost variables, but make sure to raise an error if they are used
        # in any chart
        cleanup_ghost_variables(
            dataset_upsert_results.dataset_id, upserted_variable_ids
        )
        cleanup_ghost_sources(dataset_upsert_results.dataset_id, upserted_source_ids)

    def _get_step_module(self) -> types.ModuleType:
        module_path = self.path.lstrip("/").replace("/", ".")
        step_module = import_module(f"etl.steps.grapher.{module_path}")
        if not hasattr(step_module, "get_grapher_dataset"):
            raise Exception(
                f'no get_grapher_dataset() method defined for module "{step_module}"'
            )
        if not hasattr(step_module, "get_grapher_tables"):
            raise Exception(
                f'no get_grapher_tables() method defined for module "{step_module}"'
            )
        return step_module


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

        dataset = backport_helpers.create_dataset(
            self._dest_dir.as_posix(), self._dest_dir.name
        )

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


class GrapherStepPrivate(GrapherStep):
    is_public = False

    def __str__(self) -> str:
        return f"grapher-private://{self.path}"


class BackportStepPrivate(PrivateMixin, BackportStep):
    is_public = False

    def __str__(self) -> str:
        return f"backport-private://{self.path}"


def select_dirty_steps(steps: List[Step], max_workers: int) -> List[Step]:
    """Select dirty steps using threadpool."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        steps_dirty = executor.map(lambda s: s.is_dirty(), steps)  # type: ignore
        steps = [s for s, is_dirty in zip(steps, steps_dirty) if is_dirty]
    return steps
