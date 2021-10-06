#
#  __init__.py
#  steps
#

from typing import Any, Dict, Protocol, List, Set, cast, Iterable
from pathlib import Path
import hashlib
import tempfile
from collections import defaultdict
from urllib.parse import urlparse
from dataclasses import dataclass
from glob import glob
from importlib import import_module
import warnings
import graphlib

import yaml

# smother deprecation warnings by papermill
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import papermill as pm

from owid import catalog
from owid import walden

from etl import files
from etl import paths
from etl.helpers import get_etag, get_latest_github_sha
from etl.grapher_import import upsert_table

Graph = Dict[str, Set[str]]


def select_steps(dag: Dict[str, Any], selection: List[str]) -> List[str]:
    """
    Return the list of steps which, if executed in order, mean that every
    step has its dependencies ready for it.
    """
    graph = reverse_graph(dag["steps"])

    if selection:
        # cut the graph to just the listed steps and the things that
        # then depend on them (transitive closure)
        subgraph = filter_to_subgraph(graph, selection)
    else:
        subgraph = graph

    return topological_sort(subgraph)


def load_dag(filename: str) -> Dict[str, Any]:
    with open(filename) as istream:
        dag: Dict[str, Any] = yaml.safe_load(istream)

    dag["steps"] = {
        node: set(deps) if deps else set() for node, deps in dag["steps"].items()
    }
    return dag


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


def filter_to_subgraph(graph: Graph, steps: Iterable[str]) -> Graph:
    """
    Filter to only the graph including steps and their descendents.
    """
    subgraph: Graph = defaultdict(set)

    to_visit = list(steps)
    while to_visit:
        node = to_visit.pop()
        children = graph.get(node, set())
        subgraph[node].update(children)
        to_visit.extend(children)

    return dict(subgraph)


def topological_sort(graph: Graph) -> List[str]:
    return list(reversed(list(graphlib.TopologicalSorter(graph).static_order())))


def parse_step(step_name: str, dag: Dict[str, Any]) -> "Step":
    parts = urlparse(step_name)
    step_type = parts.scheme
    step: Step
    path = parts.netloc + parts.path

    if step_type == "data":
        dependencies = dag["steps"].get(step_name, [])
        step = DataStep(path, [parse_step(s, dag) for s in dependencies])

    elif step_type == "walden":
        step = WaldenStep(path)

    elif step_type == "github":
        step = GithubStep(path)

    elif step_type == "etag":
        step = ETagStep(path)

    else:
        raise Exception(f"no recipe for executing step: {step_name}")

    return step


class Step(Protocol):
    path: str

    def run(self) -> None:
        ...

    def is_dirty(self) -> bool:
        ...

    def checksum_output(self) -> str:
        ...


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
        # make sure the encosing folder is there
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

    def is_reference(self) -> bool:
        return self.path == "reference"

    def is_dirty(self) -> bool:
        # the reference dataset never needs rebuilding
        if self.is_reference():
            return False

        if not self._dest_dir.is_dir() or any(
            isinstance(d, DataStep) and not d.has_existing_data()
            for d in self.dependencies
        ):
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
        step_module = import_module(f"etl.steps.data.{module_path}")
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
        return not Path(self._walden_dataset.local_path).exists()

    def has_existing_data(self) -> bool:
        return True

    def checksum_output(self) -> str:
        checksum: str = cast(str, self._walden_dataset.md5)
        if not checksum:
            raise Exception(
                f"no md5 checksum available for walden dataset: {self.path}"
            )
        return checksum

    @property
    def _walden_dataset(self) -> walden.Dataset:
        if self.path.count("/") != 2:
            raise ValueError(f"malformed walden path: {self.path}")

        namespace, version, short_name = self.path.split("/")
        catalog = walden.Catalog()

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
    """

    path: str
    dependencies: List[Step]

    def __init__(self, path: str, dependencies: List[Step]) -> None:
        self.path = path
        self.dependencies = dependencies

    def __str__(self) -> str:
        return f"data://{self.path}"

    def run(self) -> None:
        # make sure the encosing folder is there
        self._dest_dir.parent.mkdir(parents=True, exist_ok=True)

        sp = self._search_path
        if sp.with_suffix(".py").exists() or (sp / "__init__.py").exists():
            self._run_py()

        else:
            raise Exception(f"have no idea how to run step: {self.path}")

        # modify the dataset to remember what inputs were used to build it
        dataset = self._output_dataset
        dataset.metadata.source_checksum = self.checksum_input()
        dataset.save()

    def is_dirty(self) -> bool:
        if not self._dest_dir.is_dir() or any(
            isinstance(d, DataStep) and not d.has_existing_data()
            for d in self.dependencies
        ):
            return True

        found_source_checksum = catalog.Dataset(
            self._dest_dir.as_posix()
        ).metadata.source_checksum
        exp_source_checksum = self.checksum_input()

        if found_source_checksum != exp_source_checksum:
            return True

        return False

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
            checksums[f] = _checksum_file(f)

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
            return [p.as_posix() for p in walk(self._search_path)]

        return glob(self._search_path.as_posix() + ".*")

    @property
    def _search_path(self) -> Path:
        return Path(STEP_DIR) / "data" / self.path

    @property
    def _dest_dir(self) -> Path:
        return DATA_DIR / self.path.lstrip("/")

    def _run_py(self) -> None:
        """
        Import the Python module for this step and call get_grapher_tables() on it.
        """
        module_path = self.path.lstrip("/").replace("/", ".")
        step_module = import_module(f"etl.steps.data.{module_path}")
        if not hasattr(step_module, "get_grapher_tables"):
            raise Exception(
                f'no to_grapher_tables() method defined for module "{step_module}"'
            )

        # data steps

        # TODO: call grapher_import.upsert_dataset here

        for table in step_module.get_grapher_tables():  # type: ignore
            upsert_table(table, 1)
            # TODO: call grapher_import.upsert_table here


class GithubStep(Step):
    """
    An empty step that represents a dependency on the latest version of a Github repo.
    This has the effect of triggering a rebuild each time the repo gets new commits.
    We achieve this by using the sha1 of the most recent Github branch as the checksum.
    """

    path: str

    org: str
    repo: str
    branch: str = "master"

    def __init__(self, path: str) -> None:
        path = path.strip("/")

        self.path = path

        if path.count("/") == 1:
            self.org, self.repo = path.split("/")

        elif path.count("/") == 2:
            self.org, self.repo, self.branch = path.split("/")

        else:
            raise ValueError("github step not in form github://org/repo/[branch]")

    def is_dirty(self) -> bool:
        return False

    def run(self) -> None:
        # nothing is done for this step
        pass

    def checksum_output(self) -> str:
        return get_latest_github_sha(self.org, self.repo, self.branch)


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
