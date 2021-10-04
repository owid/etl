#!/usr/bin/env python
#
#  etl.py
#

from collections import defaultdict
from dataclasses import dataclass
from glob import glob
from importlib import import_module
from os import path
from pathlib import Path
from typing import Callable, List, Dict, Protocol, Set, Iterable, Any, cast
from urllib.parse import urlparse
import graphlib
import hashlib
import tempfile
import time
import warnings

import click
import yaml

# smother deprecation warnings by papermill
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import papermill as pm

from owid import catalog, walden

BASE_DIR = Path(__file__).parent.parent
DAG_FILE = BASE_DIR / "dag.yml"
DATA_DIR = BASE_DIR / "data"
STEP_DIR = BASE_DIR / "etl" / "steps"

Graph = Dict[str, Set[str]]


@click.command()
@click.option("--dry-run", is_flag=True, help="Only print the steps that would be run")
@click.option(
    "--force", is_flag=True, help="Redo a step even if it appears done and up-to-date"
)
@click.argument("steps", nargs=-1)
def main(steps: List[str], dry_run: bool = False, force: bool = False) -> None:
    """
    Execute all ETL steps listed in dag.yaml
    """
    # Load our graph of steps and the things they depend on
    dag = load_yaml(DAG_FILE.as_posix())

    # Run the steps we have selected, and everything downstream of them
    run_dag(dag, steps, dry_run=dry_run, force=force)


def run_dag(
    dag: Dict[str, Any],
    selection: List[str],
    dry_run: bool = False,
    force: bool = False,
) -> None:
    """
    Run the selected steps, and anything that needs updating based on them. An empty
    selection means "run all steps".

    By default, data steps do not re-run if they appear to be up-to-date already by
    looking at their checksum.
    """
    step_names = select_steps(dag, selection)
    steps = [_parse_step(name, dag) for name in step_names]

    if not force:
        steps = [s for s in steps if s.is_dirty()]

    if not steps:
        print("All datasets up to date!")
        return

    print(f"Running {len(steps)} steps:")
    for i, step in enumerate(steps, 1):
        print(f"{i}. {step}...")
        if not dry_run:
            time_taken = timed_run(lambda: step.run())
            print(f"  OK ({time_taken:.0f}s)")


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


def load_yaml(filename: str) -> Dict[str, Any]:
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
        children = graph[node]
        subgraph[node].update(children)
        to_visit.extend(children)

    return dict(subgraph)


def topological_sort(graph: Graph) -> List[str]:
    return list(reversed(list(graphlib.TopologicalSorter(graph).static_order())))


def _parse_step(step_name: str, dag: Dict[str, Any]) -> "Step":
    parts = urlparse(step_name)
    step_type = parts.scheme
    step: Step
    path = parts.netloc + parts.path

    if step_type == "data":
        dependencies = dag["steps"][step_name]
        step = DataStep(path, [_parse_step(s, dag) for s in dependencies])

    elif step_type == "walden":
        step = WaldenStep(path)

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

    def __str__(self):
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

    def is_dirty(self) -> bool:
        if not self._dest_dir.is_dir():
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
            # jupyter notebook
            or sp.with_suffix(".ipynb").exists()
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
        notebook_path = self._search_path.with_suffix(".ipynb").as_posix()
        with tempfile.TemporaryDirectory() as tmp_dir:
            notebook_out = path.join(tmp_dir, "notebook.ipynb")
            with open(path.join(tmp_dir, "output.log"), "w") as ostream:
                pm.execute_notebook(
                    notebook_path,
                    notebook_out,
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

    def __str__(self):
        return f"walden://{self.path}"

    def run(self) -> None:
        "Ensure the dataset we're looking for is there."
        self._walden_dataset.ensure_downloaded(quiet=True)

    def is_dirty(self) -> bool:
        return not Path(self._walden_dataset.local_path).exists()

    def checksum_output(self) -> str:
        checksum: str = self._walden_dataset.md5
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


def timed_run(f: Callable[[], Any]) -> float:
    start_time = time.time()
    f()
    return time.time() - start_time


def _checksum_file(filename: str) -> str:
    "Return the md5 hex digest of the file contents."
    chunk_size = 2 ** 20
    _hash = hashlib.md5()
    with open(filename, "rb") as istream:
        chunk = istream.read(chunk_size)
        while chunk:
            _hash.update(chunk)
            chunk = istream.read(chunk_size)

    return _hash.hexdigest()


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


if __name__ == "__main__":
    main()
