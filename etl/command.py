#!/usr/bin/env python
#
#  etl.py
#

from os import path
import os
from importlib import import_module
from typing import Callable, List, Dict, Set, Iterable, Tuple, Any
from collections import defaultdict
import graphlib
from urllib.parse import urlparse
import tempfile
import time
import warnings

import click
import yaml

# smother deprecation warnings by papermill
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import papermill as pm

from owid import walden

BASE_DIR = path.normpath(path.join(path.dirname(__file__), ".."))
DAG_FILE = path.join(BASE_DIR, "dag.yml")
DATA_DIR = path.join(BASE_DIR, "data")
STEP_DIR = path.join(BASE_DIR, "etl", "steps")

Graph = Dict[str, Set[str]]


@click.command()
@click.option("--dry-run", is_flag=True)
@click.argument("steps", nargs=-1)
def main(steps: List[str], dry_run: bool = False) -> None:
    """
    Execute all ETL steps listed in dag.yaml
    """
    dag = load_yaml(DAG_FILE)
    run_dag(dag, steps, dry_run)


def run_dag(dag: Dict[str, Any], selection: List[str], dry_run: bool = False) -> None:
    step_names = select_steps(dag, selection)

    print(f"Running {len(step_names)} steps:")
    for i, step_name in enumerate(step_names, 1):
        print(f"  {i}. {step_name}... ", end="", flush=True)
        if not dry_run:
            time_taken = timed_run(lambda: run_step(step_name))
            print(f"({time_taken:.0f}s)")
        else:
            print()


def select_steps(dag: Dict[str, Any], selection: List[str]) -> List[str]:
    graph = reverse_graph(dag["steps"])

    if selection:
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


def run_step(step_name: str) -> None:
    step_type, name = parse_step(step_name)

    if step_type == "data":
        run_data_step(name)

    elif step_type == "walden":
        run_walden_step(name)

    else:
        raise Exception(f"no recipe for executing step: {step_name}")


def run_data_step(step_name: str) -> None:
    # data steps are expected to create new datasets in data/
    dest_dir = path.join(DATA_DIR, step_name.lstrip("/"))
    os.makedirs(path.dirname(dest_dir), exist_ok=True)

    module_name = path.join(STEP_DIR, "data", step_name)
    if path.exists(module_name + ".py"):
        run_data_step_py(step_name, dest_dir)

    elif path.exists(module_name + ".ipynb"):
        run_data_step_notebook(module_name + ".ipynb", dest_dir)

    else:
        raise Exception(f"have no idea how to run step: {step_name}")


def run_data_step_py(step_name: str, dest_dir: str) -> None:
    step_module = import_module(f"etl.steps.data.{step_name}")
    if not hasattr(step_module, "run"):
        raise Exception(f'no run() method defined for step "{step_name}"')

    # data steps
    step_module.run(dest_dir)  # type: ignore


def run_data_step_notebook(notebook_path: str, dest_dir: str) -> None:
    "Run a parameterised Jupyter notebook."
    with tempfile.TemporaryDirectory() as tmp_dir:
        notebook_out = path.join(tmp_dir, "notebook.ipynb")
        with open(path.join(tmp_dir, "output.log"), "w") as ostream:
            pm.execute_notebook(
                notebook_path,
                notebook_out,
                parameters={"dest_dir": path.abspath(dest_dir)},
                progress_bar=False,
                stdout_file=ostream,
                stderr_file=ostream,
            )


def run_walden_step(walden_path: str) -> None:
    "Ensure the dataset we're looking for is there."
    walden_dataset = _find_walden_dataset(walden_path)
    walden_dataset.ensure_downloaded()


def _find_walden_dataset(walden_path: str) -> walden.Dataset:
    if walden_path.count("/") != 2:
        raise ValueError(f"malformed walden path: {walden_path}")

    namespace, version, short_name = walden_path.split("/")
    catalog = walden.Catalog()

    # normally version is a year or date, but we also accept "latest"
    if version == "latest":
        dataset = catalog.find_latest(namespace=namespace, short_name=short_name)
    else:
        dataset = catalog.find_one(
            namespace=namespace, version=version, short_name=short_name
        )

    return dataset


def parse_step(step_name: str) -> Tuple[str, str]:
    parts = urlparse(step_name)
    return parts.scheme, parts.netloc + parts.path


def timed_run(f: Callable[[], Any]) -> float:
    start_time = time.time()
    f()
    return time.time() - start_time


if __name__ == "__main__":
    main()
