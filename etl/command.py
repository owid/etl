#!/usr/bin/env python
#
#  etl.py
#

from os import path
from importlib import import_module
from typing import List, Dict, Set, Iterable, Tuple, Any
from collections import defaultdict
import graphlib
from urllib.parse import urlparse

import click
import yaml

BASE_DIR = path.join(path.dirname(__file__), "..")
DAG_FILE = path.join(BASE_DIR, "dag.yml")
DATA_DIR = path.join(BASE_DIR, "data")

Graph = Dict[str, Set[str]]


@click.command()
@click.option("--dry-run", is_flag=True)
@click.argument("steps", nargs=-1)
def main(steps: List[str], dry_run: bool = False) -> None:
    """
    Execute all ETL steps listed in dag.yaml
    """
    dag = load_yaml(DAG_FILE)
    graph = reverse_graph(dag["steps"])

    if len(steps) > 0:
        subgraph = filter_to_subgraph(graph, steps)
    else:
        subgraph = graph

    step_names = topological_sort(subgraph)
    for i, step_name in enumerate(step_names, 1):
        print(f"{i}. {step_name}...")
        if not dry_run:
            run_step(step_name)

    print("Done")


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
    step_type, _path = parse_step(step_name)
    if step_type == "data":
        run_data_step(_path)

    elif step_type == "walden":
        run_walden_step(_path)

    else:
        raise Exception(f"no recipe for executing step: {step_name}")


def run_data_step(dataset_path: str) -> None:
    # data steps are expected to create new datasets in data/
    dest_dir = path.join(DATA_DIR, dataset_path.lstrip("/"))

    step_module = import_module(f"etl.steps.data.{dataset_path}")
    if not hasattr(step_module, "run"):
        raise Exception(f'no run() method defined for step "{dataset_path}"')

    # data steps
    step_module.run(dest_dir)  # type: ignore


def run_walden_step(walden_path: str) -> None:
    pass


def parse_step(step_name: str) -> Tuple[str, str]:
    parts = urlparse(step_name)
    return parts.scheme, parts.netloc + parts.path


if __name__ == "__main__":
    main()
