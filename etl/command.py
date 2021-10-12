#!/usr/bin/env python
#
#  etl.py
#

from collections import defaultdict
from typing import Callable, List, Dict, Set, Iterable, Any
from urllib.parse import urlparse
import graphlib
import time

import click
import yaml

from etl.steps import Step, DataStep, WaldenStep
from etl import paths

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
    dag = load_yaml(paths.DAG_FILE.as_posix())

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
    steps = [
        _parse_step(name, dag) for name in step_names if name != "data://reference"
    ]

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
            click.echo(f"{click.style('OK', fg='blue')} ({time_taken:.0f}s)")
            print()


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
        children = graph.get(node, set())
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
        dependencies = dag["steps"].get(step_name, [])
        step = DataStep(path, [_parse_step(s, dag) for s in dependencies])

    elif step_type == "walden":
        step = WaldenStep(path)

    else:
        raise Exception(f"no recipe for executing step: {step_name}")

    return step


def timed_run(f: Callable[[], Any]) -> float:
    start_time = time.time()
    f()
    return time.time() - start_time


if __name__ == "__main__":
    main()
