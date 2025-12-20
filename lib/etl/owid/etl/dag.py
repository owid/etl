"""DAG loading and graph operations for ETL pipelines."""

from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Union

import yaml
from structlog import get_logger

from .config import ETLConfig, get_config

log = get_logger()

# Type alias for DAG structure
Graph = Dict[str, Set[str]]


def load_dag(
    filename: Optional[Union[str, Path]] = None,
    config: Optional[ETLConfig] = None,
) -> Graph:
    """Load a DAG from a YAML file.

    Args:
        filename: Path to the DAG YAML file. If None, uses config.dag_file.
        config: ETL configuration. If None, uses global config.

    Returns:
        A dictionary mapping step names to their dependencies.
    """
    if config is None:
        config = get_config()

    if filename is None:
        filename = config.dag_file

    return _load_dag(filename, {}, config)


def _load_dag(
    filename: Union[str, Path],
    prev_dag: Dict[str, Any],
    config: ETLConfig,
) -> Graph:
    """Recursive helper to load a DAG and any included sub-DAGs."""
    dag_yml = _load_dag_yaml(str(filename))
    curr_dag = _parse_dag_yaml(dag_yml)

    duplicate_steps = prev_dag.keys() & curr_dag.keys()
    if duplicate_steps:
        raise ValueError(f"Duplicate steps detected in DAG {filename}: {duplicate_steps}")

    curr_dag.update(prev_dag)

    # Load any included DAGs
    for sub_dag_filename in dag_yml.get("include", []):
        sub_dag_path = config.dag_dir / sub_dag_filename if config.dag_dir else Path(sub_dag_filename)
        sub_dag = _load_dag(sub_dag_path, curr_dag, config)
        curr_dag.update(sub_dag)

    return curr_dag


def _load_dag_yaml(filename: str) -> Dict[str, Any]:
    """Load a YAML file and return its contents."""
    with open(filename) as istream:
        return yaml.safe_load(istream)


def _parse_dag_yaml(dag: Dict[str, Any]) -> Graph:
    """Parse a DAG YAML structure into a graph dictionary."""
    steps = dag.get("steps") or {}
    return {node: set(deps) if deps else set() for node, deps in steps.items()}


def filter_to_subgraph(
    graph: Graph,
    includes: Iterable[str],
    downstream: bool = False,
    only: bool = False,
    exact_match: bool = False,
    excludes: Optional[List[str]] = None,
) -> Graph:
    """Filter a graph to only include specified nodes and their dependencies.

    Args:
        graph: The full DAG graph.
        includes: Step patterns to include (supports regex unless exact_match=True).
        downstream: If True, also include steps that depend on included steps.
        only: If True, only include explicitly selected nodes (no dependencies).
        exact_match: If True, includes must match step names exactly (no regex).
        excludes: Step patterns to exclude (supports regex).

    Returns:
        A filtered subgraph.
    """
    import re

    all_steps = graph_nodes(graph)
    includes_list = list(includes)

    # Handle exclusions first
    excluded_steps: Set[str] = set()
    if excludes:
        compiled_excludes = [re.compile(p) for p in excludes]
        for step in all_steps:
            if any(p.search(step) for p in compiled_excludes):
                excluded_steps.add(step)

    # Find downstream dependencies of excluded steps
    if excluded_steps:
        downstream_of_excluded = set(traverse(reverse_graph(graph), excluded_steps))
        excluded_steps.update(downstream_of_excluded)

    # Remove excluded steps from consideration
    available_steps = all_steps - excluded_steps

    # Find included steps
    if not includes_list:
        included = available_steps
    elif exact_match:
        included = set(includes_list) & available_steps
    else:
        compiled_includes = [re.compile(p) for p in includes_list]
        included = {s for s in available_steps if any(p.search(s) for p in compiled_includes)}

    if only:
        return {step: graph.get(step, set()) & included for step in included if step not in excluded_steps}

    if downstream:
        forward_deps = set(traverse(reverse_graph(graph), included))
        included = included.union(forward_deps)
        included = included - excluded_steps

    # Traverse to find all dependencies
    subgraph = traverse(graph, included)

    return {step: deps - excluded_steps for step, deps in subgraph.items() if step not in excluded_steps}


def traverse(graph: Graph, nodes: Set[str]) -> Graph:
    """Use BFS to find all nodes reachable from a given subset of nodes."""
    reachable: Graph = defaultdict(set)
    to_visit = nodes.copy()

    while to_visit:
        node = to_visit.pop()
        if node in reachable:
            continue
        reachable[node] = set(graph.get(node, set()))
        to_visit = to_visit.union(reachable[node])

    return dict(reachable)


def reverse_graph(graph: Graph) -> Graph:
    """Invert the edge direction of a graph."""
    g: Dict[str, Set[str]] = defaultdict(set)
    for dest, sources in graph.items():
        for source in sources:
            g[source].add(dest)
        # Trigger creation of dest if it's not there
        g[dest]
    return dict(g)


def graph_nodes(graph: Graph) -> Set[str]:
    """Get all nodes in a graph (both keys and values)."""
    all_steps = set(graph)
    for children in graph.values():
        all_steps.update(children)
    return all_steps


def to_dependency_order(dag: Graph) -> List[str]:
    """Organize steps in dependency order with topological sort.

    Returns a list of steps such that no step appears before its dependencies.
    """
    import graphlib

    return list(graphlib.TopologicalSorter(dag).static_order())
