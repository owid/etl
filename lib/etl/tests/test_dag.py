"""Tests for DAG loading and operations."""

import tempfile
from pathlib import Path

import pytest

from owid.etl import ETLConfig, set_config
from owid.etl.dag import (
    filter_to_subgraph,
    graph_nodes,
    load_dag,
    reverse_graph,
    to_dependency_order,
    traverse,
)


@pytest.fixture
def simple_dag():
    """Create a simple test DAG."""
    return {
        "data://garden/test/2024/a": {"data://meadow/test/2024/a"},
        "data://garden/test/2024/b": {"data://meadow/test/2024/b", "data://garden/test/2024/a"},
        "data://meadow/test/2024/a": set(),
        "data://meadow/test/2024/b": set(),
    }


def test_load_dag(sample_config):
    """Test loading a DAG from a YAML file."""
    dag = load_dag(config=sample_config)

    assert "data://meadow/test/2024-01-01/dataset1" in dag
    assert "data://garden/test/2024-01-01/dataset1" in dag
    assert dag["data://garden/test/2024-01-01/dataset1"] == {"data://meadow/test/2024-01-01/dataset1"}


def test_load_dag_with_includes():
    """Test loading a DAG with include statements."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create main DAG
        main_dag = tmpdir / "main.yml"
        main_dag.write_text("""
steps:
  data://garden/test/2024/a:
    - data://meadow/test/2024/a
include:
  - sub.yml
""")

        # Create sub DAG
        sub_dag = tmpdir / "sub.yml"
        sub_dag.write_text("""
steps:
  data://meadow/test/2024/a:
""")

        config = ETLConfig(
            base_dir=tmpdir,
            steps_dir=tmpdir / "steps",
            dag_file=main_dag,
        )
        set_config(config)

        dag = load_dag(config=config)

        assert "data://garden/test/2024/a" in dag
        assert "data://meadow/test/2024/a" in dag


def test_to_dependency_order(simple_dag):
    """Test topological sorting of DAG."""
    order = to_dependency_order(simple_dag)

    # Meadow steps should come before garden steps
    assert order.index("data://meadow/test/2024/a") < order.index("data://garden/test/2024/a")
    assert order.index("data://meadow/test/2024/b") < order.index("data://garden/test/2024/b")
    assert order.index("data://garden/test/2024/a") < order.index("data://garden/test/2024/b")


def test_reverse_graph(simple_dag):
    """Test graph reversal."""
    reversed_dag = reverse_graph(simple_dag)

    # In reversed graph, garden depends on meadow becomes meadow points to garden
    assert "data://garden/test/2024/a" in reversed_dag["data://meadow/test/2024/a"]


def test_graph_nodes(simple_dag):
    """Test extracting all nodes from a graph."""
    nodes = graph_nodes(simple_dag)

    assert len(nodes) == 4
    assert "data://garden/test/2024/a" in nodes
    assert "data://meadow/test/2024/a" in nodes


def test_filter_to_subgraph(simple_dag):
    """Test filtering graph to subgraph."""
    # Filter to just garden/a and its dependencies
    subgraph = filter_to_subgraph(
        simple_dag,
        includes=["data://garden/test/2024/a"],
    )

    assert "data://garden/test/2024/a" in subgraph
    assert "data://meadow/test/2024/a" in subgraph
    # b should not be included
    assert "data://garden/test/2024/b" not in subgraph


def test_filter_to_subgraph_downstream(simple_dag):
    """Test filtering with downstream dependencies."""
    subgraph = filter_to_subgraph(
        simple_dag,
        includes=["data://garden/test/2024/a"],
        downstream=True,
    )

    # Should include garden/a and garden/b (which depends on a)
    assert "data://garden/test/2024/a" in subgraph
    assert "data://garden/test/2024/b" in subgraph


def test_filter_to_subgraph_only(simple_dag):
    """Test filtering with only flag (no dependencies)."""
    subgraph = filter_to_subgraph(
        simple_dag,
        includes=["data://garden/test/2024/a"],
        only=True,
    )

    assert "data://garden/test/2024/a" in subgraph
    # Dependencies should not be included
    assert "data://meadow/test/2024/a" not in subgraph


def test_traverse(simple_dag):
    """Test BFS traversal."""
    reachable = traverse(simple_dag, {"data://garden/test/2024/b"})

    # Should find all dependencies
    assert "data://garden/test/2024/b" in reachable
    assert "data://garden/test/2024/a" in reachable
    assert "data://meadow/test/2024/a" in reachable
    assert "data://meadow/test/2024/b" in reachable
