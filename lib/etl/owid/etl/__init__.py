"""Core ETL framework for building data pipelines.

This package provides a minimal, reusable framework for building ETL pipelines
with DAG-based execution.

Example usage:

    from pathlib import Path
    from owid.etl import ETLConfig, run_dag, set_config

    # Configure the ETL
    config = ETLConfig(
        base_dir=Path("./"),
        steps_dir=Path("./steps/data"),
        dag_file=Path("./dag.yml"),
    )
    set_config(config)

    # Run steps
    run_dag(steps=["data://garden/example/2024/dataset"])
"""

__version__ = "0.1.0"

from .config import ETLConfig, get_config, set_config
from .dag import (
    Graph,
    filter_to_subgraph,
    graph_nodes,
    load_dag,
    reverse_graph,
    to_dependency_order,
    traverse,
)
from .execution import (
    enumerate_steps,
    exec_graph_parallel,
    exec_steps,
    exec_steps_parallel,
    run_dag,
    run_steps,
    timed_run,
    update_open_file_limit,
)
from .snapshot import Snapshot, SnapshotMeta, snapshot_catalog
from .steps import (
    DEFAULT_KEEP_MODULES,
    INSTANT_METADATA_DIFF,
    STEP_REGISTRY,
    DataStep,
    DataStepPrivate,
    ETagStep,
    ExportStep,
    GithubRepo,
    GithubStep,
    PrivateMixin,
    SnapshotStep,
    SnapshotStepPrivate,
    Step,
    checksum_file,
    compile_steps,
    extract_step_attributes,
    get_etag,
    isolated_env,
    load_from_uri,
    parse_step,
    register_step,
    run_module_run,
    select_dirty_steps,
    walk_files,
)

__all__ = [
    # Config
    "ETLConfig",
    "get_config",
    "set_config",
    # DAG
    "Graph",
    "load_dag",
    "filter_to_subgraph",
    "reverse_graph",
    "to_dependency_order",
    "traverse",
    "graph_nodes",
    # Steps
    "Step",
    "STEP_REGISTRY",
    "register_step",
    "DataStep",
    "DataStepPrivate",
    "SnapshotStep",
    "SnapshotStepPrivate",
    "ExportStep",
    "ETagStep",
    "GithubStep",
    "GithubRepo",
    "PrivateMixin",
    "get_etag",
    "compile_steps",
    "parse_step",
    "extract_step_attributes",
    "load_from_uri",
    "isolated_env",
    "run_module_run",
    "checksum_file",
    "walk_files",
    "DEFAULT_KEEP_MODULES",
    "INSTANT_METADATA_DIFF",
    # Execution
    "run_dag",
    "run_steps",
    "exec_steps",
    "exec_steps_parallel",
    "exec_graph_parallel",
    "select_dirty_steps",
    "enumerate_steps",
    "timed_run",
    "update_open_file_limit",
    # Snapshot
    "Snapshot",
    "SnapshotMeta",
    "snapshot_catalog",
]
