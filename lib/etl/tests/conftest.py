"""Pytest configuration and fixtures for owid-etl tests."""

import tempfile
from pathlib import Path

import pytest

from owid.etl import ETLConfig, set_config


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_dag_file(temp_dir):
    """Create a sample DAG file for testing."""
    dag_content = """
steps:
  data://meadow/test/2024-01-01/dataset1:
  data://garden/test/2024-01-01/dataset1:
    - data://meadow/test/2024-01-01/dataset1
"""
    dag_file = temp_dir / "dag.yml"
    dag_file.write_text(dag_content)
    return dag_file


@pytest.fixture
def sample_config(temp_dir, sample_dag_file):
    """Create a sample ETL configuration."""
    steps_dir = temp_dir / "steps" / "data"
    steps_dir.mkdir(parents=True)
    data_dir = temp_dir / "data"
    data_dir.mkdir(parents=True)

    config = ETLConfig(
        base_dir=temp_dir,
        steps_dir=steps_dir,
        dag_file=sample_dag_file,
        data_dir=data_dir,
    )
    set_config(config)
    return config
