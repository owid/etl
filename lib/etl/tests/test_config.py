"""Tests for ETL configuration."""

import tempfile
from pathlib import Path

import pytest

from owid.etl import ETLConfig, get_config, set_config


def test_config_creation():
    """Test basic config creation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        dag_file = tmpdir / "dag.yml"
        dag_file.write_text("steps:\n")

        config = ETLConfig(
            base_dir=tmpdir,
            steps_dir=tmpdir / "steps",
            dag_file=dag_file,
        )

        assert config.base_dir == tmpdir
        assert config.steps_dir == tmpdir / "steps"
        assert config.dag_file == dag_file
        # Defaults
        assert config.data_dir == tmpdir / "data"
        assert config.snapshots_dir == tmpdir / "snapshots"


def test_config_string_conversion():
    """Test config accepts string paths."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dag_file = Path(tmpdir) / "dag.yml"
        dag_file.write_text("steps:\n")

        config = ETLConfig(
            base_dir=tmpdir,  # string
            steps_dir=str(Path(tmpdir) / "steps"),  # string
            dag_file=str(dag_file),  # string
        )

        assert isinstance(config.base_dir, Path)
        assert isinstance(config.steps_dir, Path)
        assert isinstance(config.dag_file, Path)


def test_global_config():
    """Test global config get/set."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        dag_file = tmpdir / "dag.yml"
        dag_file.write_text("steps:\n")

        config = ETLConfig(
            base_dir=tmpdir,
            steps_dir=tmpdir / "steps",
            dag_file=dag_file,
        )

        set_config(config)
        retrieved = get_config()

        assert retrieved.base_dir == config.base_dir


def test_get_config_without_set_raises():
    """Test that get_config raises if not set."""
    # Reset global config
    from owid.etl import config as config_module

    config_module._config = None

    with pytest.raises(RuntimeError, match="ETL configuration not set"):
        get_config()
