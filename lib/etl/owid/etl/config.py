"""Configuration for the ETL framework."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Union


@dataclass
class ETLConfig:
    """User-provided configuration for the ETL framework.

    This dataclass holds all the configuration needed to run ETL pipelines.
    Users should create an instance of this class and pass it to the ETL functions.

    Attributes:
        base_dir: Root directory for the ETL project (where data/ lives)
        steps_dir: Directory containing step code (e.g., steps/data/)
        dag_file: Path to the main DAG YAML file
        dag_dir: Optional directory containing additional DAG files for includes
        data_dir: Directory where output datasets are stored
        snapshots_dir: Directory where snapshot files are stored
        archive_dir: Optional directory for archived steps (if None, no archive support)
        etl_epoch: Version epoch - changing this forces all steps to rebuild

        # Runtime options
        catalog_url: Optional base URL for downloading datasets from a remote catalog
        instant_mode: If True, garden steps only update metadata (no data processing)
        run_in_process: If True, run steps in the same process (vs subprocess)
        max_virtual_memory: Max virtual memory for subprocess execution (Linux only)
        subprocess_command: Custom command for subprocess execution
        verify_ssl: Whether to verify SSL certificates for HTTP requests
    """

    base_dir: Union[str, Path]
    steps_dir: Union[str, Path]
    dag_file: Union[str, Path]
    dag_dir: Union[str, Path, None] = None
    data_dir: Union[str, Path, None] = None
    snapshots_dir: Union[str, Path, None] = None
    archive_dir: Union[str, Path, None] = None
    etl_epoch: int = 0

    # Runtime options
    catalog_url: Optional[str] = None
    instant_mode: bool = False
    run_in_process: bool = False
    max_virtual_memory: Optional[int] = None
    subprocess_command: Optional[List[str]] = field(default=None)
    verify_ssl: bool = True

    def __post_init__(self) -> None:
        # Convert strings to Path objects
        if isinstance(self.base_dir, str):
            self.base_dir = Path(self.base_dir)
        if isinstance(self.steps_dir, str):
            self.steps_dir = Path(self.steps_dir)
        if isinstance(self.dag_file, str):
            self.dag_file = Path(self.dag_file)
        if isinstance(self.dag_dir, str):
            self.dag_dir = Path(self.dag_dir)
        if isinstance(self.data_dir, str):
            self.data_dir = Path(self.data_dir)
        if isinstance(self.snapshots_dir, str):
            self.snapshots_dir = Path(self.snapshots_dir)
        if isinstance(self.archive_dir, str):
            self.archive_dir = Path(self.archive_dir)

        # Set defaults based on base_dir
        if self.data_dir is None:
            self.data_dir = self.base_dir / "data"
        if self.snapshots_dir is None:
            self.snapshots_dir = self.base_dir / "snapshots"
        if self.dag_dir is None:
            self.dag_dir = self.dag_file.parent


# Global config instance (set by user or CLI)
_config: Optional[ETLConfig] = None


def get_config() -> ETLConfig:
    """Get the current ETL configuration.

    Raises:
        RuntimeError: If no configuration has been set.
    """
    if _config is None:
        raise RuntimeError("ETL configuration not set. Call set_config() first or pass config explicitly.")
    return _config


def set_config(config: ETLConfig) -> None:
    """Set the global ETL configuration."""
    global _config
    _config = config
