# owid-etl

Core ETL framework for building data pipelines with DAG-based execution.

## Installation

```bash
pip install owid-etl
```

## Quick Start

1. Create a DAG file (`dag.yml`):

```yaml
steps:
  data://meadow/example/2024-01-01/dataset:
  data://garden/example/2024-01-01/dataset:
    - data://meadow/example/2024-01-01/dataset
```

2. Create your step code (`steps/data/garden/example/2024-01-01/dataset.py`):

```python
from owid.catalog import Dataset, Table

def run(dest_dir: str) -> None:
    # Load input data (you handle path resolution)
    # ...

    # Create and save output dataset
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.short_name = "dataset"
    ds.add(table)
    ds.save()
```

3. Run the ETL:

```bash
owid-etl run --dag dag.yml data://garden/example/2024-01-01/dataset
```

## Features

- DAG-based dependency resolution
- Content-addressed checksums for dirty detection
- Parallel execution support
- Works with owid-catalog for data management

## Configuration

Configure your ETL via `ETLConfig`:

```python
from pathlib import Path
from owid.etl import ETLConfig, set_config

config = ETLConfig(
    base_dir=Path("./"),
    steps_dir=Path("./steps/data"),
    dag_file=Path("./dag.yml"),
)
set_config(config)
```

## License

MIT
