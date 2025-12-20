# Cherry Blossom Example

This is a minimal example showing how to use the `owid.etl` framework to build a data pipeline.

## Structure

```
cherry_blossom/
├── dag.yml                    # DAG definition
├── data/                      # Output data directory
└── steps/
    └── data/
        ├── meadow/            # Raw data processing
        │   └── example/2024-01-01/cherry_blossom.py
        └── garden/            # Data transformation
            └── example/2024-01-01/cherry_blossom.py
```

## Running the Pipeline

```bash
# From the cherry_blossom directory:

# Dry run to see what would be executed
owid-etl run --dag dag.yml --dry-run

# Run all steps
owid-etl run --dag dag.yml

# Run only the garden step
owid-etl run --dag dag.yml "garden/" --only

# Run with force (even if up-to-date)
owid-etl run --dag dag.yml --force
```

## Using from Python

```python
from pathlib import Path
from owid.etl import ETLConfig, set_config, run_dag

# Configure the ETL
config = ETLConfig(
    base_dir=Path("./"),
    steps_dir=Path("./steps/data"),
    dag_file=Path("./dag.yml"),
)
set_config(config)

# Run the pipeline
run_dag(steps=["data://garden/example/2024-01-01/cherry_blossom"])
```
