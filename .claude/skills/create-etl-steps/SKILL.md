---
name: create-etl-steps
description: Create vanilla meadow, garden, and grapher ETL step files from the wizard's cookiecutter templates, given a snapshot path.
triggers:
  - create etl steps
  - create meadow garden grapher
  - create pipeline steps
  - scaffold etl steps
metadata:
  internal: true
---

# Create ETL Steps

Create meadow, garden, and grapher step files from the wizard's vanilla cookiecutter templates for a given snapshot.

## Inputs

Required:
- `snapshot_path` — in the format `namespace/version/short_name` (e.g. `washu/2026-04-22/pm25_air_pollution`)

Optional:
- `dag_file` — which DAG file to add entries to (e.g. `environment`, `climate`). If not provided, ask the user.

## Workflow

### 1. Parse the snapshot path

Extract:
- `namespace` — e.g. `washu`
- `version` — e.g. `2026-04-22`
- `short_name` — e.g. `pm25_air_pollution`

### 2. Find the snapshot file extension

Look in `snapshots/<namespace>/<version>/` for a `.dvc` file matching `<short_name>.*`. The part between `<short_name>.` and `.dvc` is the `file_extension`.

For example: `pm25_air_pollution.csv.dvc` → `file_extension = csv`

The full snapshot filename (used in meadow) is `<short_name>.<file_extension>`.

### 3. Determine the DAG file

If the user has not specified a DAG file, list the available files in `dag/` (excluding `archive/`) and ask the user which one to use.

### 4. Create directories

Create the following directories (if they don't exist):
```
etl/steps/data/meadow/<namespace>/<version>/
etl/steps/data/garden/<namespace>/<version>/
etl/steps/data/grapher/<namespace>/<version>/
```

### 5. Write the step files

#### Meadow — `etl/steps/data/meadow/<namespace>/<version>/<short_name>.py`

```python
"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("<short_name>.<file_extension>")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Improve tables format.
    tables = [
        tb.format(["country", "year"])
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
```

#### Garden — `etl/steps/data/garden/<namespace>/<version>/<short_name>.py`

```python
"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("<short_name>")

    # Read table from meadow dataset.
    tb = ds_meadow.read("<short_name>")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
```

#### Garden metadata — `etl/steps/data/garden/<namespace>/<version>/<short_name>.meta.yml`

```yaml
# NOTE: To learn more about the fields, hover over their names.


# Learn more about the available fields:
# http://docs.owid.io/projects/etl/architecture/metadata/reference/
dataset:
  update_period_days: 365


tables:
  <short_name>:
    variables:
      # testing_variable:
      #   title: Testing variable title
      #   unit: arbitrary units
      #   short_unit: au
      #   description_short: Short description of testing variable.
      #   description_key: List of key points about the indicator.
      #   processing_level: minor
      #   description_processing: Description of processing of testing variable.
      #   description_from_producer: Description of testing variable from producer.
      #   type:
      #   sort:
      #   presentation:
      #     attribution:
      #     attribution_short:
      #     faqs:
      #     grapher_config:
      #     title_public:
      #     title_variant:
      #     topic_tags:
      #   display:
      #     name: Testing variable
      #     numDecimalPlaces: 0
      #     tolerance: 0
      #     color:
      #     conversionFactor: 1
      #     description:
      #     entityAnnotationsMap: Test annotation
      #     includeInTable:
      #     isProjection: false
      #     unit: arbitrary units
      #     shortUnit: au
      #     tableDisplay:
      #       hideAbsoluteChange:
      #       hideRelativeChange:
      #     yearIsDay: false
      #     zeroDay:
      #     roundingMode:
      #     numSignificantFigures:
      #
      {}
```

#### Garden countries file — `etl/steps/data/garden/<namespace>/<version>/<short_name>.countries.json`

```json
{}
```

#### Garden excluded countries file — `etl/steps/data/garden/<namespace>/<version>/<short_name>.excluded_countries.json`

```json
[]
```

#### Grapher — `etl/steps/data/grapher/<namespace>/<version>/<short_name>.py`

```python
"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("<short_name>")

    # Read table from garden dataset.
    tb = ds_garden.read("<short_name>", reset_index=False)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
```

### 6. Add DAG entries

Append the following entries to `dag/<dag_file>.yml` under the `steps:` key. Use `ruamel_load` / `ruamel_dump` to preserve comments:

```yaml
  data://meadow/<namespace>/<version>/<short_name>:
    - snapshot://<namespace>/<version>/<short_name>.<file_extension>
  data://garden/<namespace>/<version>/<short_name>:
    - data://meadow/<namespace>/<version>/<short_name>
  data://grapher/<namespace>/<version>/<short_name>:
    - data://garden/<namespace>/<version>/<short_name>
```

To append cleanly while preserving YAML comments, use the Python helper:

```python
from etl.files import ruamel_load, ruamel_dump

dag_path = "dag/<dag_file>.yml"
with open(dag_path, "r") as f:
    data = ruamel_load(f)
data["steps"]["data://meadow/<namespace>/<version>/<short_name>"] = ["snapshot://<namespace>/<version>/<short_name>.<file_extension>"]
data["steps"]["data://garden/<namespace>/<version>/<short_name>"] = ["data://meadow/<namespace>/<version>/<short_name>"]
data["steps"]["data://grapher/<namespace>/<version>/<short_name>"] = ["data://garden/<namespace>/<version>/<short_name>"]
with open(dag_path, "w") as f:
    f.write(ruamel_dump(data))
```

### 7. Report to the user

List all files created and the DAG entries added. Suggest running:

```bash
.venv/bin/etlr <namespace>/<version>/<short_name> --private
```
