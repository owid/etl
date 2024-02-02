The wizard is an interactive web UI for setting up the different ETL steps. It creates the base files to help you
create the recipe for a dataset.

It builds on `walkthrough`, and is compatible with the latest metadata format.

## Get set up

Before you begin, make sure you've set up the ETL as described in [Getting Started](../../getting-started/index.md).

## Wizard options
The walkthrough currently supports the following stages:

| Option      | Description                                                                                                   |
| ----------- | ------------------------------------------------------------------------------------------------------------- |
| `snapshot`  | Create a Snapshot step: Insert upstream data to our catalog.                                                  |
| `meadow`    | Create a Meadow step: Format data.                                                                            |
| `garden`    | Create a Garden step: Harmonize and process data.                                                             |
| `grapher`   | Create a Grapher step: Transform data to be Grapher-ready.                                                    |
| `charts`    | Tool to update our charts.                                                                                    |

Find more details with **`--help`**:
```bash
$ etl-wizard --help
2023-09-14 12:43:39.395 WARNING streamlit.runtime.caching.cache_data_api: No runtime found, using MemoryCacheStorageManager

 Usage: etl-wizard [OPTIONS] [[all|snapshot|meadow|garden|grapher|charts]]

 Generate template fo each step of ETL.

╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --run-checks/--skip-checks             Environment checks                                                                              │
│ --dummy-data                           Prefill form with dummy data, useful for development                                            │
│ --port                        INTEGER  Application port                                                                                │
│ --help                                 Show this message and exit.                                                                     │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## Start the wizard

Wizard consists of a web app that will ask you for various metadata fields (such as `namespace`, `version`, `source_url`, etc.). Based on your input,
it will generate the required files in the appropriate `snapshots/` or `etl/` directory. **Wizard will point you to the commands that you need to run once you
have implemented the step.**

Just start by running

```bash
poetry run etl-wizard
```

and going to [localhost:8053](localhost:8053). You can create all the steps from there.



!!! info
    Minor issues with wizard<br>
    [Add a comment to this central issue :octicons-arrow-right-24:](https://github.com/owid/etl/issues/1563)<br>

    Major issue<br>
    [Create a new issue :octicons-arrow-right-24:](https://github.com/owid/etl/issues/new)
