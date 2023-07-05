# Using the interactive walkthrough

The walkthrough is an interactive web UI for setting up the different ETL steps. It creates the base files to help you
create the recipe for a dataset.

## Get set up

Before you begin, make sure you've set up the ETL as described in [Getting Started](../../getting-started/index.md).

## Walkthrough options
The walkthrough currently supports the following stages:

| Option      | Description                                                                                                   |
| ----------- | ------------------------------------------------------------------------------------------------------------- |
| `snapshot`  | Create a Snapshot step: Insert upstream data to our catalog.                                                  |
| `walden`    | (This step is deprecated. Use `snapshot` instead). Create a Walden step: Insert upstream data to our catalog. |
| `meadow`    | Create a Meadow step: Format data.                                                                            |
| `garden`    | Create a Garden step: Harmonize and process data.                                                             |
| `grapher`   | Create a Grapher step: Transform data to be Grapher-ready.                                                    |
| `explorers` | Create an Explorer step: Transform data to be Explorer-ready.                                                 |
| `charts`    | Tool to update our charts.                                                                                    |

```bash
$ walkthrough --help
Usage: walkthrough [OPTIONS] {walden|snapshot|meadow|garden|grapher|explo
                   rers|charts}

Options:
  --run-checks / --skip-checks  Environment checks
  --dummy-data                  Prefill form with dummy data, useful for
                                development
  --auto-open / --no-auto-open  Auto open browser on port 8082
  --port INTEGER                Application port
  --help                        Show this message and exit.
```

## Start the walkthrough

Walkthrough consists of a web app that will ask you for various metadata fields (such as `namespace`, `version`, `source_url`, etc.). Based on your input,
it will generate the required files in the appropriate `snapshots/` or `etl/` directory. **Walkthrough will point you to the commands that you need to run once you
have implemented the step.**

A typical workflow would involve:

1. Create a snapshot step: `poetry run walkthrough snapshot`.
2. Create a meadow step based on the output from 1: `poetry run walkthrough meadow`.
3. Create a garden step based on the output from 2: `poetry run walkthrough garden`.
4. Create a grapher step based on the output from 3: `poetry run walktrhough grapher`.
