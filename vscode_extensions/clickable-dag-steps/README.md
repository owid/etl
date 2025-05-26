# Clickable DAG Steps

**Clickable DAG Steps** is a VS Code extension that makes `dag.yml` files easier to navigate by turning DAG entries like `data://...` and `export://...` into clickable links. These links open the corresponding Python ETL scripts in your workspace, or, in the case of snapshots, the corresponding `.dvc` file of the snapshot.

This extension also shows a symbol next to each entry in the dag, which is:
- 🟢 if a file exists for that step, and the step's version is the latest in the DAG.
- 🟡 if a file exists for that step, but there is a newer version of that step.
- 🔴 if no file is found for that step.
- ⚠️ if the step is repeated in the DAG.

## Usage

Once this extension is installed, simply Cmd/Ctrl+Click on a step entry.

## Changelog

- Version 0.0.2 makes entries in the DAG clickable, and also shows a colored circle next to the step.
- Version 0.0.1 simply makes entries in the DAG clickable.
