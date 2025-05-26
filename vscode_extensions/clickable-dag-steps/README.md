# Clickable DAG Steps

**Clickable DAG Steps** is a VS Code extension that makes `dag.yml` files easier to navigate and maintain. It turns DAG entries into clickable links that open the corresponding `.py` files (for data and export steps) or `.dvc` files (for snapshots).

Additionally, it displays an emoji indicator next to each step to signal its status:

- 🟢 The step file exists and is the latest version in the DAG.
- 🟡 The step file exists but a newer version is present in the DAG. On hover, the corresponding latest version is shown in the tooltip.
- 🔴 No corresponding file found for the step.
- ⚠️ This step is **defined more than once** in the DAG (a likely mistake).

### Installation

Simply run `make install-vscode-extensions`.

