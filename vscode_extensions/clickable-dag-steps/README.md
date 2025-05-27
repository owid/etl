# Clickable DAG Steps

**Clickable DAG Steps** is a VS Code extension that makes `dag.yml` files easier to navigate and maintain. It turns DAG entries into clickable links that open the corresponding `.py` files (for data and export steps) or `.dvc` files (for snapshots).

Additionally, it displays an emoji indicator next to each step to signal its status:

- 🟢 (OK) The step file exists and is the latest version in the DAG.
- 🟡 (OK) The step file exists but a newer version exists in the DAG. On hover, the corresponding latest version is shown in the tooltip.
- ⚪ (OK) The step file exists, the step is defined in the archive DAG, and used in the archive DAG.
- ❗ (ERROR) The step file exists, the step is defined in the archive DAG, and mistakenly used in the active DAG.
- ❓ (ERROR) The step file exists, but the step is defined nowhere in the DAG.
- 🔴 (ERROR) No file exists for the step.
- ⚠️ (ERROR) This step is mistakenly defined more than once in the DAG.

### Installation

Simply run `make install-vscode-extensions`.

