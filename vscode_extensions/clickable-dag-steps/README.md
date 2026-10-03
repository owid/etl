# Clickable DAG Steps

**Clickable DAG Steps** is a VS Code extension that makes `dag.yml` files easier to navigate and maintain. It turns DAG entries into clickable links that open the corresponding `.py` files (for data and export steps) or `.dvc` files (for snapshots).

Additionally, it displays emoji indicators before each step to signal its status:

- 🟢 There are no issues, and the step is the latest version.
- 🟡 There are no issues, but there is a newer version of the step.
- ⚪ There are no issues, but the step is archived (i.e. defined in the archive DAG).
- 🔴 There are issues with the step.

Types of issues:
- ❌ No file was found for the step.
- ⚠️ The step is defined more than once in the DAG.
- ❗ The step is defined in the archive DAG, but used in the active DAG.
- ❓ The step is not defined anywhere in the DAG.

### Installation

Simply run `make vsce-sync`.

