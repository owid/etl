---
icon: material/folder
---

# Project content

There are various folders and directories in the `etl` project. In this entry we summarize the most important ones.

You don't need to understand all of them to get started, but as you work more with the ETL you will likely find yourself exploring some of these folders.

### Folders
| Name      | Description                          |
| ----------- | ------------------------------------ |
| `api/`    | API to ETL for internal uses like updating metadata, search, etc. |
| `apps/`    | Apps built around and for ETL management. Some include `wizard`, `backport`, `fasttrack`, etc. |
| `dag/`    | Contains the dataset dependencies. That is, if `dataset A` needs `dataset B` to be up to date, this should be listed here. |
| `data/`    | When you run the recipe code for a dataset, the dataset will be created under this directory. Note that not all the content from this directory is added to git. |
| `docs/`, `.readthedocs.yaml`, `mkdocs.yml`    | Project documentation config files and directory. |
| `etl/`       | This is home to our ETL library. This is where all the recipes to generate our datasets live. |
| `export/`    | Similar to `data/` but for `export` steps. |
| `lib/`    | Other OWID sub-packages. |
| `owid_mcp/`    | OWID's MCP server code. |
| `schemas/`    | Metadata schemas for ETL datasets. |
| `scripts/`    | Various scripts. |
| `snapshots/`       | This is the entry point to ETL. This folder contains metadata and code to get external data and import it to our pipeline. |
| `tests/`    | ETL library tests. |
| `vscode_extensions/`    | In-house VS Code extensions. |
| `.claude/`, `.github`, `.streamlit`, `.vscode`    | Config for various tools. |
| `Makefile`, `default.mk`    | `make`-related files. |

### Files
| Name      | Description                          |
| ----------- | ------------------------------------ |
| `.env.example`    | Example environment variables file. |
| `AGENTS.md`, `CLAUDE.md`    | Documentation for AI agents used in ETL. |
| `Makefile`, `default.mk`    | `make`-related files. |
| `README.md`    | Main readme file. |
| `pyproject.toml`, `uv.lock` |    | Python project config files. |
| `zensical.toml`    | Config for Zensical documentation tool. |
