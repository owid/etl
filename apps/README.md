# apps

Tooling built around the ETL pipeline. Most are exposed as subcommands of the unified `etl` CLI (defined in `apps/cli/__init__.py`); some — like `wizard` — run as standalone services.

## Layout

| App | What it does | Entry point |
|---|---|---|
| [`wizard`](wizard/) | Streamlit UI for browsing data, editing metadata, and running ETL workflows | `etlwiz` |
| [`browser`](browser/) | Interactive terminal browser for steps and snapshots (the default UI of bare `etl`) | `etl` |
| [`step_update`](step_update/) | Create new versions of existing steps and archive old ones | `etl update` / `etl archive` |
| [`pr`](pr/) | Create a draft PR + staging server for the current branch | `etl pr` |
| [`chart_sync`](chart_sync/) | Sync chart configs between staging and production | `etl chart-sync` |
| [`chart_approval`](chart_approval/) | Auto-approve chart diffs where staging matches production | `etl approve` |
| [`chart_animation`](chart_animation/) | Generate GIFs/videos from a Grapher chart URL | `etl chart-animation` |
| [`anomalist`](anomalist/) | Detect anomalies in indicator time series | `etl anomalist` |
| [`inspector`](inspector/) | Audit OWID metadata quality | `etl inspector` |
| [`indicator_upgrade`](indicator_upgrade/) | Match and remap indicators between old/new dataset versions | `etl indicator-upgrade` |
| [`autoupdate`](autoupdate/) | Refresh autoupdate-enabled snapshots and open PRs on changes | `etl autoupdate` |
| [`owidbot`](owidbot/) | GitHub bot that posts ETL/chart/data diffs on PRs | `etl owidbot` |
| [`housekeeper`](housekeeper/) | Periodic catalog hygiene checks (e.g. chart review reminders) | `etl d housekeeper` |
| [`backport`](backport/) | Bring legacy datasets and fasttrack imports into the ETL | `etl b run` / `etl b fasttrack` / `etl b migrate` |
| [`cli`](cli/) | The unified `etl` click group that wires the subcommands above | — |
| [`utils`](utils/) | Shared helpers (LLM clients, profiling, dataset mapping, …) | — |

## Adding a new subcommand

Register it in `GROUPS` (or one of the `SUBGROUPS`) inside `apps/cli/__init__.py`. Subcommands are lazy-loaded, so the import path just needs to point at a click `Command`/`Group` object.

Run `etl --help` to see the live command tree.
