---
name: create-etl-steps
description: Create vanilla meadow, garden, and grapher ETL step files by invoking the wizard's cookiecutter templates, given a snapshot path.
triggers:
  - create etl steps
  - create meadow garden grapher
  - create pipeline steps
  - scaffold etl steps
metadata:
  internal: true
---

# Create ETL Steps

Create meadow, garden, and grapher step files for a given snapshot, by running the same cookiecutter templates the wizard runs.

> **Never copy the templates into this file.** `apps/wizard/etl_steps/cookiecutter/{meadow,garden,grapher}/` is the single source of truth, and this skill invokes it via `generate_step_to_channel`. An earlier version of this skill embedded hand-copied templates; they drifted (the multi-snapshot meadow branch and `non_redistributable` for private datasets both went missing) while the copies that hadn't drifted made the rest look current. If a template needs changing, change it in `apps/wizard/etl_steps/cookiecutter/`.

`/create-dataset` calls this skill at its Step 5. Keep the two consistent: if the inputs or generated files change here, check whether `create-dataset/SKILL.md` needs a matching edit, and make it in the same commit.

## Inputs

Required:
- `snapshot_path` — in the format `namespace/version/short_name` (e.g. `washu/2026-04-22/pm25_air_pollution`)

Optional:
- `dag_file` — which DAG file to add entries to (e.g. `environment`, `climate`). If not provided, ask the user.
- `is_private` — default `False`. Affects both the generated metadata and the DAG URIs (see steps 4 and 5).
- `update_period_days` — default `365`.
- `topic_tags` — default none.

## Workflow

### 1. Parse the snapshot path

Extract:
- `namespace` — e.g. `washu`
- `version` — e.g. `2026-04-22`
- `short_name` — e.g. `pm25_air_pollution`

### 2. Find the snapshot file extension

Look in `snapshots/<namespace>/<version>/` for a `.dvc` file matching `<short_name>.*`. The part between `<short_name>.` and `.dvc` is the `file_extension`.

For example: `pm25_air_pollution.csv.dvc` → `file_extension = csv`

The full snapshot filename is `<short_name>.<file_extension>`. Collect **all** the snapshot filenames the meadow step should read — if the chain has more than one snapshot, pass them all in step 4 and the meadow template generates the multi-snapshot loop by itself.

### 3. Determine the DAG file

If the user has not specified a DAG file, list the available files in `dag/` (excluding `archive/`) and ask the user which one to use.

### 4. Generate the step files

Call the wizard's generator once per channel. It creates the directories, renders the templates, runs ruff on the generated Python, and copies the result into `etl/steps/data/<channel>/`:

```bash
.venv/bin/python -c "
from apps.utils.files import generate_step_to_channel
from apps.wizard.etl_steps.utils import COOKIE_STEPS, remove_playground_notebook
from etl.owners import resolve_owner
import subprocess

namespace, version, short_name = '<namespace>', '<version>', '<short_name>'
snapshot_names = ['<short_name>.<file_extension>']  # every snapshot the meadow step reads
dag_file, is_private = '<dag_file>.yml', False
update_period_days, topic_tags = 365, []

git_name = subprocess.check_output(['git', 'config', 'user.name'], text=True).strip()
owner = resolve_owner(git_name) or ''

common = {
    'namespace': namespace,
    'short_name': short_name,
    'version': version,
    'add_to_dag': True,
    'dag_file': dag_file,
    'is_private': is_private,
}
per_channel = {
    'meadow': {'channel': 'meadow', 'snapshot_names_with_extension': snapshot_names},
    # topic_tags must be a pre-joined string, not a list: cookiecutter renders a list
    # as only its first element.
    'garden': {
        'channel': 'garden',
        'meadow_version': version,
        'update_period_days': update_period_days,
        'topic_tags': ('- ' + '\n- '.join(topic_tags)) if topic_tags else '',
        'owner': owner,
    },
    'grapher': {'channel': 'grapher', 'garden_version': version},
}

for channel, extra in per_channel.items():
    dataset_dir = generate_step_to_channel(cookiecutter_path=COOKIE_STEPS[channel], data={**common, **extra})
    # The meadow and garden cookiecutters both ship a playground.ipynb. The wizard keeps it only
    # for garden when the user asked for a notebook; this skill never does, so always drop it.
    remove_playground_notebook(dataset_dir)
    print(f'{channel}: {dataset_dir}')
"
```

Notes on this call:

- **Run the channels one at a time, never concurrently.** `generate_step` writes a temporary `cookiecutter.json` into the template directory and deletes it afterwards, so two simultaneous runs corrupt each other's context.
- **Every variable a template references must be present in `data`.** There is no committed `cookiecutter.json` supplying defaults, so a missing key is a Jinja `UndefinedError`, not a silent blank. The dicts above are what `apps/wizard/etl_steps/forms.py:309` passes; if a template gains a variable, it has to be added here too.
- `generate_step` prints the context dictionary to stdout, and importing `apps.wizard` logs a `No runtime found, using MemoryCacheStorageManager` warning from Streamlit. Both are expected noise, not errors.
- Use `/create-playground` if the user does want a playground notebook, rather than keeping the cookiecutter's copy.

Files generated, after the playground removal: meadow `.py`; garden `.py`, `.meta.yml`, `.countries.json`, `.excluded_countries.json`; grapher `.py`. Verify the notebook is gone — leaving one behind is the easiest thing to get wrong here, since two of the three channels ship it.

### 5. Add DAG entries

Append the following entries to `dag/<dag_file>.yml` under the `steps:` key, using `ruamel_load` / `ruamel_dump` to preserve comments. For a private dataset every `data://` below becomes `data-private://` (matching `private_suffix` in the wizard form) — the snapshot URI keeps its own form:

```yaml
  data://meadow/<namespace>/<version>/<short_name>:
    - snapshot://<namespace>/<version>/<short_name>.<file_extension>
  data://garden/<namespace>/<version>/<short_name>:
    - data://meadow/<namespace>/<version>/<short_name>
  data://grapher/<namespace>/<version>/<short_name>:
    - data://garden/<namespace>/<version>/<short_name>
```

List every snapshot from step 2 as a dependency of the meadow step, not just the first.

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

### 6. Run the checks on the generated files

New step files get checked here the same way they do when `/create-dataset` (Step 5) and `/update-dataset` (step 1b) generate them — a standalone run of this skill must not be the one path that produces unchecked step files.

Run `/check-outdated-practices` on every generated `.py`. The templates should come back clean; a hit means the template itself has drifted, and the fix belongs in `apps/wizard/etl_steps/cookiecutter/`, not only in the generated file.

The metadata checks have nothing to bite on yet — the scaffolded `.meta.yml` is entirely commented out. Name them in the report as the checks to run once the steps are filled in:

- `/check-metadata-style` — user-facing text against the Writing and Style Guide
- `/check-metadata-typos` — spelling
- `/check-metadata-spacing` — Jinja rendering artifacts, once the metadata uses templates

Also flag `.claude/rules/sanity-checks.md` if the garden step will do more than load-and-format: assertions are expected in the step, and the scaffold has none.

### 7. Report to the user

List all files created and the DAG entries added, the result of the outdated-practices check, and the deferred checks from step 6. Suggest running:

```bash
.venv/bin/etlr <namespace>/<version>/<short_name> --private
```
