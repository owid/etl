---
name: sync-grapher-schema
description: Sync upstream grapher schema changes (new chart types, config fields, enum values) into the ETL repo — vendored schema, multidim-schema, dataset-schema, and regenerated Python types. Use when the scheduled sync workflow opened a draft PR or issue that needs completing, when the web team announces a grapher schema change ("new chart type in Grapher", "I added a field to the grapher config"), when someone asks to "sync the grapher schema", or when grapher configs fail ETL validation on fields that work fine in the grapher admin.
triggers:
  - sync grapher schema
  - grapher schema changed
  - new chart type in grapher
  - update schemas from upstream
  - new grapher config field
  - finish the auto-sync schema PR
  - complete the bot schema PR
metadata:
  internal: true
---

# Sync Grapher Schema

The grapher chart-config schema is owned by the web team in [`owid-grapher`](https://github.com/owid/owid-grapher/tree/master/packages/%40ourworldindata/grapher/src/schema) and published at `https://files.ourworldindata.org/schemas/grapher-schema.NNN.json`. **It is mutated in place without version bumps** (e.g. dumbbell plots landed in `.010` directly), so when it changes upstream, four things in this repo need to follow:

| File | Role | Sync mechanism |
|---|---|---|
| `schemas/grapher-schema.NNN.json` | Vendored copy of upstream | automatic (`--refresh`) |
| `schemas/multidim-schema.json` + `schemas/explorer-schema.json` | View config `$ref`s into the grapher schema | manual: add `$ref` for **new** properties |
| `schemas/dataset-schema.json` | Embedded `grapher_config` block (validates garden `.meta.yml`) | manual: mirror changes, preserve deviations |
| `etl/collection/model/schema_types.py` | Generated Python TypedDicts | automatic (regenerate) |

Unit tests enforce consistency between all of these (`tests/test_schema_types_generation.py`, `test_grapher_config_schema_sync` in `tests/test_metadata_schemas.py`), so partial syncs fail CI. Full background: `docs/guides/grapher-schema-sync.md`.

## Entry points

**A. Completing a bot PR** (the common case). The scheduled workflow (`.github/workflows/sync-grapher-schema.yml`) detected an upstream change and opened a draft PR on the `auto-sync-grapher-schema` branch with the automatic part (refreshed vendored copy + regenerated types) already committed.

- Check out that branch — do NOT create a new PR (skip step 0; step 1's refresh is already done, just read the committed vendored diff).
- The PR's failing `test_grapher_config_schema_sync` output is the todo list — usually just steps 2-3 below.
- ⚠️ If upstream changes again before this PR merges, the workflow **force-updates the branch and clobbers manual commits**. Finish promptly; if the sync needs longer, move the work to your own branch (`git checkout -b <new>` + close the bot PR).
- When done: push, mark the PR ready for review.

**B. Ad-hoc / from scratch.** Someone announced a change and you're not waiting for the cron (alternatively, trigger the workflow manually: `gh workflow run sync-grapher-schema.yml`). Follow all steps below.

**C. Version bump.** The workflow opened a "New grapher schema version published upstream" issue → see the "Version bump" section at the bottom.

## Workflow

### 0. Branch + PR

(Entry point B only.) Use the standard flow: `.venv/bin/etl pr "sync grapher schema (<short summary>)" chore`, unless the user wants the changes on the current branch.

### 1. Refresh the vendored schema

(Entry point A: already committed by the workflow — just read the diff with `git show` on the bot commit, then continue at step 2.)

```bash
.venv/bin/python scripts/generate_schema_types.py --refresh
git diff schemas/grapher-schema.*.json
```

- **Diff is empty** → nothing changed upstream at the pinned version. Check whether a *new schema version* was published (see "Version bump" below); otherwise report there's nothing to sync and stop.
- **Diff is non-empty** → read it carefully. It is the authoritative list of what must propagate in steps 2-3. Summarize it for the user (new properties, new enum values, changed descriptions/defaults).

### 2. Propagate to `schemas/multidim-schema.json`

Only needed for **new top-level properties** (new chart-type config objects like `dumbbell`, new view-level fields). Existing `$ref`s resolve against the live schema automatically.

For each new upstream property that makes sense in a multidim/explorer view, add a `$ref` entry to the view config properties block (search for `"chartTypes"` to find it). The same applies to `schemas/explorer-schema.json`. Refs are **local relative refs** to the vendored copy (resolved offline by `Collection.validate_schema`):

```json
"<newProp>": {
    "$ref": "grapher-schema.NNN.json#/properties/<newProp>"
},
```

Lesson learned (#6196 → #6200): forgetting this step is how `dumbbell` went missing — the generated types were patched by hand instead, which regeneration would have destroyed. Never edit `schema_types.py` directly.

### 3. Propagate to `schemas/dataset-schema.json`

The grapher config is **embedded inline** (not `$ref`'d) under `...variables.additionalProperties.properties.presentation.properties.grapher_config.properties`. Mirror every change from the step-1 diff into that block — new properties, new enum values, updated descriptions.

**Preserve these deliberate ETL-side deviations** (do NOT "fix" them to match upstream):

- Extra properties not in upstream: `data`, `includedEntities`.
- `chartTypes` enum includes `WorldMap` (not upstream).
- Many enum fields are wrapped in `oneOf` with a Jinja escape hatch — keep the wrapper, edit only the enum branch:
  ```json
  "oneOf": [
      { "enum": [...sync these values...] },
      { "type": "string", "pattern": "{definitions" }
  ]
  ```
  (Some metadata fields use `"pattern": "<%"` instead — same idea.)

### 4. Regenerate the Python types

```bash
.venv/bin/python scripts/generate_schema_types.py
git diff etl/collection/model/schema_types.py
```

Sanity-check the diff: it should reflect exactly the upstream changes (plus any multidim `$ref` additions). If a class or field unexpectedly *disappears*, a `$ref` is probably missing (step 2).

Hand-written types (e.g. `GroupViewsConfig`) live in `etl/collection/model/params.py` — never add them to the generated file.

### 5. Validate

```bash
.venv/bin/pytest tests/test_schema_types_generation.py tests/test_metadata_schemas.py tests -k "collection or schema" -m "not integration" -q
make check
```

`test_grapher_config_schema_sync` pinpoints any enum value or property still missing from the embedded block (exact JSON path in the failure message) — iterate on step 3 until green.

### 6. Commit & PR description

Commit with `✨🤖`. In the PR body, list the upstream changes synced (link the Slack announcement if there is one) and which of the four files each change touched. For entry point A, mark the bot PR ready for review instead of writing a new body — just add a comment summarizing the manual propagation you did.

## Version bump (upstream publishes grapher-schema.NNN+1)

Rarer case — when the web team publishes a new schema version instead of mutating in place. Detected by the integration test `test_no_newer_grapher_schema_version` (compares the `$id` of upstream `grapher-schema.latest.json` against `DEFAULT_GRAPHER_SCHEMA`).

1. Bump `DEFAULT_GRAPHER_SCHEMA` in `etl/config.py`. (Keep it a concrete version, never `latest` — it is written into chart configs as `$schema`, and grapher's config migrations are keyed on the version.)
2. Update every `$ref` in `schemas/multidim-schema.json` and `schemas/explorer-schema.json`: `sed -i 's/grapher-schema.NNN.json/grapher-schema.MMM.json/g' schemas/multidim-schema.json schemas/explorer-schema.json`.
3. `.venv/bin/python scripts/generate_schema_types.py --refresh` (vendors the new version — the filename follows `DEFAULT_GRAPHER_SCHEMA`), then `git rm` the old vendored file.
4. Continue from step 1's diff review above (diff old vendored vs new: `git diff --no-index schemas/grapher-schema.NNN.json schemas/grapher-schema.MMM.json`).
