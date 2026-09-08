---
tags:
  - Development
  - Advanced
icon: lucide/file-json
---

# Schema Management

This guide covers how to manage schema updates in the ETL system, particularly for grapher configurations and dataset metadata.

## Update grapher schema version

The grapher chart-config schema version lives in exactly one place: the vendored copy under `schemas/grapher-schema.NNN.json`. `DEFAULT_GRAPHER_SCHEMA` in `etl/config.py` is *derived* from that file's own `$id` (`vendored_grapher_schema_id()`), so there is no constant to keep in sync — and the constant can never name a version we don't have on disk.

### What touches the network, and when

Nothing about the schema version is resolved while ETL runs. Steps, `etlr`, `import etl.config` — all read the vendored file on disk, offline and deterministic. The two commands below are the *only* things that fetch from `files.ourworldindata.org`, and a person or the scheduled workflow runs them deliberately.

| When | What runs | Network? | Who runs it |
|---|---|---|---|
| Any ETL step, any import | reads `schemas/grapher-schema.NNN.json` | no | — |
| Upstream edited the current version in place (the common case) | `--refresh` | yes | the scheduled workflow, which commits it to a draft PR; or you, ad-hoc |
| Upstream published a *new* version (rare: `011` in ~2 years) | `--bump-version` | yes | you, prompted by the issue the workflow opens |

### Update process

Upstream publishes a new version rarely; most changes are made in place to the current one.

```bash
# In-place upstream change to the version we already vendor
python scripts/generate_schema_types.py --refresh

# Upstream published a new version (NNN → MMM)
python scripts/generate_schema_types.py --bump-version
```

`--bump-version` reads the new version from `grapher-schema.latest.json`, vendors it, deletes the old copy, repoints the `$ref`s in `schemas/multidim-schema.json` and `schemas/explorer-schema.json`, and regenerates `etl/collection/model/schema_types.py`. It prints what still needs a human: reviewing the upstream diff, and mirroring genuinely new properties into the `grapher_config` block embedded in `schemas/dataset-schema.json` while preserving the deliberate ETL-side deviations.

!!! warning "Don't touch the `grapher_schema` pins in collection configs"

    A bump repoints *validation* at the new version; it must not rewrite the pins in `etl/steps/viz/chart/**` configs. Those record what each config was authored against, which is what lets grapher migrate them forward. See [MDIMs and Explorers](data-work/mdims.md).

!!! note "Never resolve `grapher-schema.latest.json` at run time"

    It is an alias for one concrete version — its `properties.$schema.const` names that version, so it cannot validate a config pinned at an older one. It belongs in the bump path only.

The full workflow, including what the scheduled sync automates, is in [Grapher schema sync](grapher-schema-sync.md).

