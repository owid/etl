---
tags:
  - Development
  - Advanced
icon: lucide/file-json
---

# Schema Management

This guide covers how to manage schema updates in the ETL system, particularly for grapher configurations and dataset metadata.

## Update grapher schema version

ETL does not vendor the grapher chart-config schema, and its steps do not validate the chart configs they write against it. The grapher admin API validates indicator configs, chart configs and multidim view configs on upsert and rejects a bad one, so a typo in one of those fails the `grapher://` or `viz://` step that pushes it, not the local build. Grapher does not check explorer view configs or the per-indicator `display` overrides in a multidim. What ETL keeps is the *version*, because grapher keys its config migrations on it.

Editors still lint these blocks. `make .venv` builds `schemas/editor/grapher-config.json` (gitignored) from the published schema at `DEFAULT_GRAPHER_SCHEMA`, with `$schema` no longer required and templates (Jinja, `{definitions…}`) accepted in typed fields. The overlays next to it are mapped in `.vscode/settings.json` alongside the ETL schemas, so a misspelled key in `presentation.grapher_config` or in a multidim view `config` or `display` block is underlined. Nothing in ETL reads the file: it is editor feedback, not validation. Run `make editor-schema` to rebuild it on its own; a "Problems loading reference" warning in a `.meta.yml` means it hasn't been built yet.

That version lives in exactly one place, `etl/config.py`:

```python
DEFAULT_GRAPHER_SCHEMA = "https://files.ourworldindata.org/schemas/grapher-schema.011.json"
```

Upstream publishes a new version rarely — most changes are made in place to the current one, and those need nothing here. `test_no_newer_grapher_schema_version` (integration-marked) watches for a new version being published. Bump the constant only once grapher renders that version: stamping an older one is safe, because grapher migrates the config forward, while stamping one grapher does not know is rejected outright.

!!! warning "Don't touch the `grapher_schema` pins in chart configs"

    A bump changes what *new* configs are stamped with; it must not rewrite the pins in `etl/steps/viz/chart/**`. Those record what each config was authored against, which is what lets grapher migrate them forward. See [MDIMs and Explorers](data-work/mdims.md).

!!! note "Never stamp `grapher-schema.latest.json`"

    It is an alias for whatever version is current upstream, which may be one grapher does not yet render. It belongs in the discovery path only.
