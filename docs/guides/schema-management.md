---
tags:
  - Development
  - Advanced
icon: lucide/file-json
---

# Schema Management

This guide covers how to manage schema updates in the ETL system, particularly for grapher configurations and dataset metadata.

## Update grapher schema version

ETL does not vendor the grapher chart-config schema and does not validate configs against it. The
grapher admin API validates every config it is sent and rejects a bad one on upsert. What ETL keeps
is the *version*, because grapher keys its config migrations on it.

That version lives in exactly one place, `etl/config.py`:

```python
DEFAULT_GRAPHER_SCHEMA = "https://files.ourworldindata.org/schemas/grapher-schema.011.json"
```

Upstream publishes a new version rarely — most changes are made in place to the current one, and
those need nothing here. `test_no_newer_grapher_schema_version` (integration-marked) watches for a
new version being published. Bump the constant only once grapher renders that version: stamping an
older one is safe, because grapher migrates the config forward, while stamping one grapher does not
know is rejected outright.

!!! warning "Don't touch the `grapher_schema` pins in chart configs"

    A bump changes what *new* configs are stamped with; it must not rewrite the pins in
    `etl/steps/viz/chart/**`. Those record what each config was authored against, which is what
    lets grapher migrate them forward. See [MDIMs and Explorers](data-work/mdims.md).

!!! note "Never stamp `grapher-schema.latest.json`"

    It is an alias for whatever version is current upstream, which may be one grapher does not yet
    render. It belongs in the discovery path only.
