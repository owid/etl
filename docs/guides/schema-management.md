---
tags:
  - Development
  - Advanced
icon: lucide/file-json
---

# Schema Management

This guide covers how to manage schema updates in the ETL system, particularly for grapher configurations and dataset metadata.

## Update grapher schema version

ETL does not vendor or validate against the grapher chart-config schema — the grapher admin
API validates every config it is sent, and rejects a bad one on upsert. What ETL keeps is the
*version*, because grapher keys its config migrations on it.

When the web team publishes a new schema version, bump the constant in `etl/config.py`:

```python
DEFAULT_GRAPHER_SCHEMA = "https://files.ourworldindata.org/schemas/grapher-schema.011.json"
```

`test_no_newer_grapher_schema_version` (integration-marked) watches for a new version upstream.
Bump only once grapher renders it: ETL stamping an older version is safe (grapher migrates the
config forward), stamping one grapher does not know is rejected.

!!! warning "Do not bump the `grapher_schema` pins in MDIM configs"

    Each MDIM config pins the version its view configs were authored against, and that pin is
    exactly what lets grapher migrate them forward. Bumping it tells grapher the configs are
    already current and skips the migration.

