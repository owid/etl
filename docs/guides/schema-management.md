---
tags:
  - Development
  - Advanced
icon: lucide/file-json
---

# Schema Management

This guide covers how to manage schema updates in the ETL system, particularly for grapher configurations and dataset metadata.

## Update grapher schema version

When the grapher API changes and requires a new schema version, you need to update references throughout the ETL codebase to maintain compatibility.

### Update process

1. **Update the default schema version** in `etl/config.py`:
   ```python
   DEFAULT_GRAPHER_SCHEMA = "https://files.ourworldindata.org/schemas/grapher-schema.010.json"
   ```

2. **Update schema references** in these files:
   - `schemas/multidim-schema.json` - Multiple `$ref` references
   - `schemas/explorer-schema.json` - Multiple `$ref` references
   - `apps/wizard/utils/chart_config.py` - Default schema reference

