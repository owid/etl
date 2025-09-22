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

3. **Search for hardcoded references**:
   ```bash
   grep -r "grapher-schema\.009" .
   ```

4. **Test the changes**:
   ```bash
   make test  # Run all tests including schema validation
   pytest tests/test_metadata_schemas.py  # Test metadata schemas specifically
   ```

### Updating individual steps

YAML metadata files that don't pass `test_metadata_schemas.py` need to fix their `$schema` in `presentation.grapher_config`, for example:

```yaml
variables:
   foo:
      presentation:
         grapher_config:
            $schema: https://files.ourworldindata.org/schemas/grapher-schema.008.json
            ...
```
