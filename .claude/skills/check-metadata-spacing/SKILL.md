---
name: check-metadata-spacing
description: Check rendered metadata (titles, descriptions, display names) for spacing issues caused by Jinja/Jinja2 templates. Use when user mentions spacing, whitespace, Jinja rendering, or wants to verify metadata renders cleanly after editing .meta.yml files.
---

# Check Metadata Spacing

Check that Jinja templates in `.meta.yml` files render without unwanted spacing artifacts (double spaces, leading/trailing whitespace, stray newlines).

## When to use

- After editing `.meta.yml` files that use Jinja templates (`<%- if %>`, `<<variable>>`, `{definitions.xxx}`)
- When verifying that chart metadata looks correct after template changes
- When the user asks to check for spacing or whitespace issues in metadata

## Scope Options

Ask the user which scope they want to check:

1. **Current step only** - Check a specific dataset step (default if user is working on one)
2. **All active garden steps** - Check all non-archived garden steps

---

## Implementation

### 1. Load the garden dataset and inspect rendered metadata

For a specific step, load the dataset and check all variable metadata fields for spacing issues:

```python
.venv/bin/python -c "
from etl.paths import DATA_DIR
from owid.catalog import Dataset

ds = Dataset(DATA_DIR / '<channel>/<namespace>/<version>/<dataset>')
issues = []

for table_name in ds.table_names:
    tb = ds[table_name]
    for col in tb.columns:
        m = tb[col].metadata
        # Check title, description_short, description_processing
        for field_name in ['title', 'description_short', 'description_processing']:
            val = getattr(m, field_name, None)
            if val and ('  ' in val or val != val.strip() or '\n' in val):
                issues.append(f'{table_name}.{col}.{field_name}: {repr(val[:150])}')

        # Check description_key entries
        dk = getattr(m, 'description_key', None) or []
        for i, entry in enumerate(dk):
            if entry and ('  ' in entry or entry != entry.strip()):
                issues.append(f'{table_name}.{col}.description_key[{i}]: {repr(entry[:150])}')

        # Check display name
        display = getattr(m, 'display', None) or {}
        dn = display.get('name', '')
        if dn and ('  ' in dn or dn != dn.strip() or '\n' in dn):
            issues.append(f'{table_name}.{col}.display.name: {repr(dn[:150])}')

if issues:
    print(f'Found {len(issues)} spacing issues:')
    for i in issues:
        print(f'  {i}')
else:
    print('No spacing issues found.')
"
```

### 2. What counts as an issue

| Pattern | Example | Why it's a problem |
|---------|---------|-------------------|
| Double space | `Share of  children` | Jinja `if/else` block left extra whitespace |
| Leading whitespace | ` Share of children` | Template newline rendered as leading space |
| Trailing whitespace | `Share of children ` | Template block left trailing space |
| Embedded newline | `Share of\nchildren` | Multi-line Jinja block not properly trimmed |

### 3. Common Jinja fixes

If issues are found, they're typically in the `.meta.yml` file. Common fixes:

- **Use `<%-` and `-%>` trim markers** instead of `<%` and `%>` to strip whitespace around control blocks
- **Use `|-` YAML block scalar** for multi-line definitions to control trailing newlines
- **Check `{definitions.xxx}` references** — the definition itself may have leading/trailing whitespace

### 4. Present results

Group issues by table and field type. Show the rendered value with `repr()` so whitespace is visible.

If no issues are found, confirm that all templates render cleanly.

---

## Notes

- This check requires the garden step to have been run already (it reads from the built dataset, not the YAML directly)
- If the dataset hasn't been built yet, run the garden step first: `.venv/bin/etlr <path> --private --force --only`
- The check looks at the **rendered** output, not the raw YAML — this catches issues that only appear after Jinja evaluation
