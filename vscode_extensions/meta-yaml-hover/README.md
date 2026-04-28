# Meta YAML Hover

Hover details for Jinja-style placeholders inside OWID `*.meta.yml` files.

## What it does

When you hover over a placeholder like `{definitions.poverty_line_per_day}` or `{macros}` in a `*.meta.yml` file, the extension shows the resolved raw text from the same file's top-level `definitions:` block (or top-level `macros:` block).

## Supported patterns

- `{<dotted.path>}` — any dot-path resolved against the YAML root, e.g.
  - `{definitions.gdoc_id}`
  - `{definitions.poverty_line_per_day}`
  - `{tables.population.variables.population.title}`
  - `{descriptions.gini}`, `{source_common.date_accessed}`
- `{macros}` — top-level `macros:` literal block (text-extracted, preserves Jinja syntax)
- `*<anchor>` — YAML alias, e.g. `customNumericValues: *map_brackets_headcount_ratio`
- `<<: *<anchor>` — YAML merge alias (hover the `*<anchor>` part)

The hover lists every nested `{...}` / `*anchor` reference found in the resolved value as a clickable **Drill into** link that jumps to the declaration line. `Cmd+Click` (or `F12`) on a reference in the document also jumps to its declaration via Go-to-Definition.

Runtime placeholders like `<<welfare_type>>`, `{TODAY}`, `{LATEST_YEAR}`, `{date_accessed}`, `{year}` are **not** resolved — their values come from the Python step's `yaml_params=...` at render time, not from the YAML.

## Scope

Only `.meta.yml` / `.meta.yaml` files. Other YAML files are ignored.

## Build & install

```bash
cd vscode_extensions/meta-yaml-hover
npm install
npm run compile
npx @vscode/vsce package --out install/meta-yaml-hover-<version>.vsix
code --install-extension install/meta-yaml-hover-<version>.vsix --force
```

Then `Cmd+Shift+P` → "Developer: Reload Window".
