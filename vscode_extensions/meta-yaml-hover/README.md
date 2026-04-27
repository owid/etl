# Meta YAML Hover

Hover details for Jinja-style placeholders inside OWID `*.meta.yml` files.

## What it does

When you hover over a placeholder like `{definitions.poverty_line_per_day}` or `{macros}` in a `*.meta.yml` file, the extension shows the resolved raw text from the same file's top-level `definitions:` block (or top-level `macros:` block).

## Supported patterns

- `{definitions.<key>}` — simple reference, e.g. `{definitions.gdoc_id}`
- `{definitions.<key>.<subkey>...}` — arbitrarily nested dot-paths
- `{macros}` — top-level `macros:` literal block

Nested `{definitions.X}` placeholders inside a resolved value are shown as-is — the extension does not recursively expand them.

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
