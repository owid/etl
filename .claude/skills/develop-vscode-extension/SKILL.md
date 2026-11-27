---
name: develop-vscode-extension
description: Develop, build, and test VSCode extensions in this repo. Use when editing extension source code, compiling TypeScript, packaging .vsix files, or installing extensions for testing.
metadata:
  internal: true
  owner: Marigold
---

# VSCode Extension Development

## Extension locations

Extensions live in `vscode_extensions/<name>/`. Each has:
- `src/extension.ts` — main source code
- `dist/extension.js` — compiled output (esbuild)
- `install/<name>-<version>.vsix` — packaged extension
- `package.json` — manifest with activation events, commands, config

## Build & install workflow

VS Code runs the installed `.vsix` from `~/.vscode/extensions/`, not the repo's `dist/`, so a change takes effect only once it is packaged and installed: `make vsce-compile EXT=<name> INSTALL=1` (add `BUMP=patch` before committing a new feature), then ask the user to reload the window (`Cmd+Shift+P` → "Developer: Reload Window").

## Verifying installed code

To check what code is actually running:
```bash
# Check installed extension has your changes
grep "some_unique_string" ~/.vscode/extensions/<publisher>.<name>-<version>/dist/extension.js
```

## Common pitfalls

- **Version conflicts**: If the installed `.vsix` version matches, VSCode may cache. Use `--force` flag on install.
