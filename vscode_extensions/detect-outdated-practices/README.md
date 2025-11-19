# Detect Outdated Practices

A VS Code extension that detects and highlights outdated code practices in Python files.

## Features

- Real-time detection of outdated code patterns
- Configurable warning messages for each pattern
- Easy to extend with new patterns

## Detected Patterns

Currently, the extension detects the following outdated practices:

### 1. `dest_dir` Usage
Detects use of this outdated variable in various contexts:
- Function parameters: `create_dataset(dest_dir,` or `func(dest_dir)`
- Type annotations: `dest_dir: str` or `dest_dir: Path`
- Variable usage: `dest_dir,` or `dest_dir)`
- String literals: `"dest_dir"` or `'dest_dir'`

**Recommended alternative:** Use modern path handling patterns

### 2. `geo.harmonize_countries()`
Detects calls to the outdated country harmonization function:
- `geo.harmonize_countries(tb, countries_file=...)`
- `tb = geo.harmonize_countries(...)`

**Recommended alternative:** Use `paths.regions.harmonize_names(tb)` instead

### 3. `paths.load_dependency()`
Detects calls to the deprecated dependency loader:
- `paths.load_dependency("namespace/version/dataset")`
- `ds = paths.load_dependency(...)`

**Recommended alternatives:**
- Use `paths.load_dataset()` for loading datasets
- Use `paths.load_snapshot()` for loading snapshots

## Adding New Patterns

To add new patterns, edit `src/extension.ts` and add entries to the `OUTDATED_PATTERNS` array:

```typescript
const OUTDATED_PATTERNS: OutdatedPattern[] = [
    {
        pattern: /\bdest_dir\b/,
        message: 'Use of "dest_dir" is outdated. Please use the recommended alternative.',
        severity: vscode.DiagnosticSeverity.Warning,
        scope: 'etl/steps/data/**'  // Optional: restrict to specific paths
    },
    // Add more patterns here
    {
        pattern: /\bold_function\b/,
        message: 'old_function is deprecated, use new_function instead',
        severity: vscode.DiagnosticSeverity.Warning
        // No scope = applies to all Python files
    }
];
```

### Scope Configuration

Each pattern can optionally include a `scope` field to restrict where it applies:

- **No scope** (undefined): Pattern applies to all Python files in the workspace
- **Single glob pattern**: `scope: 'etl/steps/data/**'` - Applies only to files matching this pattern
- **Multiple patterns**: `scope: ['etl/steps/**', 'apps/**']` - Applies to files matching any pattern

**Glob pattern syntax:**
- `*` - Matches any characters except `/` (single directory level)
- `**` - Matches any characters including `/` (multiple directory levels)
- Examples:
  - `etl/steps/data/**` - All files under `etl/steps/data/`
  - `etl/**/*.py` - All Python files under `etl/`
  - `apps/*/utils.py` - `utils.py` files in any direct subdirectory of `apps/`

After making changes:

1. Run `npm run compile` to build
2. Run `npx @vscode/vsce package` to create the VSIX
3. Move the VSIX file to the `install/` directory
4. Run `make install-vscode-extensions` from the project root

## Pattern Configuration

Each pattern can specify:

- **pattern**: A string or RegExp to match against code
- **message**: The warning message to display
- **severity**: The diagnostic severity level:
  - `vscode.DiagnosticSeverity.Error` (red squiggles)
  - `vscode.DiagnosticSeverity.Warning` (yellow squiggles)
  - `vscode.DiagnosticSeverity.Information` (blue squiggles)
  - `vscode.DiagnosticSeverity.Hint` (gray dots)
- **scope**: Optional path restriction (glob pattern or array of patterns)

## Testing the Scope Feature

To test the scope functionality, open these files in VS Code:

1. **`etl/steps/data/test_scope/inside_scope.py`** - Inside scope, all patterns will be detected
2. **`outside_scope.py`** - Outside scope, no patterns will be detected

Both files contain the same outdated code patterns, but only the file inside `etl/steps/data/**` will show warnings.

## Development

```bash
# Install dependencies
npm install

# Build the extension
npm run compile

# Watch for changes (auto-rebuild)
npm run watch

# Package into VSIX
npx @vscode/vsce package
```

## Installation

This extension is installed automatically via `make install-vscode-extensions` from the project root.

To manually install:
```bash
code --install-extension vscode_extensions/detect-outdated-practices/install/detect-outdated-practices-0.0.1.vsix
```
