---
icon: material/microsoft-visual-studio-code
status:
    - new
---

# VS Code Extensions

While VS Code offers excellent built-in features and a rich marketplace, our ETL workflow has specific needs that generic extensions can't address. These extensions save time and reduce errors by understanding the ETL project structure and conventions.

Custom VS Code extensions enhance your ETL development workflow by automating repetitive tasks, improving navigation, and catching code quality issues early.


## Installing extensions

### First-time setup

To install all custom extensions:

```bash
make install-vscode-extensions
```

This installs both marketplace extensions (Ruff, YAML) and our custom extensions from `vscode_extensions/*/install/*.vsix`.

### Updating extensions

After compiling an extension, force-reinstall all extensions with their latest versions:

```bash
make vsc-ext-sync
```

This command force-reinstalls all custom extensions, ensuring you get the latest compiled versions even if they're already installed.

## Available extensions

### Find Latest ETL Step

!!! tip "**Shortcut**: ++ctrl+shift+l++"

Navigate ETL steps efficiently by searching only the latest version of each step.

**Use case**: You want to open the garden step for `energy/electricity_mix` but there are multiple versions (`2023-01-15`, `2023-06-20`, `2024-02-10`). This extension shows only the latest version files.

!!! note "How it works"

    1. Press ++ctrl+shift+l++
    2. Type the step name (e.g., "electricity_mix")
    3. See only files from the latest version
    4. Press Enter to open

### Clickable DAG Steps

!!! tip "Makes `dag.yml` entries clickable with status indicators"

Turn DAG entries into clickable links with visual status feedback:

- 🟢 Up-to-date, no issues
- 🟡 Older version exists, no issues
- ⚪ Archived step
- 🔴 Issues detected

**Use case**: While editing `dag/energy.yml`, you want to check the implementation of a dependency. Click the step URI to jump directly to the Python file.

!!! note "How it works"

    - Click any DAG entry to open its `.py` file (data/export steps) or `.dvc` file (snapshots)
    - Status emoji appears based on step version and health
    - Works across all DAG files in the project

### Run Until Cursor

!!! tip "**Use it**: ++cmd+enter++, or ++ctrl+enter++ (Windows)"

Execute code from the start of `run()` function up to your cursor position in the Interactive Window.

**Use case**: You're developing a garden step and want to test the first 10 lines of `run()` without executing the entire function.

!!! note "How it works"

    - **Outside `run()`**: Press ++cmd+enter++ → executes entire script, cursor moves to start of `run()`
    - **Inside `run()`**: Press ++cmd+enter++ → executes from start of `run()` to cursor
    - Useful for step-by-step debugging and iterative development

### Compare Previous Version

!!! tip "**Use it**: ++cmd+shift+d++, or ++ctrl+shift+d++ (Windows)"

Quickly compare the current file with its previous version.

**Use case**: You're reviewing changes in `energy/2024-02-10/electricity_mix.py` and want to see what changed from `2023-06-20`.

!!! note "How it works"

    1. Open a file with date-versioned paths (e.g., `energy/2024-02-10/file.py`)
    2. Press ++cmd+shift+d++
    3. Diff view opens comparing with previous version (`2023-06-20`)

### DoD Syntax
!!! tip "Syntax highlighting, autocomplete, and hover tooltips for Details on Demand references"

Work seamlessly with DoD references in YAML and Python files.

**Use case**: You're documenting indicator metadata and need to reference DoD entries. Start typing `#dod:` and get autocomplete suggestions with descriptions.

!!! note "How it works"

    - **Autocomplete**: Type `#dod:` → dropdown shows all available DoD names
    - **Hover tooltips**: Hover over `[Title](#dod:key)` → see full definition
    - **Syntax highlighting**: DoD references are underlined and clickable
    - **Multi-language**: Works in YAML files and Python strings
    - **Smart loading**: Pre-fetches definitions when you open a file

### Detect Outdated Practices
!!! tip "Real-time detection of deprecated code patterns"

Catch outdated code patterns as you type with configurable warnings.

**Use case**: You copy code from an old step that uses `dest_dir`. The extension highlights it with a warning: "Use `paths.create_dataset` instead."

**Detected patterns**:

- `dest_dir` usage → Use modern path handling
- `geo.harmonize_countries()` → Use `paths.regions.harmonize_names(tb)`
- `paths.load_dependency()` → Use `paths.load_dataset()` or `paths.load_snapshot()`
- `if __name__ == "__main__"` in snapshots → Remove this outdated pattern

!!! note "How it works"

    - Patterns apply only to files in `etl/steps/data/**` (scope-based detection)
    - Yellow squiggles appear under outdated code
    - Hover for suggested alternatives
    - Extensible: add new patterns in `src/extension.ts`

## Developing extensions

Improving existing extensions or creating a new ones is simple. Below, we outline the basic workflow for extension development.


!!! info "See the [extension development README](https://github.com/owid/etl/blob/master/vscode_extensions/README.md) for detailed instructions."


### Edit the extension

Edit code in `vscode_extensions/[extension-name]/src/extension.ts`

### Compile the extension

Run the following command from the ETL root directory:

```bash
make vsc-ext-compile EXT=extension-name
```

This compiles the extension and packages it into a `.vsix` file located in `vscode_extensions/[extension-name]/install/`.

Optionally, you can add `BUMP=patch` (or `minor`/`major`) to increment the version in `package.json`. And `INSTALL=1` to install the compiled extension into your local VS Code.


### Sync extensions
Install latest versions (including the one you just compiled) of all extensions:
```bash
make vsc-ext-sync
```

### Reload VS Code

Reload VS Code: cmd+shift+p → "Developer: Reload Window"


## Troubleshooting

### Extension not working after update

**Problem**: Ran `make install-vscode-extensions` but extension didn't update.

**Cause**: This command skips already-installed extensions.

**Solution**: Use `make vsc-ext-sync` to force-reinstall all extensions:

```bash
make vsc-ext-sync
```

Then reload VS Code: ++cmd+shift+p++ → "Developer: Reload Window"

### Changes not taking effect

**Problem**: Modified extension code but changes don't appear in VS Code.

**Solution**: Compile and reinstall the extension:

```bash
# Quick method
make vsc-ext-compile EXT=extension-name INSTALL=1

# Or compile then sync
make vsc-ext-compile EXT=extension-name
make vsc-ext-sync
```

Then reload VS Code: ++cmd+shift+p++ → "Developer: Reload Window"

### Manual troubleshooting

If the automated commands don't work, try manual reinstallation:

```bash
cd /path/to/etl
code --install-extension vscode_extensions/[extension]/install/[extension]-[version].vsix --force
```

Then reload VS Code: ++cmd+shift+p++ → "Developer: Reload Window"

### Extension conflicts

**Solution**: Check for conflicting keybindings in VS Code settings:

File → Preferences → Keyboard Shortcuts → Search for the shortcut

## Updating dependencies

See the [extension development README](https://github.com/owid/etl/blob/master/vscode_extensions/README.md#updating-dependencies) for guidance on updating npm dependencies, handling security vulnerabilities, and understanding when to bump extension versions.
