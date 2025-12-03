# VS Code Extensions for ETL

## Quick Reference

### Available Commands

| Command | Purpose | When to Use |
|---------|---------|-------------|
| `make install-vscode-extensions` | Install all extensions for first time | Initial setup only (skips already-installed) |
| `make vsce-sync` | Force-reinstall all extensions | After compiling any extension to update it |
| `make vsce-compile EXT=name INSTALL=1` | Compile and install single extension | Quick development iteration |
| `make vsce-compile EXT=name BUMP=patch INSTALL=1` | Bump version, compile, and install | Before committing new features |

### Common Workflows

**Rapid development (most common):**
```bash
# Edit code → compile → install immediately (keeps current version)
make vsce-compile EXT=detect-outdated-practices INSTALL=1
```

**Before committing (bump version):**
```bash
# Bump version → compile → install
make vsce-compile EXT=detect-outdated-practices BUMP=patch INSTALL=1
```

**Alternative workflow (compile then sync):**
```bash
# Step 1: Compile one or more extensions
make vsce-compile EXT=detect-outdated-practices

# Step 2: Sync all extensions at once
make vsce-sync
```

### Key Concepts

**Version Bumping:**
- **No BUMP parameter**: Keeps current version (use during development)
- **BUMP=patch**: Increment patch version (0.0.1 → 0.0.2) for bug fixes
- **BUMP=minor**: Increment minor version (0.1.0 → 0.2.0) for new features
- **BUMP=major**: Increment major version (1.0.0 → 2.0.0) for breaking changes

**Installation:**
- **INSTALL=1**: Immediately installs after compilation with `--force` flag
- **make vsce-sync**: Force-reinstalls ALL custom extensions with their latest VSIX files
- **make install-vscode-extensions**: Only installs extensions not already installed (skips updates)

**File Structure:**
```
vscode_extensions/
├── extension-name/
│   ├── src/extension.ts          # Your extension code
│   ├── package.json               # Version and metadata
│   ├── install/                   # Generated VSIX files
│   │   ├── extension-0.0.3.vsix  # Latest version
│   │   └── archived/             # Old versions (auto-archived on BUMP)
│   └── dist/extension.js          # Compiled output
```

**Note**: When using `BUMP`, old versions are automatically moved to `install/archived/` to keep the directory clean.

### Troubleshooting

**Problem: "I ran `make install-vscode-extensions` but my extension didn't update"**
- **Cause**: This command skips already-installed extensions
- **Solution**: Use `make vsce-sync` instead to force-reinstall all extensions

**Problem: "Changes to my code aren't showing up in VS Code"**
- **Cause**: Extension wasn't reinstalled with the new version
- **Solution**: After compiling, run `make vsce-sync` or use `INSTALL=1` flag

**Problem: "I want to test without bumping the version"**
- **Solution**: Omit the `BUMP` parameter: `make vsce-compile EXT=name INSTALL=1`

**Problem: "How do I know which version is installed?"**
```bash
code --list-extensions --show-versions | grep extension-name
```

## Creating a new extension

### Initialize code

Prerequisites to be able to create an extension: Node.js, npm

Requirements: Yeoman, VS Code extension generator, and VS Code CLI for extension packaging:
```
npm install -g yo generator-code vsce
```

Run the tool to create all the boilerplate code:
```
yo code
```

Follow the steps:
- What type of extension do you want to create? -> New Extension (TypeScript)
- What's the name of your extension? -> Some Good Title
- What's the identifier of your extension? -> some-good-title
- What's the description of your extension? -> A good description
- Initialize a git repository? No
- Which bundler to use? esbuild
- Which package manager to use? Npm

This will create a new folder with all boilerplate. Then:
- Do you want to open the new folder with Visual Studio Code? Yes

For convenience, drag this new window to the left of the screen (the right will be for testing).

### Develop the extension

Modify the `src/extension.ts` file to define what the extension should do (AI will be helpful at this stage).

Modify the `package.json` file accordingly (and possibly other files).

**Testing during development:**

From the extension directory, open a terminal (cmd+j) and run:
```bash
npm run compile
```

Then hit F5 to run the extension in debug mode. This opens a new VS Code window for testing.

On the test window, you can test the extension behavior (e.g., cmd+shift+p to search for commands).

**Making changes:**
- Edit code in the main window
- Recompile: `npm run compile`
- Reload test window: Click reload button or press `cmd+r`

### Package and Install the Extension

**Modern workflow (recommended):**

From the project root:
```bash
# Option 1: Compile and install in one step
make vsce-compile EXT=your-extension-name INSTALL=1

# Option 2: Compile, then sync all extensions
make vsce-compile EXT=your-extension-name
make vsce-sync

# With version bump (patch, minor, or major)
make vsce-compile EXT=your-extension-name BUMP=patch INSTALL=1
```

**Legacy workflow:**

From extension directory:
```bash
npm run compile
npx @vscode/vsce package
mkdir -p install
mv *.vsix install/
```

Then install via:
- GUI: `cmd+shift+p` → "Extensions: Install from VSIX"
- CLI: `code --install-extension install/extension-name-version.vsix --force`
- Or: `make vsce-sync` from project root

## Updating dependencies

Extensions have npm dependencies that should be updated periodically for security patches and bug fixes.

### Quick workflow

1. **Check for updates**: `npm outdated` and `npm audit`
2. **Update dependencies**: Accept Dependabot PR or run `npm update`
3. **Verify build**: `npm run compile && npm audit`
4. **Test packaging**: `npx @vscode/vsce package` (then delete the `.vsix` file)
5. **Commit changes**: Only commit `package-lock.json` (don't bump extension version)

### Key concepts

- **package.json**: Defines acceptable version ranges (e.g., `^9.11.1` = any 9.x >= 9.11.1)
- **package-lock.json**: Records exact versions installed; should be committed to git
- **Transitive dependencies**: Dependencies of your dependencies (e.g., `js-yaml` via `eslint`)

### When to bump extension version

**Bump version** when you change extension code or features.

**Don't bump version** when only updating dev dependencies or `package-lock.json` - version numbers signal changes to your extension's functionality, not build tools.
