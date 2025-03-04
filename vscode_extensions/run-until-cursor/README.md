# Run Until Cursor

This extension allows you to **execute all lines inside `run()` up to the line where the cursor currently is** in the VS Code **Interactive Window**.

## Usage
1. Place your cursor anywhere inside `run()`.
2. Press **Cmd+Enter**.
3. The extension will **automatically select and execute the code inside run() up to the cursor**.

## Installation

- Press `cmd+shift+p` and select "Extensions: Install from VSIX".
- Select the latest version of the packaged extension, e.g. `install/run-until-cursor-0.0.1.vsix`.
- Restart VSCode to ensure the new version is installed (although it may not be necessary).

Alternatively, from the command line, go to `vscode_extensions/run-until-cursor/` and run:
```
code --install-extension run-until-cursor-0.0.1.vsix
```

## Development

To make changes to the src/extension.ts, first install dependencies (`npm install`) from the folder `run-until-cursor`. Then, after any changes in the code:
```
npm run compile
vsce package
code --install-extension run-until-cursor-0.0.1.vsix
```
(or whatever the version is, specified in `package.json`).
