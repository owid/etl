# Run Until Cursor

This extension allows you to **execute all lines inside `run()` up to the line where the cursor currently is** in the VS Code **Interactive Window**.

## Usage
* If the cursor is anywhere outside the `run()` function, press `cmd+Enter` to execute the entire script in the interactive window. After doing this, the cursor will be moved to the beginning of the `run()` function.
  * This is useful when you just opened a new data step script and haven't yet initialized an interactive window.
* Once an interactive window has already been initialized, if the cursor is inside the `run()` function, press `cmd+Enter` to execute all the code inside the function until the current line (the current line will also be executed).
  * This is useful while working on a data step, when you want to quickly test the entire content of `run()` from the beginning.

## Installation

You don't have to install it manually if you are on a working ETL environment. Just running `make test` should install the latest version of this extension.

However, if you want to manually install this extension, you can:
- Press `cmd+shift+p` and select "Extensions: Install from VSIX".
- Select the latest version of the packaged extension, e.g. `install/run-until-cursor-0.0.1.vsix`.
- Restart VSCode to ensure the new version is installed (although it may not be necessary). You can do it by doing `cmd+shift+p` and selecting "Developer: Reload Window".

Alternatively, from the command line, go to `vscode_extensions/run-until-cursor/` and run:
```
code --install-extension install/run-until-cursor-0.0.1.vsix
```
(or any other version).

## Development

To make changes to the src/extension.ts, first install dependencies (`npm install`) from the folder `run-until-cursor`. Then, after any changes in the code:
```
npm run compile
vsce package
code --install-extension run-until-cursor-0.0.1.vsix
```
(or whatever version is specified in `package.json`). Then, from VSCode, `cmd+shift+p` and select "Developer: Reload Window".
