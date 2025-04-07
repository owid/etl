# Run Until Cursor

This extension allows you to **execute all lines inside `run()` up to the line where the cursor currently is** in the VS Code **Interactive Window**.

## Usage
* If the cursor is anywhere outside the `run()` function, press `cmd+Enter` to execute the entire script in the interactive window. After doing this, the cursor will be moved to the beginning of the `run()` function.
  * This is useful when you just opened a new data step script and haven't yet initialized an interactive window.
* Once an interactive window has already been initialized, if the cursor is inside the `run()` function, press `cmd+Enter` to execute all the code inside the function until the current line (the current line will also be executed).
  * This is useful while working on a data step, when you want to quickly test the entire content of `run()` from the beginning.

## Installation

You don't have to install it manually if you are on a working ETL environment. Just running `make install-vscode-extensions` should install the latest version of this extension.

However, if you want to manually install this extension, you can:
- Press `cmd+shift+p` and select "Extensions: Install from VSIX".
- Select the latest version of the packaged extension, e.g. `install/run-until-cursor-0.0.2.vsix`.
- Restart VSCode to ensure the new version is installed (although it may not be necessary). You can do it by doing `cmd+shift+p` and selecting "Developer: Reload Window".

Alternatively, from the command line, go to `vscode_extensions/run-until-cursor/` and run:
```
code --install-extension install/run-until-cursor-0.0.2.vsix
```
(or any other version).

## Troubleshooting

This extension relies on two parameters: `"runUntilCursor.execDelay"` and `"runUntilCursor.cursorDelay"`. These are short delays used to ensure that things are executed in the correct order (unfortunately, I haven't found a reliable way to avoid these manual delays).

If the extension doesn't seem to work — for example, you press `Cmd+Enter`, the interactive window is opened, but **nothing is actually executed** — then the issue may be that `"cursorDelay"` is too short for your machine or VS Code version.

To fix this, go to your `.vscode/settings.json` file and increase the value of `"runUntilCursor.cursorDelay"`. After a recent VS Code or Jupyter update, this delay often needs to be **longer than before**. In my case, setting it to `1300` milliseconds works reliably.

However, if you are working in a **multi-folder workspace** (e.g. loading both `etl` and `owid-grapher`), then the `.vscode/settings.json` file will be ignored. In that case, you need to define the settings in your workspace file (e.g. `owid.code-workspace`). Example:

```json
{
  "folders": [
    {
      "path": "etl"
    },
    {
      "path": "owid-grapher"
    }
  ],
  "settings": {
    "runUntilCursor.execDelay": 100,
    "runUntilCursor.cursorDelay": 1300
  }
}
```

Save this file and open it as your workspace. The settings will now be respected when you launch the extension.

## Development

To make changes to the src/extension.ts, first install dependencies (`npm install`) from the folder `run-until-cursor`. Then, after any changes in the code:
```
npm run compile
vsce package
code --install-extension run-until-cursor-0.0.2.vsix
```
(or whatever version is specified in `package.json`). Then, from VSCode, `cmd+shift+p` and select "Developer: Reload Window".
