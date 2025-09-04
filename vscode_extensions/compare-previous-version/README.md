# Compare Previous Version Extension

This VSCode extension allows you to quickly compare the currently open file with its previous version by finding the highest date version that's lower than the current version.

## Features

- **Keyboard Shortcut**: `Ctrl+Shift+D` (Windows/Linux) or `Cmd+Shift+D` (Mac)
- **Command Palette**: "Compare Previous Version"
- Automatically finds the previous version of the current file based on date folders
- Directly opens a diff view comparing current version with previous version
- Works with versioned folder structures using YYYY-MM-DD format
- Smart version detection and sorting

## How to Use

1. Open a file in VSCode that has a date version in its path (e.g., `snapshots/biodiversity/2025-04-07/cherry_blossom.py`)
2. Press `Ctrl+Shift+D` (or `Cmd+Shift+D` on Mac) or use Command Palette > "Compare Previous Version"
3. The extension will automatically find the previous version and open a diff view
4. If no previous version exists, you'll be notified

## Example

If you have a file open at `snapshots/biodiversity/2025-04-07/cherry_blossom.py`, the extension will:
- Look in `snapshots/biodiversity/` for other date folders
- Find the highest date that's lower than `2025-04-07` (e.g., `2024-01-25`)
- Open a diff view comparing `snapshots/biodiversity/2024-01-25/cherry_blossom.py` with your current file

## Installation

1. Navigate to the extension directory
2. Run `npm install` to install dependencies
3. Run `npm run compile` to build the extension
4. Run `vsce package` to create the .vsix file
5. Install the .vsix file in VSCode using "Extensions: Install from VSIX"