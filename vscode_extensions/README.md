# Install all VS Code extensions available for ETL

To install the latest available version of all VS Code extensions for ETL, simply run:
```
make install-vscode-extensions
```

# Creating a new extension

## Initialize code

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

## Develop the extension

Modify the `src/extension.ts` file to define what the extension should do (AI will be helpful at this stage).

Modify the `package.json` file accordingly (and possibly other files).

From the new window (on the left), open a terminal (cmd+j) and run
```
npm run compile
```

Then hit F5 to run the extension. This will open a new VS Code window; for convenience, drag it to the right of the screen.

On the right window, you can now test the behaviour of the extension. You may, e.g. hit cmd+shift+p and type the name of the extension (or its shortcut).

If you make changes to the code (on the left window) you need to re-compile on the terminal (with `npm run compile`) and then click on the reload button at the top right (on the bar with buttons to run the extension).Alternatively, go to the right window and click `cmd+r` to refresh the window.

## Package the extension

Once you are happy with the result, bump up the version in package.json, go to the terminal, ensure the extension compiles, and package it.
```
npm run compile
vsce package
```

This creates a new .vsix file for the new version.

## Install a specific extension

Press `cmd+shift+p` and select "Extensions: Install from VSIX". Select the `*.vsix` file of the extension you want to install. Restart VSCode to ensure the new version is installed (although it may not be necessary).

Alternatively, from the command line, go to the root folder of the extension code and run:
```
code --install-extension [name-of-the-extension-with-version].vsix
```
