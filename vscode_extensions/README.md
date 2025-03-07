# Creating a new extension

## Initialize code

Install the tool to create all the boilerplate code:
```
npm install -g yo generator-code
```

Run the tool:
```
yo code
```

At this stage you may need to add some code to `src/extension.ts`, `package.json`, and elsewhere.

## Set up debug mode

Go to the root folder of the extension, and run
```
npm run install
```

Open a VSCode on that folder. On the terminal, run
````
npm run watch
````
(and keep this terminal open).

Then go to the debug pane, and hit the play button to "Run Extension". This will open a new VSCode window.

On the new window, hit cmd+shift+p and type the name of the extension (or its shortcut).

You can now make changes in the code, then go to the new window, and hit `cmd+r` to refresh.
Then, rerun the extension and it should reflect the latest changes.

## Package the extension

Once you are happy with the result, bump up the version in package.json, go to the terminal, ensure the extension compiles, and package it.
```
npm run compile
vsce package
```

This creates a new .vsix file for the new version.


## Install an extension

Press `cmd+shift+p` and select "Extensions: Install from VSIX". Select the `*.vsix` file of the extension you want to install. Restart VSCode to ensure the new version is installed (although it may not be necessary).

Alternatively, from the command line, go to the root folder of the extension code and run:
```
code --install-extension [name-of-the-extension-with-version].vsix
```
