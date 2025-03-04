import * as vscode from "vscode";

export function activate(context: vscode.ExtensionContext) {
    console.log("🟢 Run Until Cursor Extension is activating...");

    let disposable = vscode.commands.registerCommand("extension.runUntilCursor", async () => {
        console.log("✅ Command extension.runUntilCursor is running!");

        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage("No active editor found.");
            return;
        }

        const document = editor.document;
        let cursorLine = editor.selection.active.line;
        let startLine: number | null = null;

        // Find 'def run()'
        for (let i = 0; i < document.lineCount; i++) {
            if (/^\s*def run\(/.test(document.lineAt(i).text)) {
                startLine = i + 1;
                break;
            }
        }

        if (startLine === null) {
            vscode.window.showErrorMessage("Could not find `run()`.");
            return;
        }

        // If cursor is outside 'run()', execute the full function
        if (cursorLine < startLine) {
            cursorLine = document.lineCount - 1;
        }

        // Select the range
        const range = new vscode.Range(startLine, 0, cursorLine + 1, 0);
        editor.selection = new vscode.Selection(range.start, range.end);

        console.log("📤 Selection updated. Executing in Interactive Window...");

        // Wait a short moment to ensure selection is applied before execution
        setTimeout(async () => {
            await vscode.commands.executeCommand("jupyter.execSelectionInteractive");
        }, 100);
    });

    context.subscriptions.push(disposable);
    console.log("🟢 Command extension.runUntilCursor registered!");
}

export function deactivate() {
    console.log("🔴 Run Until Cursor Extension deactivated.");
}
