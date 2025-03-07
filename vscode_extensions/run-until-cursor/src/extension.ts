import * as vscode from "vscode";

export function activate(context: vscode.ExtensionContext) {
    let disposable = vscode.commands.registerCommand("extension.runUntilCursor", async () => {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            vscode.window.showErrorMessage("No active editor found.");
            return;
        }

        const document = editor.document;
        let cursorLine = editor.selection.active.line;
        let startLine: number | null = null;
        let endLine: number | null = null;

        // Read timeout values from VS Code settings (default: 100ms and 500ms)
        const execDelay = vscode.workspace.getConfiguration("runUntilCursor").get<number>("execDelay", 100);
        const cursorDelay = vscode.workspace.getConfiguration("runUntilCursor").get<number>("cursorDelay", 500);

        // Step 1: Find where `run()` starts
        for (let i = 0; i < document.lineCount; i++) {
            if (/^\s*def run\(/.test(document.lineAt(i).text)) {
                startLine = i + 1; // Start selection after `def run():`
                break;
            }
        }

        if (startLine !== null) {
            // Step 2: Find where `run()` ends (detecting **non-indented** lines)
            for (let i = startLine; i < document.lineCount; i++) {
                const lineText = document.lineAt(i).text;

                // If we find a **non-indented line** (not empty & not a comment), assume `run()` ends here
                if (lineText.trim().length > 0 && !lineText.startsWith(" ") && !lineText.startsWith("\t")) {
                    endLine = i;
                    break;
                }
            }

            // If no non-indented line was found, assume `run()` ends at the last line
            if (endLine === null) {
                endLine = document.lineCount;
            }
        }

        let executeRange: vscode.Range;
        let shouldMoveCursor = false;

        if (startLine !== null && endLine !== null) {
            // If inside `run()`, execute up to the cursor
            if (cursorLine >= startLine && cursorLine < endLine) {
                executeRange = new vscode.Range(startLine, 0, cursorLine + 1, 0);
            }
            // If outside `run()` (either above or below), execute **the entire script**
            else {
                executeRange = new vscode.Range(0, 0, document.lineCount, 0);
                shouldMoveCursor = true;
            }
        } else {
            // If `run()` is not found, execute everything as a fallback
            executeRange = new vscode.Range(0, 0, document.lineCount, 0);
        }

        // Step 3: Apply selection and execute immediately (with an adjustable delay)
        editor.selection = new vscode.Selection(executeRange.start, executeRange.end);

        setTimeout(async () => {
            await vscode.commands.executeCommand("jupyter.execSelectionInteractive");
        }, execDelay); // User-configurable execution delay

        // Step 4: Move cursor inside `run()` **only if the cursor was outside**
        if (shouldMoveCursor && startLine !== null) {
            setTimeout(() => {
                const newPosition = new vscode.Position(startLine, 0);
                editor.selection = new vscode.Selection(newPosition, newPosition);
                editor.revealRange(new vscode.Range(newPosition, newPosition));
            }, cursorDelay); // User-configurable cursor movement delay
        }
    });

    context.subscriptions.push(disposable);
}

export function deactivate() {}
